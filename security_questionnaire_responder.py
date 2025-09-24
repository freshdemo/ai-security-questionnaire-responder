
import gspread
import google.generativeai as genai
import time
import os
from google.oauth2.service_account import Credentials
from pathlib import Path
import random
import requests
import tempfile
import json
import hashlib
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse
from bs4 import BeautifulSoup
from google.api_core import exceptions as gcloud_exceptions
from gspread.exceptions import APIError as GSpreadAPIError
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Set, List, Optional, Dict, Any
from urllib.robotparser import RobotFileParser
import time

# Configuration
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
# Allow overriding spreadsheet and worksheet via environment variables for flexibility
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', '1es_mPMrkUO2Ez5FtdRUkAPNDaB7i2oBa0BBzUTRCIbg')
WORKSHEET_INDEX = int(os.getenv('WORKSHEET_INDEX', '0'))
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', './snyk-cX-se-demo-0ce146967b8c.json')

# Source configuration
SOURCES = os.getenv('SOURCES', 'both').lower()  # 'both', 'website', or 'docs'
DOCS_DIR = os.getenv('DOCS_DIR', './docs')  # Folder containing documents (PDFs, spreadsheets) to provide as context
WEBSITE_URLS = [url.strip() for url in os.getenv('WEBSITE_URLS', '').split(',') if url.strip()]  # List of website URLs to include as context

# Performance settings
MAX_WORKERS = int(os.getenv('GEMINI_MAX_WORKERS', '8'))  # Concurrency for Gemini requests
VERIFY_WRITES = os.getenv('VERIFY_WRITES', 'false').lower() in {'1', 'true', 'yes'}
PERSIST_UPLOADS = os.getenv('PERSIST_UPLOADS', 'true').lower() in {'1', 'true', 'yes'}  # Whether to persist uploaded files
UPLOAD_CACHE_FILE = os.path.expanduser('~/.gemini_upload_cache.json')  # File to store upload cache

# Validate source configuration
if SOURCES not in ['both', 'website', 'docs']:
    raise ValueError("SOURCES must be one of: 'both', 'website', or 'docs'")

# Track auth mode for help messages
ACTIVE_AUTH = None  # "service_account" | "oauth"
SERVICE_ACCOUNT_EMAIL = None

# Configure Gemini
if not GEMINI_API_KEY:
    raise SystemExit("GEMINI_API_KEY is not set. Export it and re-run.")
genai.configure(api_key=GEMINI_API_KEY)

# Set up Google Sheets access
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def _find_doc_paths(directory: str) -> list[Path]:
    """
    Find all document files (PDF, Markdown, and other supported formats) in the given directory and its subdirectories.
    
    Args:
        directory: Path to the directory to search in
        
    Returns:
        List of Path objects for all found documents
    """
    supported_extensions = {
        # Document formats
        '.pdf',
        '.md', '.markdown',
        # Spreadsheet formats
        '.xlsx', '.xls', '.csv', '.tsv', '.ods'
    }
    
    try:
        doc_paths = []
        for ext in supported_extensions:
            doc_paths.extend(Path(directory).rglob(f'*{ext}'))
        return sorted(doc_paths)
    except Exception as e:
        print(f"Error finding documents in {directory}: {e}")
        return []

def _load_upload_cache() -> Dict[str, str]:
    """Load the upload cache from disk."""
    if not PERSIST_UPLOADS:
        print("Upload persistence is disabled (PERSIST_UPLOADS=False)")
        return {}
        
    print(f"Loading upload cache from: {UPLOAD_CACHE_FILE}")
    
    if not os.path.exists(UPLOAD_CACHE_FILE):
        print("No existing cache file found. A new one will be created.")
        return {}
        
    try:
        with open(UPLOAD_CACHE_FILE, 'r') as f:
            cache = json.load(f)
            print(f"Loaded {len(cache)} cached uploads")
            return cache
    except json.JSONDecodeError as e:
        print(f"Warning: Cache file is corrupted. Starting with an empty cache. Error: {e}")
        return {}
    except Exception as e:
        print(f"Warning: Could not load upload cache: {e}")
        return {}

def _save_upload_cache(cache: Dict[str, Any]) -> None:
    """Save the upload cache to disk."""
    if not PERSIST_UPLOADS:
        print("Not saving cache: PERSIST_UPLOADS is False")
        return
        
    try:
        cache_dir = os.path.dirname(UPLOAD_CACHE_FILE)
        if cache_dir:  # Only try to create directory if path is not empty
            os.makedirs(cache_dir, exist_ok=True)
            
        temp_file = f"{UPLOAD_CACHE_FILE}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(cache, f, indent=2)
            
        # Atomic write by renaming the temp file
        os.replace(temp_file, UPLOAD_CACHE_FILE)
        print(f"Saved {len(cache)} uploads to cache: {UPLOAD_CACHE_FILE}")
        
    except Exception as e:
        print(f"Error saving upload cache: {e}")
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass

def _chunk_text(text: str, max_chunk_size: int = 4000) -> list[str]:
    """Split text into chunks of approximately max_chunk_size characters, breaking at paragraph boundaries."""
    if len(text) <= max_chunk_size:
        return [text]
        
    chunks = []
    current_chunk = []
    current_size = 0
    
    # Split by paragraphs first
    paragraphs = text.split('\n\n')
    
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
            
        # If paragraph is too large, split it into sentences
        if len(para) > max_chunk_size // 2:
            sentences = para.replace('. ', '.\n').split('\n')
            for sent in sentences:
                sent = sent.strip()
                if not sent:
                    continue
                    
                if current_size + len(sent) > max_chunk_size and current_chunk:
                    chunks.append('\n\n'.join(current_chunk))
                    current_chunk = []
                    current_size = 0
                    
                current_chunk.append(sent)
                current_size += len(sent) + 2  # +2 for the newlines
        else:
            if current_size + len(para) > max_chunk_size and current_chunk:
                chunks.append('\n\n'.join(current_chunk))
                current_chunk = []
                current_size = 0
                
            current_chunk.append(para)
            current_size += len(para) + 2  # +2 for the newlines
    
    if current_chunk:
        chunks.append('\n\n'.join(current_chunk))
        
    return chunks

def _get_file_fingerprint(file_path: Path) -> str:
    """Generate a fingerprint for a file based on its content and metadata."""
    hasher = hashlib.sha256()
    # Include file size and modification time in the fingerprint
    stat = file_path.stat()
    hasher.update(f"{stat.st_size}:{stat.st_mtime}:".encode())
    # Include first and last 8KB of file content
    with open(file_path, 'rb') as f:
        # Read first 8KB
        chunk = f.read(8192)
        hasher.update(chunk)
        # Read last 8KB if file is larger than 16KB
        if stat.st_size > 16384:
            f.seek(-8192, 2)
            chunk = f.read()
            hasher.update(chunk)
    return hasher.hexdigest()

def _upload_single_file(p: Path, upload_cache: dict, ext_to_mime: dict):
    """Helper function to upload a single file with proper error handling."""
    try:
        # Get absolute path as string for the cache
        abs_path = str(p.absolute())
        file_fingerprint = _get_file_fingerprint(p)
        
        # Check if we have a cached upload for this exact file
        cache_key = f"{abs_path}:{file_fingerprint}"
        if cache_key in upload_cache:
            print(f"Using cached upload for: {p.name}")
            return genai.get_file(name=upload_cache[cache_key]), cache_key, None
            
        # No cache hit, upload the file
        print(f"Uploading to Gemini: {p.name}")
        mime = ext_to_mime.get(p.suffix.lower())
        if not mime:
            print(f"  Warning: Unsupported file type: {p.suffix}")
            return None, None, None
            
        f = genai.upload_file(path=str(p), mime_type=mime)
        print(f"  Uploaded: {f.name}")
        return f, cache_key, f.name
    except Exception as e:
        print(f"  Error uploading {p.name}: {e}")
        return None, None, None

def _upload_pdfs(paths):
    """
    Upload files to Gemini in parallel, reusing existing uploads when possible.
    
    Returns:
        List of uploaded file objects
    """
    uploaded = []
    ext_to_mime = {
        '.pdf': 'application/pdf',
        '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.xls': 'application/vnd.ms-excel',
        '.csv': 'text/csv',
        '.tsv': 'text/tab-separated-values',
        '.ods': 'application/vnd.oasis.opendocument.spreadsheet',
        '.md': 'text/markdown',
        '.markdown': 'text/markdown',
        '.txt': 'text/plain'
    }
    
    # Always start with a fresh cache to ensure new files are processed
    upload_cache = {}
    if PERSIST_UPLOADS and os.path.exists(UPLOAD_CACHE_FILE):
        try:
            os.remove(UPLOAD_CACHE_FILE)
            print("Cleared existing upload cache to ensure fresh uploads")
        except Exception as e:
            print(f"Warning: Could not clear upload cache: {e}")
    cache_updated = False
    
    # Process files in parallel with a reasonable number of workers
    max_workers = min(MAX_WORKERS, 8)  # Limit to 8 workers to avoid rate limiting
    print(f"Uploading {len(paths)} files using {max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all upload tasks
        future_to_path = {
            executor.submit(_upload_single_file, Path(p), upload_cache, ext_to_mime): p 
            for p in paths
            if Path(p).exists()  # Only include files that exist
        }
        
        # Process results as they complete
        for future in as_completed(future_to_path):
            path = future_to_path[future]
            try:
                result, cache_key, file_name = future.result()
                if result:
                    uploaded.append(result)
                    if cache_key and file_name:
                        upload_cache[cache_key] = file_name
                        cache_updated = True
                else:
                    print(f"Warning: Failed to upload {path}")
            except Exception as e:
                print(f"Error processing {path}: {e}")
    
    # Save updated cache if we made any changes
    if cache_updated and PERSIST_UPLOADS:
        _save_upload_cache(upload_cache)
    
    print(f"Successfully uploaded {len(uploaded)}/{len(paths)} files")
    return uploaded

def _wait_for_files_active(files, timeout_seconds: int = 180, poll_seconds: int = 2):
    if not files:
        return []
    deadline = time.time() + timeout_seconds
    remaining = {f.name for f in files}
    last_states = {}
    while remaining and time.time() < deadline:
        next_remaining = set()
        for fid in list(remaining):
            try:
                f = genai.get_file(name=fid)
                state = getattr(getattr(f, 'state', None), 'name', None)
                last_states[fid] = state
                if state == 'ACTIVE':
                    continue
                elif state == 'FAILED':
                    print(f"File processing failed: {fid}")
                else:
                    next_remaining.add(fid)
            except Exception as e:
                print(f"Error checking file {fid}: {e}")
        remaining = next_remaining
        if remaining:
            time.sleep(poll_seconds)
    # Return only ACTIVE files
    ready = []
    for f in files:
        try:
            g = genai.get_file(name=f.name)
            if getattr(getattr(g, 'state', None), 'name', None) == 'ACTIVE':
                ready.append(g)
        except Exception:
            pass
    return ready

def can_fetch_url(url: str, rp: Optional[RobotFileParser] = None) -> bool:
    """Check if we're allowed to fetch this URL based on robots.txt."""
    if not url.startswith('http'):
        return False
        
    if rp is None:
        rp = RobotFileParser()
        robots_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}/robots.txt"
        try:
            rp.set_url(robots_url)
            rp.read()
        except Exception as e:
            print(f"Couldn't fetch robots.txt from {robots_url}, proceeding with caution: {e}")
            return True
    
    return rp.can_fetch("*", url)

def get_links_from_page(url: str, base_domain: str) -> Set[str]:
    """Extract all internal links from a page."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        links = set()
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Convert relative URLs to absolute
            abs_url = urljoin(url, href)
            parsed = urlparse(abs_url)
            
            # Filter out non-http(s) and external links
            if parsed.scheme not in ('http', 'https'):
                continue
                
            # Only include links from the same domain
            if parsed.netloc != base_domain:
                continue
                
            # Clean up URL (remove fragments, query params)
            clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
            links.add(clean_url)
            
        return links
    except Exception as e:
        print(f"Error getting links from {url}: {e}")
        return set()

def crawl_website(start_url: str, max_pages: int = 50) -> List[str]:
    """Crawl a website starting from start_url, up to max_pages."""
    parsed_url = urlparse(start_url)
    base_domain = parsed_url.netloc
    base_url = f"{parsed_url.scheme}://{base_domain}"
    
    # Check robots.txt
    rp = RobotFileParser()
    try:
        robots_url = f"{parsed_url.scheme}://{base_domain}/robots.txt"
        rp.set_url(robots_url)
        rp.read()
        if not rp.can_fetch("*", start_url):
            print(f"Access to {start_url} is disallowed by robots.txt")
            return []
    except Exception as e:
        print(f"Couldn't fetch robots.txt, proceeding with caution: {e}")
    
    to_visit = {start_url}
    visited = set()
    all_urls = set()
    
    print(f"Crawling {base_domain} starting from {start_url}...")
    
    while to_visit and len(visited) < max_pages:
        current_url = to_visit.pop()
        
        if current_url in visited:
            continue
            
        if not can_fetch_url(current_url, rp):
            print(f"Skipping {current_url} - disallowed by robots.txt")
            continue
            
        print(f"Crawling: {current_url}")
        visited.add(current_url)
        all_urls.add(current_url)
        
        # Get links and add to queue
        new_links = get_links_from_page(current_url, base_domain)
        to_visit.update(new_links - visited)
        
        # Be nice to the server
        time.sleep(1)
    
    print(f"Crawling complete. Found {len(all_urls)} pages.")
    return list(all_urls)

def fetch_website_content(url: str, crawl: bool = True, max_pages: int = 50):
    """Fetch and process website content, with optional crawling."""
    try:
        urls_to_fetch = [url]
        
        if crawl:
            print(f"\n=== Starting crawl of {url} ===")
            urls_to_fetch = crawl_website(url, max_pages)
            if not urls_to_fetch:
                print("No pages found to crawl, using only the provided URL")
                urls_to_fetch = [url]
        else:
            print(f"\n=== Fetching single page: {url} ===")
        
        all_content = []
        
        for page_url in urls_to_fetch:
            print(f"Fetching content from: {page_url}")
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                response = requests.get(page_url, headers=headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Remove unwanted elements
                for element in soup(['script', 'style', 'nav', 'footer', 'header', 'iframe', 'noscript']):
                    element.decompose()
                    
                # Get page title and main content
                title = soup.title.string if soup.title else 'No title'
                text = ' '.join(soup.stripped_strings)
                
                all_content.append(f"# {title}\nURL: {page_url}\n\n{text}\n")
                print(f"Processed: {title} ({len(text)} chars)")
                
            except Exception as e:
                print(f"Error processing {page_url}: {e}")
            
            # Be nice to the server
            time.sleep(0.5)
        
        if not all_content:
            print("No content was fetched from any pages.")
            return None
            
        # Create a temporary text file with all the content
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write("\n\n".join(all_content))
            print(f"Saved website content to temporary file: {f.name}")
            return f.name
            
    except Exception as e:
        print(f"Error in fetch_website_content: {e}")
        import traceback
        traceback.print_exc()
        return None

def prepare_gemini_files():
    """Prepare documents based on SOURCES configuration."""
    all_files = []
    website_files = []  # Initialize website_files here
    temp_files = []  # Track temporary files for cleanup
    
    try:
        # Process sources based on configuration
        if SOURCES in ['both', 'docs']:
            paths = _find_doc_paths(DOCS_DIR)
            if not paths:
                print("No documents found in the specified directory.")
                return []
                
            print(f"\n=== Found {len(paths)} local documents in {DOCS_DIR} ===")
            
            # Convert markdown files to text files for Gemini
            processed_paths = []
            
            for path in paths:
                try:
                    path = Path(path)  # Ensure we have a Path object
                    if not path.exists():
                        print(f"Warning: File not found, skipping: {path}")
                        continue
                        
                    print(f"Processing: {path.name}")
                    
                    if path.suffix.lower() in ('.md', '.markdown'):
                        # Create a text version of markdown files
                        try:
                            with open(path, 'r', encoding='utf-8') as f:
                                content = f.read()
                            # Create a temporary file for the combined content
                            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
                                temp_file_path = Path(f.name)
                                f.write(f"# {path.name}\n\n{content}")
                            processed_paths.append(temp_file_path)
                            temp_files.append(temp_file_path)  # Track for cleanup
                            print(f"  Converted to text: {temp_file_path.name}")
                        except Exception as e:
                            print(f"Error processing {path}: {e}")
                    else:
                        processed_paths.append(path)
                        print(f"  Added to upload queue: {path.name}")
                except Exception as e:
                    print(f"Error handling {path}: {e}")
            
            print(f"\n=== Uploading {len(processed_paths)} processed documents to Gemini ===")
            
            if processed_paths:
                uploaded = _upload_pdfs(processed_paths)
                ready = _wait_for_files_active(uploaded)
                
                if ready:
                    all_files.extend(ready)
                    names = "\n- " + "\n- ".join(getattr(f, 'display_name', getattr(f, 'name', str(f))) for f in ready)
                    print(f"\n=== Successfully processed {len(ready)} local documents ==={names}")
                else:
                    print("Warning: No local documents were successfully processed")
            else:
                print("No valid documents found to process after filtering")
    
    finally:
        # Always clean up temporary files, even if there was an error
        if temp_files:
            print("\n=== Cleaning up temporary files ===")
            for temp_file in temp_files:
                try:
                    if temp_file.exists():
                        temp_file.unlink()
                        print(f"  Deleted temporary file: {temp_file}")
                except Exception as e:
                    print(f"  Warning: Could not delete temporary file {temp_file}: {e}")
    
    # Process website content if enabled
    if SOURCES in ['both', 'website'] and WEBSITE_URLS:
        print("\n=== Processing website content ===")
        website_files = []
        for url in WEBSITE_URLS:
            file_path = None
            try:
                print(f"Fetching content from: {url}")
                file_path = fetch_website_content(url, crawl=True, max_pages=50)
                if file_path and os.path.exists(file_path):
                    print(f"  Successfully processed website content: {file_path}")
                    website_files.append(file_path)
                else:
                    print(f"  Failed to process website: {url} - No content returned")
            except Exception as e:
                print(f"Error processing website {url}: {e}")
                import traceback
                traceback.print_exc()
            finally:
                # Clean up temporary files
                if file_path and os.path.exists(file_path):
                    try:
                        os.unlink(file_path)
                        print(f"  Cleaned up temporary file: {file_path}")
                    except Exception as e:
                        print(f"  Warning: Could not clean up temporary file {file_path}: {e}")
        
        # Combine files based on priority
        if SOURCES == 'both':
            # When using both, website content comes first
            all_files = website_files + all_files
        elif SOURCES == 'website':
            all_files = website_files
    # else: all_files already contains just docs
    
    # Print summary
    print("\n=== Source Configuration ===")
    print(f"Active sources: {SOURCES.upper()}")
    if SOURCES in ['both', 'docs']:
        local_count = len(all_files) - len(website_files) if SOURCES == 'both' else len(all_files)
        print(f"Local documents: {local_count} files")
    if SOURCES in ['both', 'website']:
        print(f"Website content: {len(website_files)} URLs processed")
    
    if not all_files:
        print("WARNING: No files were successfully processed. Continuing without document context.")
    else:
        print("\n=== Ready Files ===")
        for i, f in enumerate(all_files, 1):
            source_type = "WEBSITE" if i <= len(website_files) and SOURCES == 'both' else \
                        "WEBSITE" if SOURCES == 'website' else "DOCUMENT"
            name = getattr(f, 'display_name', getattr(f, 'name', str(f)))
            print(f"{i}. [{source_type}] {name}")
    
    return all_files

def _backoff_sleep(attempt: int, base: float = 2.0, factor: float = 2.0, jitter: float = 0.5):
    delay = base * (factor ** (attempt - 1))
    delay *= 1.0 + random.uniform(0.0, jitter)
    time.sleep(min(delay, 30.0))

def generate_with_retry(model_obj, inputs, max_attempts: int = 5):
    attempt = 1
    while True:
        try:
            return model_obj.generate_content(inputs)
        except (gcloud_exceptions.DeadlineExceeded,
                gcloud_exceptions.ServiceUnavailable,
                gcloud_exceptions.InternalServerError,
                requests.exceptions.ConnectionError) as e:
            if attempt >= max_attempts:
                raise e
            print(f"Gemini transient error (attempt {attempt}/{max_attempts}): {e}. Retrying...")
            _backoff_sleep(attempt)
            attempt += 1

def _normalize_compliance_statement(compliance_statement: str, allowed_doc_names):
    """Normalize model output to either a valid statement or 'not_found'."""
    if not compliance_statement:
        return 'not_found'

    normalized_reply = compliance_statement.strip().lower()
    not_found_indicators = [
        'not_found',
        'insufficient information',
        'insufficient evidence',
        'cannot be found',
        'not found in the provided documents',
    ]
    if (not normalized_reply) or any(ind in normalized_reply for ind in not_found_indicators):
        return 'not_found'

    # Enforce that cited document is among uploaded files; otherwise mark as not_found
    if allowed_doc_names:
        lower_stmt = compliance_statement.lower()
        if not any(name and name.lower() in lower_stmt for name in allowed_doc_names):
            return 'not_found'

    return compliance_statement

def update_cell_with_retry(sheet, row: int, col: int, value: str, max_attempts: int = 5):
    attempt = 1
    current_sheet = sheet
    while True:
        try:
            current_sheet.update_cell(row, col, value)
            return current_sheet
        except (GSpreadAPIError, requests.exceptions.ConnectionError) as e:
            if attempt >= max_attempts:
                raise e
            print(f"Sheets transient error (attempt {attempt}/{max_attempts}): {e}. Reconnecting and retrying...")
            _backoff_sleep(attempt)
            # Recreate client and worksheet
            client_re = setup_sheets_client()
            try:
                spreadsheet_re = client_re.open_by_key(SPREADSHEET_ID)
                worksheets_re = spreadsheet_re.worksheets()
                # Keep the same worksheet index if possible
                idx = WORKSHEET_INDEX if 0 <= WORKSHEET_INDEX < len(worksheets_re) else 0
                current_sheet = worksheets_re[idx]
            except Exception as open_err:
                print(f"Failed to reopen spreadsheet during retry: {open_err}")
            attempt += 1

def _find_header_column_index(sheet, possible_names):
    """Return 1-based column index by matching headers case-insensitively; None if not found."""
    try:
        headers = sheet.row_values(1)
    except Exception as _:
        return None
    normalized_headers = [h.strip().lower() for h in headers]
    for name in possible_names:
        lower = name.strip().lower()
        if lower in normalized_headers:
            return normalized_headers.index(lower) + 1
    return None

def _column_index_to_letter(index_one_based: int):
    """Convert 1-based column index to A1 column letter(s)."""
    result = ""
    n = int(index_one_based)
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

def setup_sheets_client():
    """Initialize Google Sheets client.

    Prefers Service Account (non-interactive, reliable). Falls back to user OAuth.
    """
    global ACTIVE_AUTH, SERVICE_ACCOUNT_EMAIL

    # 1) Try Service Account
    try:
        sa_path_env = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        sa_path = os.path.abspath(os.path.expanduser(sa_path_env or SERVICE_ACCOUNT_FILE))
        if os.path.isfile(sa_path):
            creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
            SERVICE_ACCOUNT_EMAIL = getattr(creds, 'service_account_email', None)
            client = gspread.authorize(creds)
            ACTIVE_AUTH = "service_account"
            return client
        else:
            print(f"Service Account JSON not found at: {sa_path}")
    except Exception as e:
        print(f"Service Account auth failed: {e}")

    # 2) Fallback to OAuth
    try:
        cred_path = os.path.expanduser('~/.config/gspread/credentials.json')
        auth_user_path = os.path.expanduser('~/.config/gspread/authorized_user.json')
        client = gspread.oauth(
            credentials_filename=cred_path,
            authorized_user_filename=auth_user_path
        )
        ACTIVE_AUTH = "oauth"
        return client
    except FileNotFoundError:
        print("OAuth credentials not found. Please set up Google Sheets API:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Enable Google Sheets API")
        print("3. Create OAuth credentials")
        print("4. Download and save as ~/.config/gspread/credentials.json")
        raise SystemExit(1)
    except Exception as e:
        print(f"Authentication failed: {e}")
        raise SystemExit(1)

def process_requirements():
    """Main function to process requirements and update sheets"""
    # Connect to Google Sheets
    client = setup_sheets_client()
    
    print(f"Attempting to open spreadsheet with ID: {SPREADSHEET_ID}")
    
    try:
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        print(f"Successfully opened: {spreadsheet.title}")
        
        # List available sheets
        worksheets = spreadsheet.worksheets()
        print(f"Available sheets: {[ws.title for ws in worksheets]}")
        
        # Use selected worksheet index
        if WORKSHEET_INDEX < 0 or WORKSHEET_INDEX >= len(worksheets):
            print(f"WORKSHEET_INDEX {WORKSHEET_INDEX} out of range, defaulting to 0")
            selected_index = 0
        else:
            selected_index = WORKSHEET_INDEX
        sheet = worksheets[selected_index]
        print(f"Using sheet: {sheet.title} (index {selected_index})")
        
    except Exception as e:
        print(f"Error opening spreadsheet: {e}")
        print("Please check:")
        print("1. Spreadsheet ID is correct")
        print("2. Sheet is a regular Google Sheets document (not Excel import)")
        print("3. You have edit access to the sheet")
        if ACTIVE_AUTH == "service_account":
            hint_email = SERVICE_ACCOUNT_EMAIL or "your service account"
            print(f"4. Share the sheet with the service account email: {hint_email}")
        return
    
    # Prepare document context based on SOURCES configuration
    print("\n" + "="*50)
    print(f"PREPARING SOURCES (Mode: {SOURCES.upper()})")
    print("="*50)
    
    provided_files = prepare_gemini_files()
    
    # Build allowed document names list for citation
    allowed_doc_names = []
    if provided_files:
        for _f in provided_files:
            display_name = getattr(_f, 'display_name', None)
            name_fallback = getattr(_f, 'name', '')
            base_name = os.path.basename(name_fallback) if name_fallback else None
            
            # For website content, use the URL as the display name
            if 'http' in str(display_name or ''):
                allowed_doc_names.append(display_name)
            else:
                allowed_doc_names.append(display_name or base_name)
    
    # Add website URLs directly to the allowed document names if using website sources
    if SOURCES in ['both', 'website']:
        for url in WEBSITE_URLS:
            if url not in '\n'.join(allowed_doc_names):
                allowed_doc_names.append(f"Website: {url}")
    
    print("\n" + "="*50)
    print(f"SOURCE PREPARATION COMPLETE")
    print(f"Total sources available: {len(provided_files)}")
    print("="*50)
    
    allowed_doc_names_text = "\n".join(f"- {n}" for n in allowed_doc_names) if allowed_doc_names else ""
    
    # Resolve column indexes dynamically by header
    requirement_col_index = _find_header_column_index(sheet, [
        'Requirement',
        'requirement',
    ])
    compliance_col_index = _find_header_column_index(sheet, [
        'Compliance Statement',
        'Compliance_Statement',
        'compliance statement',
        'compliance_statement',
    ])

    if requirement_col_index is None:
        print("Could not find 'Requirement' column header. Please ensure row 1 has a 'Requirement' header.")
        return
    if compliance_col_index is None:
        print("Could not find 'Compliance Statement' column header. Please ensure row 1 has a 'Compliance Statement' header.")
        return
    print(f"Detected columns -> Requirement: {requirement_col_index}, Compliance: {compliance_col_index}")

    # Collect rows to process by reading raw columns to preserve physical row numbers
    rows_to_process = []  # list[(row_index, requirement_text)]
    try:
        requirement_col_values = sheet.col_values(requirement_col_index)
        compliance_col_values = sheet.col_values(compliance_col_index)
    except Exception as e:
        print(f"Failed to read column values: {e}")
        return

    # Ensure both lists cover the same number of rows for safe indexing
    max_len = max(len(requirement_col_values), len(compliance_col_values))
    # Pad lists to max_len
    requirement_col_values += [''] * (max_len - len(requirement_col_values))
    compliance_col_values += [''] * (max_len - len(compliance_col_values))

    for physical_row in range(2, max_len + 1):  # start from row 2 (after header)
        requirement_text = requirement_col_values[physical_row - 1]
        compliance_value = compliance_col_values[physical_row - 1]
        if (not requirement_text or not requirement_text.strip()) or (compliance_value and compliance_value.strip()):
            print(f"Skipping row {physical_row} - already processed or empty")
            continue
        rows_to_process.append((physical_row, requirement_text))

    if not rows_to_process:
        print("No new requirements to process.")
        print("Processing complete!")
        return

    print(f"Submitting {len(rows_to_process)} rows to Gemini (max_workers={MAX_WORKERS})...")

    def _build_prompt(req_text: str) -> str:
        source_instructions = ""
        if SOURCES == 'both':
            source_instructions = """# SOURCE PRIORITY (MUST FOLLOW):
1. FIRST check ALL website content for relevant information
2. ONLY if no relevant website content is found, check local documents
3. NEVER mix information from different sources

# IMPORTANT:
- WEBSITE CONTENT TAKES PRECEDENCE OVER LOCAL DOCUMENTS
- If ANY website content is relevant, you MUST use it and IGNORE local documents
- Only look at local documents if ALL website content is irrelevant"""
        elif SOURCES == 'website':
            source_instructions = """# SOURCE INSTRUCTIONS:
- Use ONLY the provided website content
- If no website content is relevant, respond with: not_found
- Do not reference or use any local documents"""
        else:  # docs only
            source_instructions = """# SOURCE INSTRUCTIONS:
- Use ONLY the provided local documents
- If no document is relevant, respond with: not_found"""

        prompt = f"""# INSTRUCTION: EVALUATE REQUIREMENT USING SPECIFIED SOURCES
# ACTIVE SOURCES: {SOURCES.upper()}

Requirement to evaluate:
"{req_text}"

{source_instructions}

# RESPONSE REQUIREMENTS:
- Base your response on the most relevant single source
- Be specific about which part of the source supports your answer
- Keep the entire response on a single line (no newlines)
- Keep the reasoning concise (<= 40 words)
- If the requirement is multi-part, clearly indicate which parts are addressed
- If no source contains relevant information, respond with exactly: not_found

# RESPONSE FORMAT (follow exactly):
"[Compliant/Non-compliant/Partially Compliant] - [brief reasoning] (Reference: [Source identifier], [specific section if applicable])"

# CRITICAL REMINDERS:
- NEVER combine information from multiple sources
- Choose the single best source for your response
- If uncertain, respond with: not_found

Allowed document names and URLs (you must cite exactly one of these when providing a reference):
{allowed_doc_names_text}"""
        return prompt

    def _worker_generate(req_text: str) -> str:
        prompt = _build_prompt(req_text)
        inputs = [prompt] + provided_files if provided_files else [prompt]
        # Create a local model instance per thread for safety
        local_model = genai.GenerativeModel('gemini-1.5-pro')
        response = generate_with_retry(local_model, inputs)
        compliance_statement = getattr(response, 'text', '')
        compliance_statement = compliance_statement.strip() if compliance_statement else ''
        return _normalize_compliance_statement(compliance_statement, allowed_doc_names)

    # Run Gemini generations concurrently and write each result as it completes
    with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS)) as executor:
        future_to_row = {
            executor.submit(_worker_generate, req_text): row
            for (row, req_text) in rows_to_process
        }
        for future in as_completed(future_to_row):
            row = future_to_row[future]
            try:
                value = future.result()
            except Exception as e:
                value = f"ERROR: {e}"
            print(f"✓ Gemini completed for row {row}")
            try:
                sheet = update_cell_with_retry(sheet, row, compliance_col_index, value)
                # Optional verification and A1 fallback
                if VERIFY_WRITES:
                    try:
                        read_back = sheet.cell(row, compliance_col_index).value or ''
                        if not read_back.strip():
                            col_letter = _column_index_to_letter(compliance_col_index)
                            a1 = f"{col_letter}{row}"
                            print(f"Write verification failed for row {row}. Retrying with range update at {a1}...")
                            sheet.update(a1, [[value]])
                    except Exception as _verify_err:
                        print(f"Verification error for row {row}: {_verify_err}")
                print(f"✓ Updated row {row}")
                time.sleep(0.5)  # gentle pacing for Sheets API
            except Exception as e:
                print(f"✗ Error updating row {row}: {e}")
                time.sleep(0.5)

    print("Processing complete!")

if __name__ == "__main__":
    # Run the actual processing
    process_requirements()