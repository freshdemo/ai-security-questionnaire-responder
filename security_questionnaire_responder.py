
import gspread
import google.generativeai as genai
import time
import os
import sys
from google.oauth2.service_account import Credentials
from pathlib import Path
import random
import requests
import tempfile
import json
import hashlib
import re
from bs4 import BeautifulSoup
from google.api_core import exceptions as gcloud_exceptions
from gspread.exceptions import APIError as GSpreadAPIError
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser
from typing import Set, List, Optional, Dict, Any, Tuple, NamedTuple
import difflib

def _find_best_source_match(reference: str, allowed_sources: List[str]) -> Optional[str]:
    """Find the best matching source from allowed sources."""
    if not reference or not allowed_sources:
        return None
    
    # Try exact match first
    reference_lower = reference.lower()
    for src in allowed_sources:
        if reference_lower == src.lower():
            return src
    
    # Try partial match
    for src in allowed_sources:
        if reference_lower in src.lower() or src.lower() in reference_lower:
            return src
    
    # Try fuzzy matching
    matches = difflib.get_close_matches(reference, allowed_sources, n=1, cutoff=0.6)
    return matches[0] if matches else None

def _extract_references(text: str) -> List[str]:
    """Extract source references from text."""
    # Look for patterns like "Reference: X", "Source: X", "(Source: X)", etc.
    patterns = [
        r'(?:Reference|Source|Document|From)[: ]+([^\n\),;]+)',
        r'\(\s*(?:Reference|Source|Document|From)[: ]+([^)\\n]+)\)',
        r'\[(?:Reference|Source|Document|From)[: ]+([^\]]+)\]'
    ]
    
    references = []
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            # Clean up the reference
            ref = re.sub(r'^[\s\-\*]+|[\s\-\*]+$', '', match)
            if ref and len(ref) > 2:  # Filter out very short references
                references.append(ref)
    
    return references

class ProcessedFile(NamedTuple):
    """Container for processed file information."""
    path: Path
    source_url: Optional[str] = None

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

def _extract_source_url(content: str) -> Optional[str]:
    """Extract SOURCE_URL comment from markdown content.
    
    Args:
        content: The markdown content to search in
        
    Returns:
        The source URL if found, None otherwise
    """
    # Look for <!-- SOURCE_URL: http://example.com --> pattern
    match = re.search(r'<!--\s*SOURCE_URL\s*:\s*(https?://[^\s>]+)\s*-->', content, re.IGNORECASE)
    return match.group(1) if match else None

def _get_file_fingerprint(file_path: Path) -> str:
    """Generate a fingerprint for a file based on its full content."""
    hasher = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):  # Read in 8KB chunks
                hasher.update(chunk)
        return hasher.hexdigest()
    except IOError as e:
        print(f"  ⚠️  Could not read file for fingerprinting: {file_path.name}, {e}")
        # Return a random hash to ensure it's treated as a new file
        return hashlib.md5(os.urandom(16)).hexdigest()

def _upload_single_file(p: Path, upload_cache: dict, ext_to_mime: dict, source_url: Optional[str] = None):
    """Helper function to upload a single file with proper error handling.
    
    Args:
        p: Path to the file to upload
        upload_cache: Dictionary containing cached uploads
        ext_to_mime: Mapping of file extensions to MIME types
        source_url: Optional source URL to use as the display name
        
    Returns:
        Tuple of (uploaded_file, cache_key, display_name) or (None, None, None) on failure
    """
    def _log_error(msg: str, error: Optional[Exception] = None):
        """Helper to log errors consistently"""
        error_msg = f"  ✗ {msg}"
        if error:
            error_msg += f": {str(error)}"
        print(error_msg)
    
    try:
        # Input validation
        if not p:
            _log_error("No file path provided")
            return None, None, None
            
        if not p.exists():
            _log_error(f"File does not exist: {p}")
            return None, None, None
            
        # Get file info
        abs_path = str(p.absolute())
        file_size_mb = p.stat().st_size / (1024 * 1024)  # Size in MB
        file_fingerprint = _get_file_fingerprint(p)
        display_name = source_url or p.name
        
        # Check cache first - use filename and content hash as key
        cache_key = f"{p.name}:{file_fingerprint}"
        if cache_key in upload_cache and upload_cache[cache_key]:
            print(f"  🔍 Found in cache: {display_name}")
            try:
                # Get the file from Gemini using the cached file ID
                file_obj = genai.get_file(name=upload_cache[cache_key])
                # Double-check the file is still valid
                if file_obj.state.name == 'ACTIVE':
                    print(f"  ✓ Using cached version: {file_obj.name}")
                    return file_obj, cache_key, display_name
                else:
                    print(f"  ⚠️ Cached file is {file_obj.state.name}, re-uploading...")
                    # Remove the invalid cache entry
                    del upload_cache[cache_key]
            except Exception as e:
                _log_error(f"Failed to retrieve cached file {cache_key}", e)
                # Remove the invalid cache entry
                if cache_key in upload_cache:
                    del upload_cache[cache_key]
        
        # Check file size (Gemini has upload limits)
        if file_size_mb > 20:  # 20MB limit for Gemini
            _log_error(f"File too large ({file_size_mb:.2f}MB > 20MB): {p.name}")
            return None, None, None
            
        # Get MIME type
        mime = ext_to_mime.get(p.suffix.lower())
        if not mime:
            _log_error(f"Unsupported file type: {p.suffix}")
            return None, None, None
            
        # Upload the file
        print(f"  ⬆️  Uploading: {display_name} ({file_size_mb:.2f}MB)")
        try:
            start_time = time.time()
            f = genai.upload_file(path=str(p), mime_type=mime)
            upload_time = time.time() - start_time
            speed_mbps = file_size_mb / upload_time if upload_time > 0 else 0
            
            print(f"  ✓ Uploaded in {upload_time:.1f}s ({speed_mbps:.1f} MB/s): {f.name}")
            return f, cache_key, display_name
            
        except Exception as upload_error:
            _log_error(f"Upload failed for {p.name}", upload_error)
            return None, None, None
            
    except Exception as e:
        _log_error(f"Unexpected error processing {p.name if p else 'unknown file'}", e)
        return None, None, None

def _upload_pdfs(paths, timeout_seconds=300):
    """
    Upload files to Gemini in parallel, reusing existing uploads when possible.
    
    Args:
        paths: List of file paths or ProcessedFile objects to upload
        timeout_seconds: Maximum time to wait for uploads to complete
        
    Returns:
        List of tuples containing (file_object, display_name)
    """
    if not paths:
        print("Warning: No files to upload")
        return []
        
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
        '.md': 'text/markdown',
        '.txt': 'text/plain',
        '.text': 'text/plain'
    }
    
    # Load existing upload cache if it exists
    upload_cache = _load_upload_cache() if PERSIST_UPLOADS else {}
    cache_updated = False
    
    # Process files in parallel with a reasonable number of workers
    max_workers = min(MAX_WORKERS, 4)  # Reduced from 8 to 4 to avoid rate limiting
    print(f"\n=== Starting upload of {len(paths)} files using {max_workers} workers (timeout: {timeout_seconds}s) ===")
    
    start_time = time.time()
    processed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all upload tasks
        future_to_path = {}
        for p in paths:
            try:
                if isinstance(p, ProcessedFile):
                    path = p.path
                    source_url = p.source_url
                else:
                    path = Path(p) if not isinstance(p, Path) else p
                    source_url = None
                
                if not path or not path.exists():
                    print(f"Warning: File does not exist, skipping: {getattr(p, 'name', str(p))}")
                    continue
                    
                future = executor.submit(_upload_single_file, path, upload_cache, ext_to_mime, source_url)
                future_to_path[future] = path
                processed_count += 1
                
            except Exception as e:
                print(f"Error preparing upload for {getattr(p, 'name', str(p))}: {e}")
        
        if not future_to_path:
            print("No valid files to process after validation")
            return []
            
        print(f"Processing {len(future_to_path)} files...")
        
        # Process results as they complete with timeout
        try:
            for future in concurrent.futures.as_completed(future_to_path, timeout=timeout_seconds):
                remaining_time = timeout_seconds - (time.time() - start_time)
                if remaining_time <= 0:
                    print(f"\nUpload timeout after {timeout_seconds} seconds. Some files may not have been uploaded.")
                    break
                    
                path = future_to_path[future]
                try:
                    result, cache_key, display_name = future.result(timeout=min(60, remaining_time))
                    if result and cache_key and display_name:
                        # Store the file and its display name as a tuple
                        uploaded.append((result, display_name))
                        # Store the file name in the cache, not the display name
                        upload_cache[cache_key] = result.name  # Store the Gemini file ID
                        cache_updated = True
                        print(f"  ✓ Successfully processed: {display_name}")
                    else:
                        print(f"  ✗ Failed to process: {getattr(path, 'name', str(path))}")
                        
                except concurrent.futures.TimeoutError:
                    print(f"\nTimeout processing {getattr(path, 'name', str(path))}. Skipping...")
                except Exception as e:
                    print(f"Error processing {getattr(path, 'name', str(path))}: {e}")
                    
        except concurrent.futures.TimeoutError:
            print(f"\nOverall upload timeout after {timeout_seconds} seconds. Some files may not have been processed.")
        except KeyboardInterrupt:
            print("\nUpload process interrupted by user. Cleaning up...")
            raise
        except Exception as e:
            print(f"\nUnexpected error during upload: {e}")
    
    # Save updated cache if we made any changes
    if cache_updated and PERSIST_UPLOADS:
        try:
            _save_upload_cache(upload_cache)
        except Exception as e:
            print(f"Warning: Could not save upload cache: {e}")
    
    elapsed = time.time() - start_time
    success_count = len(uploaded)
    failed_count = processed_count - success_count
    
    print("\n" + "="*50)
    print(f"UPLOAD SUMMARY")
    print("="*50)
    print(f"Total files processed: {processed_count}")
    print(f"Successfully uploaded: {success_count}")
    if failed_count > 0:
        print(f"Failed uploads: {failed_count}")
    print(f"Time taken: {elapsed:.1f} seconds")
    print("="*50)
    
    return uploaded

def _wait_for_files_active(uploaded_files, timeout_seconds: int = 600, initial_poll: int = 5, max_poll: int = 60):
    """
    Wait for all uploaded files to become active using an exponential backoff polling strategy
    to avoid triggering API rate limits.
    
    Args:
        uploaded_files: List of (file_object, display_name) tuples from _upload_pdfs
        timeout_seconds: Maximum time to wait for all files to become active (default: 600s/10min)
        initial_poll: The first wait time between status checks (default: 5s)
        max_poll: The maximum wait time between status checks for a single file (default: 60s)
        
    Returns:
        List of (file_object, display_name) tuples for files that became active
    """
    if not uploaded_files:
        print("No files to wait for - empty input list")
        return []
        
    start_time = time.time()
    active_files = []
    failed_files = []
    
    # Create a dictionary to track each file's polling state
    pending_files = {
        file_obj.name: {
            "file_info": (file_obj, display_name),
            "poll_interval": initial_poll, # Time to wait before next check
            "next_check": time.time() + initial_poll # The time at which we should next check this file
        } for file_obj, display_name in uploaded_files if file_obj
    }

    print(f"\n🔄 Waiting for {len(pending_files)} files to become active (timeout: {timeout_seconds}s)...")

    while pending_files and (time.time() - start_time) < timeout_seconds:
        current_time = time.time()
        
        # Find files that are ready to be checked
        files_to_check = [
            name for name, state in pending_files.items() if current_time >= state["next_check"]
        ]

        if not files_to_check:
            # If no files are ready for a check, sleep for a short duration to avoid a busy-wait loop
            time.sleep(1)
            continue

        for name in files_to_check:
            state = pending_files[name]
            file_obj, display_name = state["file_info"]

            try:
                # Get the latest status of the file
                file_status = genai.get_file(name=file_obj.name)
                
                if file_status.state.name == 'ACTIVE':
                    print(f"  ✅ {display_name} is now ACTIVE")
                    # Store both file object and display name as a tuple
                    active_files.append((file_obj, display_name))
                    del pending_files[name] # Remove from pending list
                    
                elif file_status.state.name == 'FAILED':
                    error_msg = getattr(file_status, 'error', 'No error details')
                    print(f"  ❌ {display_name} FAILED: {error_msg}")
                    failed_files.append((file_obj, display_name, error_msg))
                    del pending_files[name] # Remove from pending list
                    
                else: # Still PROCESSING or in another state
                    # Increase the poll interval for the next check (exponential backoff)
                    state["poll_interval"] = min(state["poll_interval"] * 1.5, max_poll)
                    state["next_check"] = time.time() + state["poll_interval"]
                    print(f"  ⏳ {display_name} is {file_status.state.name}. Retrying in {state['poll_interval']:.0f}s...")

            except Exception as e:
                # If a check fails, increase interval and retry
                state["poll_interval"] = min(state["poll_interval"] * 2, max_poll)
                state["next_check"] = time.time() + state["poll_interval"]
                print(f"  ⚠️  Error checking {display_name}: {e}. Retrying in {state['poll_interval']:.0f}s...")

    # Final status update
    print("\n" + "="*50)
    print("📋 Final Status:")
    print(f"  ✅ Successfully activated: {len(active_files)}/{len(uploaded_files)} files")
    
    if failed_files:
        print(f"\n❌ Failed files ({len(failed_files)}):")
        for file_obj, display_name, error in failed_files:
            print(f"  - {display_name}: {error}")
    
    if pending_files:
        timed_out_count = len(pending_files)
        print(f"\n⚠️  Timed out waiting for {timed_out_count} files:")
        for name, state in pending_files.items():
            print(f"  - {state['file_info'][1]}")
    
    print("="*50 + "\n")
    # Return list of (file_object, display_name) tuples for active files
    return active_files

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

def cleanup_temp_files(temp_files):
    """Safely clean up temporary files."""
    if not temp_files:
        return
        
    print("\n=== Cleaning up temporary files ===")
    for temp_file in temp_files:
        try:
            if temp_file and isinstance(temp_file, (str, Path)):
                temp_path = Path(temp_file)
                if temp_path.exists():
                    temp_path.unlink()
                    print(f"  Deleted temporary file: {temp_path}")
        except Exception as e:
            print(f"  Warning: Could not delete temporary file {temp_file}: {e}")

def prepare_gemini_files():
    """
    Prepare documents based on SOURCES configuration.
    
    Returns:
        List of (file_object, display_name) tuples for successfully processed files
    """
    all_files = []
    website_files = []
    temp_files = []
    
    # Verify Gemini API is configured
    if not GEMINI_API_KEY:
        print("Error: GEMINI_API_KEY is not set. Cannot prepare files for Gemini.")
        return []
    
    try:
        # Configure Gemini API
        genai.configure(api_key=GEMINI_API_KEY)
        print("Gemini API configured successfully")
        # Process sources based on configuration
        if SOURCES in ['both', 'docs']:
            paths = _find_doc_paths(DOCS_DIR)
            if not paths:
                print("No documents found in the specified directory.")
                return []
                
            print(f"\n=== Found {len(paths)} local documents in {DOCS_DIR} ===")
            
            # Process documents
            processed_files = []
            
            for path in paths:
                try:
                    path = Path(path)  # Ensure we have a Path object
                    if not path.exists():
                        print(f"Warning: File not found, skipping: {path}")
                        continue
                        
                    print(f"Processing: {path.name}")
                    
                    if path.suffix.lower() in ('.md', '.markdown'):
                        # For markdown files, extract SOURCE_URL if present
                        try:
                            with open(path, 'r', encoding='utf-8') as f:
                                content = f.read()
                            
                            # Extract SOURCE_URL if present
                            source_url = _extract_source_url(content)
                            
                            # Create a temporary file for the content
                            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
                                temp_file_path = Path(f.name)
                                f.write(f"# {path.name}\n\n{content}")
                            
                            processed_files.append(ProcessedFile(
                                path=temp_file_path,
                                source_url=source_url
                            ))
                            temp_files.append(temp_file_path)
                            print(f"  Converted to text: {temp_file_path.name}")
                            if source_url:
                                print(f"  Using source URL: {source_url}")
                                
                        except Exception as e:
                            print(f"Error processing {path}: {e}")
                    else:
                        # For non-markdown files, just add them as-is
                        processed_files.append(ProcessedFile(path=path))
                        print(f"  Added to upload queue: {path.name}")
                        
                except Exception as e:
                    print(f"Error handling {path}: {e}")
            
            print(f"\n=== Uploading {len(processed_files)} processed documents to Gemini ===")
            
            if processed_files:
                uploaded = _upload_pdfs(processed_files)
                ready = _wait_for_files_active(uploaded)
                
                if ready:
                    # Store both file objects and display names for the final result
                    all_files.extend(ready)  # Each item is (file_obj, display_name)
                    
                    # Print summary with display names
                    names = "\n- " + "\n- ".join(display_name for _, display_name in ready)
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
    # Initialize Gemini API
    if not GEMINI_API_KEY:
        print("Error: GEMINI_API_KEY environment variable is not set")
        print("Please set the GEMINI_API_KEY environment variable and try again")
        return
    
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        print("Successfully initialized Gemini API")
    except Exception as e:
        print(f"Error initializing Gemini API: {e}")
        return
    
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
        for file_obj, display_name in provided_files:
            if not file_obj:
                continue
                
            # Use the display name we stored during upload
            if display_name:
                allowed_doc_names.append(display_name)
            else:
                # Fallback to file name if no display name
                file_name = getattr(file_obj, 'name', '')
                if file_name:
                    base_name = os.path.basename(file_name)
                    allowed_doc_names.append(base_name)
    
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

    def _build_prompt(req_text: str, first_pass: bool = True) -> str:
        if first_pass:
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
# PASS: FIRST PASS (QUICK SCAN)

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
        else:
            # Second pass - more thorough analysis
            prompt = f"""# INSTRUCTION: RE-ANALYZE REQUIREMENT WITH DEEPER CONTEXT
# ACTIVE SOURCES: {SOURCES.upper()}
# PASS: SECOND PASS (DETAILED ANALYSIS)

Requirement to evaluate (please analyze carefully):
"{req_text}"

# INSTRUCTIONS:
1. Perform a DEEPER SEARCH through all available sources
2. Pay special attention to:
   - Specific sections or page numbers
   - Related policies or procedures
   - Implementation details
3. If the requirement has multiple parts, address EACH part specifically
4. Be as detailed and precise as possible

# RESPONSE REQUIREMENTS:
- Base your response on the most relevant single source
- Be VERY specific about which part of the source supports your answer
- Include section numbers, page numbers, or specific document locations
- Keep the entire response on a single line (no newlines)
- If the requirement is multi-part, clearly indicate which parts are addressed
- If truly no source contains relevant information, respond with: not_found

# RESPONSE FORMAT (follow exactly):
"[Compliant/Non-compliant/Partially Compliant] - [detailed reasoning with specific references] (Reference: [Source identifier], [specific section/page])"

# CRITICAL REMINDERS:
- This is a SECOND PASS - your response should be more thorough than the first pass
- If you found partial information in the first pass, look HARDER for more complete information
- If still uncertain after thorough searching, respond with: not_found

Allowed document names and URLs (you must cite exactly one of these when providing a reference):
{allowed_doc_names_text}"""
        return prompt

    def _worker_generate(req_text: str, first_pass: bool = True) -> Tuple[str, bool]:
        """Generate a response for a requirement with optional second pass.
        
        Args:
            req_text: The requirement text to process
            first_pass: Whether this is the first pass (True) or second pass (False)
            
        Returns:
            Tuple of (response_text, needs_second_pass)
        """
        prompt = _build_prompt(req_text, first_pass)
        
        # Create a local model instance per thread for safety
        local_model = genai.GenerativeModel('gemini-1.5-pro')
        
        # Prepare the input with proper file handling
        file_objects = [file_obj for file_obj, _ in (provided_files or []) if file_obj is not None]
        inputs = [prompt] + file_objects
            
        response = generate_with_retry(local_model, inputs)
        compliance_statement = getattr(response, 'text', '')
        compliance_statement = compliance_statement.strip() if compliance_statement else ''
        
        # Normalize the response
        normalized = _normalize_compliance_statement(compliance_statement, allowed_doc_names)
        
        # Check if we need a second pass
        needs_second_pass = first_pass and (
            'not_found' in normalized.lower() or 
            'uncertain' in normalized.lower() or
            'partially' in normalized.lower() or
            'i don\'t know' in normalized.lower() or
            'i do not know' in normalized.lower()
        )
        
        return normalized, needs_second_pass
        
    def _verify_and_fix_sources(response: str) -> str:
        """Verify and fix source references in the response."""
        if not response or 'not_found' in response.lower():
            return response
            
        # Extract all references from the response
        references = _extract_references(response)
        if not references:
            return response
            
        # Check each reference against allowed sources
        for ref in references:
            best_match = _find_best_source_match(ref, allowed_doc_names)
            if best_match and best_match.lower() != ref.lower():
                # Replace the reference with the best match
                response = re.sub(
                    re.escape(ref), 
                    best_match, 
                    response, 
                    flags=re.IGNORECASE
                )
                print(f"  ✓ Fixed source reference: '{ref}' -> '{best_match}'")
                
        return response

    def process_batch(rows_batch, is_second_pass=False):
        """Process a batch of rows with optional second pass."""
        print(f"\n{'='*80}")
        print(f"PROCESSING {'SECOND PASS - ' if is_second_pass else ''}BATCH OF {len(rows_batch)} ROWS")
        print(f"{'='*80}")
        
        needs_second_pass = []
        
        with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS)) as executor:
            future_to_row = {
                executor.submit(_worker_generate, req_text, not is_second_pass): (row_idx, req_text)
                for row_idx, req_text in rows_batch
            }

            for future in as_completed(future_to_row):
                row_idx, req_text = future_to_row[future]
                try:
                    compliance_statement, needs_retry = future.result()
                    
                    # Apply source verification and fixing
                    compliance_statement = _verify_and_fix_sources(compliance_statement)
                    
                    print(f"Row {row_idx}: {compliance_statement}")
                    if is_second_pass:
                        # Mark as reviewed after second pass
                        compliance_statement = f"{compliance_statement} [REVIEWED]"
                    
                    update_cell_with_retry(sheet, row_idx, compliance_col_index, compliance_statement)
                    
                    # Track rows that need a second pass
                    if needs_retry and not is_second_pass:
                        needs_second_pass.append((row_idx, req_text))
                        print(f"  → Will reprocess row {row_idx} in second pass")
                            
                except Exception as e:
                    error_msg = f"ERROR: {str(e)[:200]}"
                    print(f"Error processing row {row_idx}: {error_msg}")
                    sheet.update_cell(row_idx, compliance_col_index, error_msg)
        
        return needs_second_pass
    
    # First pass - process all rows
    needs_second_pass = process_batch(rows_to_process, is_second_pass=False)
    
    # Second pass - reprocess rows that need it
    if needs_second_pass:
        print(f"\n{'='*80}")
        print(f"STARTING SECOND PASS FOR {len(needs_second_pass)} ROWS")
        print(f"{'='*80}")
        process_batch(needs_second_pass, is_second_pass=True)
    
    print("\nProcessing complete!")

def main():
    """Main entry point with proper error handling and cleanup."""
    try:
        print("=== Starting Security Questionnaire Responder ===")
        process_requirements()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user. Cleaning up...")
    except Exception as e:
        print(f"\nAn error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        print("\n=== Processing complete. Exiting. ===")
        sys.exit(0)

if __name__ == "__main__":
    main()