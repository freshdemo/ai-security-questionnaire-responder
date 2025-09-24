import os
import json
import re
from pathlib import Path

class DocumentationProcessor:
    def __init__(self, docs_root="../docs.snyk.io/docs/"):
        self.docs_root = docs_root
        self.url_mapping = {}
        
    def process_documentation(self, output_dir="processed_docs"):
        """Process all README.md files and create properly referenced documents."""
        os.makedirs(output_dir, exist_ok=True)
        
        for root, dirs, files in os.walk(self.docs_root):
            for file in files:
                if file == 'README.md':
                    original_path = os.path.join(root, file)
                    self._process_single_file(original_path, output_dir)
        
        # Save mapping for reference
        with open(os.path.join(output_dir, 'url_mapping.json'), 'w') as f:
            json.dump(self.url_mapping, f, indent=2)
    
    def _process_single_file(self, file_path, output_dir):
        """Process a single README.md file."""
        # Generate meaningful filename and URL
        new_filename, web_url = self._create_filename_and_url(file_path)
        
        # Read original content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Embed source URL in content
        processed_content = f"""<!-- SOURCE_URL: {web_url} -->
<!-- ORIGINAL_PATH: {file_path} -->

{content}"""
        
        # Write processed file
        output_path = os.path.join(output_dir, new_filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(processed_content)
        
        # Store mapping
        self.url_mapping[new_filename] = web_url
        
        print(f"Processed: {file_path} -> {new_filename}")
    
    def _create_filename_and_url(self, original_path):
        """Create meaningful filename and corresponding web URL."""
        # Remove the docs root and /README.md
        relative_path = original_path.replace(self.docs_root, '')
        path_without_readme = relative_path.replace('/README.md', '').replace('README.md', '')
        
        # Create safe filename
        safe_name = path_without_readme.replace('/', '-').strip('-')
        if not safe_name:
            safe_name = 'root'
        filename = f"{safe_name}.md"
        
        # Create web URL
        web_url = f"https://docs.snyk.io/{path_without_readme}".rstrip('/')
        
        return filename, web_url
    
    def fix_gemini_references(self, response_text):
        """Fix references in Gemini responses using the mapping."""
        def replace_reference(match):
            full_ref = match.group(1)
            # Extract filename from reference
            filename_match = re.search(r'([^/\\]+\.md)', full_ref)
            if filename_match:
                filename = filename_match.group(1)
                if filename in self.url_mapping:
                    return f"(Reference: {self.url_mapping[filename]})"
            return match.group(0)  # Keep original if no mapping
        
        pattern = r'\(Reference: ([^)]+)\)'
        return re.sub(pattern, replace_reference, response_text)


class GeminiReferenceHandler:
    """Helper class to handle Gemini integration with proper references."""
    
    def __init__(self, processor):
        self.processor = processor
    
    def prepare_prompt_with_context(self, security_requirements, document_content):
        """Prepare prompt that encourages proper referencing."""
        prompt = f"""
Please map the following security requirements to the provided documentation.

IMPORTANT: When referencing documentation, use the SOURCE_URL provided at the top of each document.
Format references as: (Reference: [SOURCE_URL])

Security Requirements:
{security_requirements}

Documentation:
{document_content}

Please provide mappings with proper URL references.
"""
        return prompt
    
    def post_process_response(self, gemini_response):
        """Clean up Gemini response references."""
        return self.processor.fix_gemini_references(gemini_response)


# Usage Example
def main():
    # Initialize processor
    processor = DocumentationProcessor("../docs.snyk.io/docs/")
    
    # Process all documentation
    processor.process_documentation("processed_snyk_docs")
    
    # Example of using with Gemini
    handler = GeminiReferenceHandler(processor)
    
    # Your security requirements
    requirements = "Ensure all dependencies are scanned for vulnerabilities"
    
    # Document content (you'd read from processed files)
    with open("processed_snyk_docs/dependency-scanning.md", 'r') as f:
        doc_content = f.read()
    
    # Prepare prompt
    prompt = handler.prepare_prompt_with_context(requirements, doc_content)
    
    # Send to Gemini (your existing code)
    # gemini_response = your_gemini_call(prompt)
    
    # Fix references in response
    # cleaned_response = handler.post_process_response(gemini_response)
    
    print("Processing complete! Check processed_snyk_docs/ directory.")


if __name__ == "__main__":
    main()