"""Configuration management for Security Questionnaire Responder."""
import os
import sys
import json
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, List

# Default configuration
DEFAULT_CONFIG = {
    "GEMINI_API_KEY": "",
    "SPREADSHEET_ID": "1es_mPMrkUO2Ez5FtdRUkAPNDaB7i2oBa0BBzUTRCIbg",
    "WORKSHEET_INDEX": 0,
    "MAX_WORKERS": 4,
    "SOURCES": "both",  # 'both', 'website', or 'docs'
    "DOCS_DIRECTORY": "docs",
    "WEBSITE_URL": "",
    "VERIFY_WRITES": True,
    "PERSIST_UPLOADS": True,
    "UPLOAD_CACHE_FILE": "upload_cache.json",
    "LOG_LEVEL": "INFO"
}

class Config:
    def __init__(self):
        self.config = DEFAULT_CONFIG.copy()
        self.loaded_paths = []
        self.args = None
        self._load_config()
        self._parse_args()
        self._apply_overrides()
    
    def _get_app_dir(self) -> Path:
        """Get the application directory, handling both development and bundled environments."""
        # Check if we're running in a PyInstaller/executable bundle
        if getattr(sys, 'frozen', False):
            # For --onefile mode
            if hasattr(sys, '_MEIPASS'):
                # Running in PyInstaller bundle
                return Path(sys._MEIPASS)
            # For --standalone mode
            return Path(sys.executable).parent
        # Running in normal Python
        return Path(__file__).parent

    def _load_config(self):
        """Load configuration from config.json if it exists."""
        app_dir = self._get_app_dir()
        
        # Define possible config file locations in order of priority
        config_paths = [
            Path("config.json"),  # Current working directory
            app_dir / "config.json",  # Same directory as executable
            Path.home() / ".config" / "security-questionnaire" / "config.json",
            Path("/etc/security-questionnaire/config.json"),
            app_dir / "config.json.sample"  # Fallback to sample config
        ]
        
        # If --config was specified, check it first
        if self.args and getattr(self.args, 'config', None):
            custom_config = Path(self.args.config)
            if custom_config.exists():
                config_paths.insert(0, custom_config)
        
        for path in config_paths:
            try:
                if path.exists():
                    with open(path, 'r') as f:
                        config_data = json.load(f)
                        self.config.update(config_data)
                        self.loaded_paths.append(str(path))
                        # Stop after first valid config is found
                        if path.name != "config.json.sample":
                            break
            except Exception as e:
                print(f"Warning: Could not load config from {path}: {e}", file=sys.stderr)
        
        # If no config was loaded, show a helpful message
        if not self.loaded_paths:
            print("No configuration file found. Using default settings.", file=sys.stderr)
    
    def _parse_args(self):
        """Parse command line arguments."""
        parser = argparse.ArgumentParser(
            description="Security Questionnaire Responder - Automate security questionnaire responses using AI."
        )
        
        # General options
        parser.add_argument(
            "--config", 
            type=str,
            help="Path to config file"
        )
        
        # API and authentication
        parser.add_argument(
            "--gemini-api-key",
            type=str,
            help="Google Gemini API key (or set GEMINI_API_KEY environment variable)"
        )
        
        # Google Sheets options
        parser.add_argument(
            "--spreadsheet-id",
            type=str,
            help="Google Sheets spreadsheet ID"
        )
        parser.add_argument(
            "--worksheet-index",
            type=int,
            help="Worksheet index (0-based)"
        )
        
        # Source configuration
        parser.add_argument(
            "--sources",
            choices=["both", "website", "docs"],
            help="Which sources to use: 'both', 'website', or 'docs'"
        )
        parser.add_argument(
            "--docs-directory",
            type=str,
            help="Directory containing documentation files"
        )
        parser.add_argument(
            "--website-url",
            type=str,
            help="Website URL to crawl for information"
        )
        
        # Performance options
        parser.add_argument(
            "--max-workers",
            type=int,
            help="Maximum number of worker threads"
        )
        parser.add_argument(
            "--no-verify-writes",
            action="store_false",
            dest="verify_writes",
            help="Disable write verification"
        )
        
        # Feature flags
        parser.add_argument(
            "--no-persist-uploads",
            action="store_false",
            dest="persist_uploads",
            help="Disable persistent upload cache"
        )
        
        # Logging
        parser.add_argument(
            "--log-level",
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            help="Set the logging level"
        )
        
        self.args = parser.parse_args()
    
    def _apply_overrides(self):
        """Apply overrides from environment variables and command line."""
        # Apply environment variables
        for key in self.config:
            env_value = os.getenv(key)
            if env_value is not None:
                self.config[key] = type(self.config[key])(env_value)
        
        # Apply command line arguments
        if self.args:
            for key, value in vars(self.args).items():
                if value is not None and key.upper() in self.config:
                    self.config[key.upper()] = value
    
    def __getattr__(self, name: str) -> Any:
        """Allow attribute-style access to config values."""
        if name in self.config:
            return self.config[name]
        raise AttributeError(f"No such configuration: {name}")
    
    def __getitem__(self, key: str) -> Any:
        """Allow dict-style access to config values."""
        return self.config.get(key)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a config value with a default if not present."""
        return self.config.get(key, default)
    
    def to_dict(self) -> Dict[str, Any]:
        """Return the configuration as a dictionary."""
        return self.config.copy()
    
    def print_config(self):
        """Print the current configuration."""
        print("Current configuration:")
        for key, value in self.config.items():
            if "KEY" in key and value:
                value = "*" * 8 + value[-4:] if len(value) > 8 else "*" * 8
            print(f"  {key}: {value}")
        
        if self.loaded_paths:
            print("\nConfiguration loaded from:", ", ".join(self.loaded_paths))
        else:
            print("\nUsing default configuration (no config files found)")

# Global config instance
config = Config()

# For backward compatibility
GEMINI_API_KEY = config.GEMINI_API_KEY
SPREADSHEET_ID = config.SPREADSHEET_ID
WORKSHEET_INDEX = config.WORKSHEET_INDEX
MAX_WORKERS = config.MAX_WORKERS
SOURCES = config.SOURCES
DOCS_DIRECTORY = config.DOCS_DIRECTORY
WEBSITE_URL = config.WEBSITE_URL
VERIFY_WRITES = config.VERIFY_WRITES
PERSIST_UPLOADS = config.PERSIST_UPLOADS
UPLOAD_CACHE_FILE = config.UPLOAD_CACHE_FILE
LOG_LEVEL = config.LOG_LEVEL
