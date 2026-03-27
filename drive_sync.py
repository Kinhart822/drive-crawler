import os
import sys
import re
import threading
import concurrent.futures
import time
import random
import pandas as pd  # type: ignore
from typing import Optional, Any
from dotenv import load_dotenv  # type: ignore

# Google API Libraries
from google.auth.transport.requests import Request  # type: ignore
from google.oauth2.credentials import Credentials  # type: ignore
from google_auth_oauthlib.flow import InstalledAppFlow  # type: ignore
from googleapiclient.discovery import build  # type: ignore
from googleapiclient.errors import HttpError  # type: ignore

# Rich Console
from rich.console import Console  # type: ignore
from rich.theme import Theme  # type: ignore

# Global Console Setup
custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
    "id": "blue",
    "wait": "bright_black"
})
console = Console(theme=custom_theme)

# Load environment variables
load_dotenv()

def prompt_input(msg: str) -> str:
    """Helper for console input."""
    return input(msg).strip()

class DriveDirectCopier:
    """Handles direct Google Drive server-to-server transfers."""
    
    def __init__(self, max_workers: int = 5):
        self._validate_env()
        
        self.CLIENT_SECRET_FILE = str(os.getenv("GOOGLE_CLIENT_SECRET_FILE"))
        self.TOKEN_FILE = str(os.getenv("GOOGLE_TOKEN_FILE"))
        self.SCOPES = str(os.getenv("GOOGLE_SCOPES")).split(",")
        self.ERROR_LOG_FILE = str(os.getenv("ERROR_LOG_FILE"))
        self.max_workers = max_workers
        self.failed_rows = []
        self._lock = threading.Lock() # Ensures thread-safe logging and error list modification

        # Initialize or clean error log on startup
        self._init_error_log()
        
        # Initialize Google Drive API Service
        self.service = self._get_api_service()

    def _validate_env(self):
        """Validates required .env variables."""
        required = ["GOOGLE_CLIENT_SECRET_FILE", "GOOGLE_TOKEN_FILE", "GOOGLE_SCOPES", "EXCEL_DATA_FILE", "ERROR_LOG_FILE"]
        missing = [v for v in required if not os.getenv(v)]
        if missing:
            console.print(f"[error]❌ Critical Error: Missing .env variables: {', '.join(missing)}[/error]")
            sys.exit(1)

    def _init_error_log(self):
        """Initializes error log file."""
        try:
            with open(self.ERROR_LOG_FILE, "w", encoding="utf-8") as f:
                f.write(f"# Error Log Started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        except Exception as e:
            console.print(f"[warning]⚠️ Warning: Could not initialize log file: {e}[/warning]")

    def _log_error(self, message: str):
        """Logs error with timestamp."""
        try:
            with open(self.ERROR_LOG_FILE, "a", encoding="utf-8") as f:
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{timestamp}] {message}\n")
        except Exception as e:
            console.print(f"[error]❌ Logging error: {e}[/error]")

    def _get_api_service(self):
        """Authenticates and returns Drive service."""
        creds = None
        # Use existing token if available
        if os.path.exists(self.TOKEN_FILE):
            try:
                creds = Credentials.from_authorized_user_file(self.TOKEN_FILE, self.SCOPES)
            except Exception as e:
                console.print(f"[warning]⚠️ Token file corrupted or invalid: {e}. Re-authenticating...[/warning]")
        
        # If no valid credentials, request new login
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception:
                    # Refresh failed, run auth flow
                    flow = InstalledAppFlow.from_client_secrets_file(self.CLIENT_SECRET_FILE, self.SCOPES)
                    creds = flow.run_local_server(port=0)
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.CLIENT_SECRET_FILE, self.SCOPES)
                creds = flow.run_local_server(port=0)
            
            # Save token for future use
            with open(self.TOKEN_FILE, "w") as token:
                token.write(creds.to_json())
                
        return build("drive", "v3", credentials=creds)

    def _extract_id(self, text: Optional[str]) -> Optional[str]:
        """Extracts Drive ID from text/URL."""
        if not text: return None
        patterns = [
            r"/folders/([a-zA-Z0-9_-]+)",
            r"/file/d/([a-zA-Z0-9_-]+)",
            r"id=([a-zA-Z0-9_-]+)",
            r"/u/\d+/folders/([a-zA-Z0-9_-]+)",
            r"/u/\d+/file/d/([a-zA-Z0-9_-]+)",
        ]
        for p in patterns:
            m = re.search(p, text)
            if m: return m.group(1)
        
        clean = text.strip()
        return clean if re.match(r"^[a-zA-Z0-9_-]+$", clean) else None

    def _execute_api(self, request, max_retries=5):
        """Executes API request with retry logic."""
        if not request:
            return None
            
        for attempt in range(max_retries):
            try:
                return request.execute()
            except HttpError as e:
                # Retry on rate limit (403/429) errors
                if e.resp.status in [403, 429]:
                    if attempt == max_retries - 1:
                        raise e
                    wait = (2**attempt) + random.random()
                    console.print(f"    [wait]⏳ Rate limit hit. Retrying in {wait:.2f}s...[/wait]")
                    time.sleep(wait)
                else:
                    raise e
            except Exception as e:
                # Handle other transient issues with a short delay
                if attempt == max_retries - 1:
                    raise e
                time.sleep(1)
        
        return None

    def _item_exists(self, service, name: str, parent_id: str):
        """Checks if item exists at destination."""
        try:
            safe_name = name.replace("'", "\\'")
            q = f"name = '{safe_name}' and '{parent_id}' in parents and trashed = false"
            res = self._execute_api(service.files().list(
                q=q, fields="files(id, mimeType)", supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=1
            ))
            items = res.get("files", []) if res else []
            if not items: return False, None
            
            item = items[0]
            return True, item.get("id")
        except Exception:
            return False, None

    def copy_file(self, service, file_id: str, dest_parent_id: str, name: str = ""):
        """Copies a single file."""
        try:
            body: dict[str, Any] = {"parents": [dest_parent_id]}
            if name: body["name"] = name
            
            request = service.files().copy(fileId=file_id, body=body, supportsAllDrives=True)
            new_file = self._execute_api(request)
            if new_file:
                console.print(f"  [success]✅ [OK] File Copied: '{new_file.get('name', 'Unknown')}'[/success]")
                return True
        except Exception as e:
            console.print(f"  [error]❌ Copy Error {file_id}: {e}[/error]")
        return False

    def get_or_create_folder(self, service, name: str, parent_id: str) -> Optional[str]:
        """Gets existing folder ID or creates new one."""
        exists, existing_id = self._item_exists(service, name, parent_id)
        if exists: return existing_id

        try:
            meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
            folder = self._execute_api(service.files().create(body=meta, fields="id", supportsAllDrives=True))
            return folder.get("id") if folder else None
        except Exception as e:
            console.print(f"  [error]❌ Folder Creation Error {name}: {e}[/error]")
            return None

    def copy_recursive(self, service, src_id: str, dest_parent_id: str, name: str = ""):
        """Recursively syncs folder contents."""
        try:
            if not name:
                meta = self._execute_api(service.files().get(fileId=src_id, fields="name", supportsAllDrives=True))
                if not meta:
                    console.print(f"  [error]❌ Could not retrieve metadata for {src_id}[/error]")
                    return False
                name = meta.get("name", "Unknown")

            new_folder_id = self.get_or_create_folder(service, name, dest_parent_id)
            if not new_folder_id: return False

            console.print(f"📂 [info]Scanning:[/info] '{name}'...")

            token = None
            while True:
                q = f"'{src_id}' in parents and trashed = false"
                res = self._execute_api(service.files().list(
                    q=q, fields="nextPageToken, files(id, name, mimeType)",
                    pageToken=token, supportsAllDrives=True, includeItemsFromAllDrives=True
                ))
                
                if not res:
                    break
                
                for item in res.get("files", []):
                    exists, existing_id = self._item_exists(service, item["name"], new_folder_id)
                    
                    if item["mimeType"] == "application/vnd.google-apps.folder":
                        self.copy_recursive(service, item["id"], new_folder_id, item["name"])
                    elif not exists:
                        self.copy_file(service, item["id"], new_folder_id, item["name"])

                token = res.get("nextPageToken")
                if not token: break
            return True
        except Exception as e:
             console.print(f"  [error]❌ Recursive sync error: {e}[/error]")
             return False

    def verify_folder_access(self, folder_id: str):
        """Verifies access to destination folder."""
        try:
            console.print(f"🔍 [info]Verifying Access ID:[/info] [blue]{folder_id}[/blue]...")
            res = self._execute_api(self.service.files().get(fileId=folder_id, fields="id, name, capabilities(canAddChildren)", supportsAllDrives=True))
            
            if not res:
                console.print(f"❌ [error]Access Denied or Link Missing: Could not retrieve folder metadata.[/error]")
                return False
                
            console.print(f"✅ [success]Located Folder:[/success] '[bold white]{res.get('name')}[/bold white]'")
            if not res.get('capabilities', {}).get('canAddChildren'):
                console.print(f"⚠️ [warning]WARNING: You lack 'Editor' permissions on this folder![/warning]")
            return True
        except Exception as e:
            console.print(f"❌ [error]Access Denied or Link Missing: {e}[/error]")
            return False

    def process_row(self, index, row, link_col, id_col, name_col, target_folder_id: str, total):
        """Processes one Excel row."""
        url = str(row[link_col]).strip()
        item_id = self._extract_id(url)
        name_base = str(row.get(name_col, "Unknown"))
        row_id = str(row.get(id_col, index))
        target_name = f"{name_base} ({row_id})"

        if not item_id:
            self._log_error(f"Invalid link at row {index+1}: {url}")
            return False

        try:
            console.print(f"[id][{index+1}/{total}][/id] 🔄 Processing: [bold white]{target_name}[/bold white]...")
            meta = self._execute_api(self.service.files().get(fileId=item_id, fields="id, mimeType", supportsAllDrives=True))
            
            if not meta:
                self._log_error(f"Metadata fetch failed for ID: {item_id} ({url})")
                return False

            success = False
            if meta.get("mimeType") == "application/vnd.google-apps.folder":
                success = self.copy_recursive(self.service, item_id, target_folder_id, target_name)
            else:
                item_exists, _ = self._item_exists(self.service, target_name, target_folder_id)
                success = True if item_exists else self.copy_file(self.service, item_id, target_folder_id, target_name)

            if not success:
                with self._lock: self.failed_rows.append(index)
                self._log_error(f"Sync Failure: {target_name} ({url})")
            return success
        except Exception as e:
            console.print(f"[error]❌ Thread Error on row {index+1}: {e}[/error]")
            return False

    def run_excel_sync(self, excel_file, target_folder_id: str):
        """Orchestrates bulk sync from Excel."""
        if not os.path.exists(excel_file): return
        df = pd.read_excel(excel_file)
        df.columns = [str(c).lower().strip() for c in df.columns]
        
        link_col = next((c for c in df.columns if "url" in c or df[c].astype(str).str.contains("http").any()), None)
        if not link_col: return console.print("[error]❌ No URL column found in Excel file[/error]")
        
        # Verify access to destination before starting sequence
        if not self.verify_folder_access(target_folder_id):
            return

        console.print("\n[info]Optional: Enter IDs to copy (e.g. 1-10, 15) or leave blank for ALL:[/info]")
        ids_input = input("Selection: ").strip()
        if ids_input:
             pass

        total = len(df)
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            args = [(i, r, link_col, "id", "name", target_folder_id, total) for i, r in df.iterrows()]
            list(executor.map(lambda p: self.process_row(*p), args))
        
        console.print("\n🎊 [success]EXCEL SYNC COMPLETE![/success]")

    def copy_single_manual(self, src_raw: str, dest_raw: str):
        """Manual single sync."""
        src_id = self._extract_id(src_raw)
        dest_id = self._extract_id(dest_raw)
        
        # Validate IDs are correctly extracted
        if not src_id or not dest_id:
            console.print("[error]❌ Invalid Source or Destination ID![/error]")
            return

        # Explicitly verify destination access before starting copy
        assert src_id is not None and dest_id is not None
        if self.verify_folder_access(dest_id):
            console.print(f"\n🚀 [bold cyan]MANUAL COPY STARTING...[/bold cyan]")
            if self.copy_recursive(self.service, src_id, dest_id):
                console.print(f"\n🎊 [success]SUCCESS: Entire folder hierarchy copied.[/success]")
            else:
                console.print(f"\n❌ [error]FAILURE: Some items could not be synced.[/error]")

if __name__ == "__main__":
    while True:
        console.print("\n==============================================")
        console.print("   🤖 GOOGLE DRIVE SYNC      ")
        console.print("==============================================")

        console.print("\nSELECT MODE:")
        console.print("1. Bulk Sync from Excel")
        console.print("2. Manual Copy (Single Folder/File Link)")
        console.print("Q. Quit Program")
        
        mode = prompt_input("\nMode (1/2/Q): ").lower()

        if mode in ['q', 'quit']:
            console.print("\n[yellow]👋 Exiting program...[/yellow]")
            break

        if mode not in ["1", "2"]:
            console.print("[error]❌ Invalid mode. Please try again.[/error]")
            continue

        try:
            workers_count = prompt_input("Threads (default 5, Q to go back): ").lower()
            if workers_count in ['q', 'quit']: continue
            workers = int(workers_count) if workers_count else 5
        except ValueError: 
            workers = 5

        copier = DriveDirectCopier(max_workers=workers)

        if mode == "1":
            path = os.getenv("EXCEL_DATA_FILE")
            folder_raw = prompt_input("➡️ Destination Folder (ID/Link or Q to go back): ")
            if folder_raw.lower() in ['q', 'quit']: continue
            
            folder_id = copier._extract_id(folder_raw)
            if folder_id: 
                copier.run_excel_sync(path, folder_id)
            else:
                console.print("[error]❌ Destination ID missing or invalid![/error]")
                
        elif mode == "2":
            source_raw = prompt_input("➡️ Source Link/ID (Q to go back): ")
            if source_raw.lower() in ['q', 'quit']: continue
            
            dest_raw = prompt_input("➡️ Destination Link/ID (Q to go back): ")
            if dest_raw.lower() in ['q', 'quit']: continue
            
            copier.copy_single_manual(source_raw, dest_raw)
