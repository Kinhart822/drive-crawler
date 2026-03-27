import os
import sys
import re
import threading
import pandas as pd  # type: ignore
import concurrent.futures
import time
import random
from google.auth.transport.requests import Request  # type: ignore
from google.oauth2.credentials import Credentials  # type: ignore
from google_auth_oauthlib.flow import InstalledAppFlow  # type: ignore
from googleapiclient.discovery import build  # type: ignore
from googleapiclient.errors import HttpError  # type: ignore
from typing import Optional, Any
from dotenv import load_dotenv  # type: ignore

# Load environment variables
load_dotenv()

class DriveDirectCopier:
    """Main class to handle direct server-to-server Google Drive copying."""
    
    def __init__(self, max_workers: int = 5):
        # Comprehensive environment check
        required_vars = [
            "GOOGLE_CLIENT_SECRET_FILE",
            "GOOGLE_TOKEN_FILE",
            "GOOGLE_SCOPES",
            "EXCEL_DATA_FILE",
            "ERROR_LOG_FILE",
        ]
        missing = [v for v in required_vars if not os.getenv(v)]
        if missing:
            print(f"Error: Missing required environment variables in .env: {', '.join(missing)}")
            sys.exit(1)

        self.CLIENT_SECRET_FILE = str(os.getenv("GOOGLE_CLIENT_SECRET_FILE"))
        self.TOKEN_FILE = str(os.getenv("GOOGLE_TOKEN_FILE"))
        self.SCOPES = str(os.getenv("GOOGLE_SCOPES")).split(",")
        self.ERROR_LOG_FILE = str(os.getenv("ERROR_LOG_FILE"))
        self.max_workers = max_workers
        self.failed_rows = []
        self._lock = threading.Lock() # Lock to ensure safe list appending between threads

        # Clear/Reset the log file at each run
        try:
            with open(self.ERROR_LOG_FILE, "w", encoding="utf-8") as f:
                f.write(f"# Error Log Started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        except Exception as e:
            print(f"Warning: Could not clear log file: {e}")

        # Create an initial service instance to ensure token is valid before starting workers
        self._get_api_service()

    def _log_error(self, message: str):
        """Logs an error message to the log file with a timestamp."""
        try:
            with open(self.ERROR_LOG_FILE, "a", encoding="utf-8") as f:
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{timestamp}] {message}\n")
        except Exception as e:
            print(f"  [CRITICAL ERROR] Could not write to log file: {e}")

    def _get_api_service(self):
        """Authenticates and creates a Google Drive API service object."""
        creds = None
        if os.path.exists(self.TOKEN_FILE):
            creds = Credentials.from_authorized_user_file(self.TOKEN_FILE, self.SCOPES)
        
        # If no token or expired, request new login
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.CLIENT_SECRET_FILE, self.SCOPES
                )
                creds = flow.run_local_server(port=0)
            with open(self.TOKEN_FILE, "w") as token:
                token.write(creds.to_json())
        return build("drive", "v3", credentials=creds)

    def _extract_id(self, text):
        """Extracts G-Drive File/Folder ID from various URL patterns."""
        patterns = [
            r"/folders/([a-zA-Z0-9_-]+)",
            r"/file/d/([a-zA-Z0-9_-]+)",
            r"id=([a-zA-Z0-9_-]+)",
            r"/u/\d+/folders/([a-zA-Z0-9_-]+)", # Added for /u/X/ patterns
            r"/u/\d+/file/d/([a-zA-Z0-9_-]+)",   # Added for /u/X/ patterns
        ]
        for p in patterns:
            m = re.search(p, text)
            if m:
                return m.group(1)
        
        # If it's just a raw ID, strip and return
        clean_text = text.strip()
        if re.match(r"^[a-zA-Z0-9_-]+$", clean_text):
            return clean_text
        return None

    def _execute_with_retry(self, request, max_retries=5):
        """Executes API request with exponential backoff on 403/429 errors."""
        for attempt in range(max_retries):
            try:
                return request.execute()
            except HttpError as e:
                # If rate-limited, wait and retry
                if e.resp.status in [403, 429]:
                    if attempt == max_retries - 1:
                        raise e
                    sleep_time = (2**attempt) + random.random()
                    print(f"    [WAIT] Rate limit hit. Retrying in {sleep_time:.2f}s (Attempt {attempt+1}/{max_retries})...")
                    time.sleep(sleep_time)
                else:
                    raise e
        return None

    def _check_if_exists_and_empty(self, service, name, parent_id):
        """Checks if item exists at destination. Returns (exists, ID, is_empty)."""
        try:
            safe_name = name.replace("'", "\\'")
            q = f"name = '{safe_name}' and '{parent_id}' in parents and trashed = false"
            res = self._execute_with_retry(
                service.files().list(
                    q=q, fields="files(id, mimeType)",
                    supportsAllDrives=True, includeItemsFromAllDrives=True,
                )
            )
            files = res.get("files", [])

            if not files:
                return False, None, False

            existing_id = files[0]["id"]
            if files[0]["mimeType"] == "application/vnd.google-apps.folder":
                # Check if the existing folder is empty
                q_empty = f"'{existing_id}' in parents and trashed = false"
                res_empty = self._execute_with_retry(
                    service.files().list(
                        q=q_empty, fields="files(id)", pageSize=1,
                        supportsAllDrives=True, includeItemsFromAllDrives=True,
                    )
                )
                is_empty = len(res_empty.get("files", [])) == 0
                return True, existing_id, is_empty
            else:
                return True, existing_id, False
        except Exception as e:
            print(f"    [ERROR] Check exists failed for {name}: {e}")
            return False, None, False

    def copy_file(self, service: Any, file_id: str, dest_parent_id: str, new_name: str = "") -> bool:
        """Copies a single file with built-in 2nd-attempt retry logic."""
        for attempt in range(2): 
            try:
                body: dict[str, Any] = {"parents": [dest_parent_id]}
                if new_name:
                    body["name"] = new_name
                
                request = service.files().copy(fileId=file_id, body=body, supportsAllDrives=True)
                new_file = self._execute_with_retry(request)
                if new_file:
                    print(f"  [OK] Copied/Synced: {new_file.get('name')}")
                    return True
                
                if attempt == 0:
                    print(f"  [RETRY] Copy failed for {new_name or file_id}. Retrying once...")
            except Exception as e:
                if attempt == 1:
                    print(f"  [ERROR] Final failure copying file {file_id}: {e}")
                else:
                    print(f"  [RETRY] Error during copy {file_id}: {e}. Retrying...")
        return False

    def get_or_create_folder(self, service: Any, name: str, parent_id: str) -> Optional[str]:
        """Gets existing folder ID by name in parent or creates a new one."""
        exists, existing_id, _ = self._check_if_exists_and_empty(service, name, parent_id)
        if exists and existing_id:
            return existing_id

        file_metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        try:
            request = service.files().create(body=file_metadata, fields="id", supportsAllDrives=True)
            folder = self._execute_with_retry(request)
            return folder.get("id") if folder else None
        except Exception as e:
            print(f"  [ERROR] Failed to create folder {name}: {e}")
            return None

    def copy_recursive(self, service: Any, src_folder_id: str, dest_parent_id: str, custom_folder_name: str = ""):
        """Deep sync of a folder's content (including subfolders and children)."""
        try:
            if not custom_folder_name:
                request = service.files().get(fileId=src_folder_id, fields="name", supportsAllDrives=True)
                folder_meta = self._execute_with_retry(request)
                if not folder_meta:
                    return False
                custom_folder_name = folder_meta.get("name", "Unknown")

            # Get or create current level folder
            new_folder_id = self.get_or_create_folder(service, custom_folder_name, dest_parent_id)
            if not new_folder_id:
                return False

            print(f"Checking Folder Path: {custom_folder_name}...")

            page_token = None
            while True:
                # List items in current source folder level
                request = service.files().list(
                    q=f"'{src_folder_id}' in parents and trashed = false",
                    spaces="drive",
                    fields="nextPageToken, files(id, name, mimeType)",
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                response = self._execute_with_retry(request)
                if not response:
                    break

                for file in response.get("files", []):
                    # Only copy if not present at destination (Incremental Deep Check)
                    exists, existing_id, is_empty = self._check_if_exists_and_empty(service, file["name"], new_folder_id)

                    if file["mimeType"] == "application/vnd.google-apps.folder":
                        if exists and is_empty:
                            self._log_error(f"Incomplete folder detected (EMPTY): {custom_folder_name}/{file['name']}")
                        self.copy_recursive(service, file["id"], new_folder_id, file["name"])
                    else:
                        if exists:
                            continue
                        self.copy_file(service, file["id"], new_folder_id, file["name"])

                page_token = response.get("nextPageToken", None)
                if page_token is None:
                    break
            return True
        except Exception as e:
            print(f"  [ERROR] Recursive copy failed for {src_folder_id}: {e}")
            return False

    def process_row(self, index: int, row: Any, link_col: str, id_col: Optional[str], name_col: Optional[str], target_folder_id: str, total: int) -> bool:
        """Processes a single row from the Excel data file."""
        url = str(row[link_col]).strip()
        item_id = self._extract_id(url)
        row_id = str(row.get(id_col, index))
        name_base = str(row.get(name_col, "Unknown"))
        target_name = f"{name_base} ({row_id})"

        if not item_id:
            msg = f"Skipping invalid URL at row {index+1}: {url}"
            print(f"[{index+1}/{total}] {msg}")
            self._log_error(msg)
            return False

        local_service = self._get_api_service()
        try:
            # Check if root item already exists
            exists, existing_id, is_empty = self._check_if_exists_and_empty(local_service, target_name, target_folder_id)
            print(f"[{index+1}/{total}] Processing: {target_name}...")

            request = local_service.files().get(fileId=item_id, fields="id, name, mimeType", supportsAllDrives=True)
            meta = self._execute_with_retry(request)
            if not meta:
                msg = f"Failed to fetch metadata for {target_name} (ID: {item_id})"
                self._log_error(msg)
                return False

            result = False
            if meta["mimeType"] == "application/vnd.google-apps.folder":
                if exists and is_empty:
                     self._log_error(f"Incomplete (EMPTY) root folder: {target_name}")
                
                # First attempt: Deep Sync
                result = self.copy_recursive(local_service, item_id, target_folder_id, target_name)
                
                # Auto-Healing: If failed, perform a second deep check pass
                if not result:
                    print(f"[{index+1}/{total}] [HEAL] Sync incomplete for {target_name}. Attempting to heal/retry...")
                    result = self.copy_recursive(local_service, item_id, target_folder_id, target_name)
            else:
                if exists:
                    print(f"[{index+1}/{total}] File {target_name} already exists. Skipping.")
                    return True
                
                # Copy single file with retry loop
                result = self.copy_file(local_service, item_id, target_folder_id, target_name)
                if not result:
                    result = self.copy_file(local_service, item_id, target_folder_id, target_name)

            if not result:
                with self._lock:
                    self.failed_rows.append(index)
                self._log_error(f"FINAL FAILURE: Failed to fully sync/copy: {target_name} (URL: {url})")

            return result
        except Exception as e:
            msg = f"Critical error for {target_name}: {e}"
            print(f"[{index+1}/{total}] {msg}")
            if "notFound" in str(e) or "404" in str(e):
                with self._lock:
                    self.failed_rows.append(index)
            self._log_error(msg)
            return False

    def verify_folder_access(self, folder_id: str):
        """Checks if the given folder ID exists and is accessible."""
        service = self._get_api_service()
        try:
            print(f"Verifying access to ID: {folder_id}...")
            res = service.files().get(
                fileId=folder_id, 
                fields="id, name, capabilities(canAddChildren)", 
                supportsAllDrives=True
            ).execute()
            print(f"✅ Found: '{res.get('name')}'")
            if not res.get('capabilities', {}).get('canAddChildren'):
                print(f"⚠️ Warning: You may not have 'Editor' permissions on this folder.")
            return True
        except Exception as e:
            print(f"❌ Error accessing folder: {e}")
            return False

    def copy_single_folder_manual(self, src_folder_id: str, dest_parent_id: str):
        """Helper to quickly copy a single folder manually without Excel."""
        service = self._get_api_service()
        print(f"\n--- 🚀 STARTING MANUAL FOLDER COPY ---")
        success = self.copy_recursive(service, src_folder_id, dest_parent_id)
        if success:
            print(f"\n✅ FOLDER COPY COMPLETED!")
        else:
            print(f"\n❌ FOLDER COPY FAILED. Check logs or ID.")

    def run(self, excel_file, target_folder_id):
        """Main runner that reads Excel data and dispatches worker threads."""
        if not os.path.exists(excel_file):
            print(f"Error: {excel_file} not found.")
            return

        try:
            df = pd.read_excel(excel_file)
            df.columns = [str(c).lower().strip() for c in df.columns]
        except Exception as e:
            print(f"Error reading Excel: {e}")
            return

        # Map required columns automatically
        link_col = "url" if "url" in df.columns else None
        if not link_col:
            for col in df.columns:
                if df[col].astype(str).str.contains("http", na=False).any():
                    link_col = col
                    break
        name_col = "name" if "name" in df.columns else None
        id_col = "id" if "id" in df.columns else None

        if not link_col:
            print("Error: Could not find 'url' column in Excel.")
            return

        final_link_col, final_id_col, final_name_col = str(link_col), (str(id_col) if id_col else None), (str(name_col) if name_col else None)
        df = df.dropna(subset=[link_col])
        total_available = len(df)
        print(f"Loaded {total_available} items from {excel_file}.")

        # Interactive ID selection for partial runs
        print("\nSpecify IDs from column 'ID' to copy (comma-separated or range e.g. 1-10, 15, 20-30)")
        print("Leave empty to copy ALL items.")
        user_ids_input = input("Enter IDs to copy: ").strip()
        
        if user_ids_input:
            selected_ids = set()
            try:
                parts = [p.strip() for p in user_ids_input.split(',')]
                for part in parts:
                    if '-' in part:
                        s, e = part.split('-')
                        selected_ids.update(range(int(s), int(e) + 1))
                    else:
                        selected_ids.add(int(part))
                if final_id_col and final_id_col in df.columns:
                    df[final_id_col] = pd.to_numeric(df[final_id_col], errors='coerce')
                    df = df[df[final_id_col].isin(selected_ids)]
                else:
                    print("Warning: No 'ID' column found. Filtering by row position.")
                    df = df.iloc[list(selected_ids)]
            except Exception as e:
                print(f"Error parsing IDs: {e}. Syncing ALL items.")

        total = len(df)
        if total == 0:
            print("No items selected. Exiting.")
            return

        print(f"Syncing {total} items using {self.max_workers} workers.")

        # Dispatch worker pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = [
                executor.submit(self.process_row, i, row, final_link_col, final_id_col, final_name_col, target_folder_id, total)  # type: ignore
                for i, row in df.iterrows()
            ]
            for future in concurrent.futures.as_completed(tasks):
                try:
                    future.result()
                except Exception as e:
                    print(f"Worker task error detail: {e}")

        print("\nSync completed!")

        # Final interactive cleanup of sources
        if self.failed_rows:
            print(f"\n[CLEANUP] Found {len(self.failed_rows)} items with dead links (404).")
            if input("Do you want to DELETE these rows from your Excel file? (y/n): ").strip().lower() == "y":
                try:
                    df_full = pd.read_excel(excel_file)
                    df_cleaned = df_full.drop(self.failed_rows)
                    df_cleaned.to_excel(excel_file, index=False)
                    print(f"  [OK] Successfully cleaned up '{excel_file}'.")
                except Exception as e:
                    print(f"  [ERROR] Cleanup failed: {e}")

if __name__ == "__main__":
    print("==============================================")
    print("   🤖 GOOGLE DRIVE MULTI-WORKER SYNC TOOL     ")
    print("==============================================")

    # Load environment variables for Excel path
    excel_file = os.getenv("EXCEL_DATA_FILE")
    
    print("\nCHOOSE MODE:")
    print("1. Sync from Excel file (Multi-workers)")
    print("2. Copy 1 Single Folder (Manual Copy)")
    
    choice = input("\nEnter your choice (1/2): ").strip()

    if choice == "1":
        # MODE 1: EXCEL SYNC
        if not excel_file:
            print("Error: EXCEL_DATA_FILE not defined in .env")
            sys.exit(1)

        target_id = input("➡️ Enter DESTINATION Folder ID: ").strip()
        while not target_id:
            target_id = input("Error: Destination ID is required: ").strip()

        try:
            workers_input = input("➡️ Enter number of workers (default 5): ").strip()
            workers = int(workers_input) if workers_input else 5
        except ValueError:
            workers = 5

        copier = DriveDirectCopier(max_workers=workers)
        copier.run(excel_file, target_id)

    elif choice == "2":
        # MODE 2: MANUAL SINGLE FOLDER COPY
        src_input = input("➡️ Enter SOURCE Folder Link or ID: ").strip()
        dest_input = input("➡️ Enter DESTINATION Folder Link or ID: ").strip()

        try:
            workers_input = input("➡️ Enter number of workers (default 5): ").strip()
            workers = int(workers_input) if workers_input else 5
        except ValueError:
            workers = 5
        
        copier = DriveDirectCopier(max_workers=workers)
        src_id = copier._extract_id(src_input)
        dest_id = copier._extract_id(dest_input)

        if not src_id or not dest_id:
            print("Error: Could not extract valid IDs from your input.")
            sys.exit(1)

        # Pre-verification
        print("\nChecking permissions...")
        if not copier.verify_folder_access(str(dest_id)):
            print("Aborting because destination is not reachable.")
            sys.exit(1)

        # Start manual copy
        copier.copy_single_folder_manual(str(src_id), str(dest_id))

    else:
        print("Invalid choice. Exiting.")
