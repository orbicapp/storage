#!/usr/bin/env python3
"""
Multipart file upload client for Hono R2 worker.
Usage: python upload.py <file_path> <resource_name>
"""

import os
import sys
import json
import time
import mimetypes
from typing import List, Dict, Any, Optional
import requests
import jwt
from datetime import datetime, timedelta

# Configuration
CHUNK_SIZE = 5 * 1024 * 1024  # 5MB - must match server
CONFIG_FILE = "upload_config.json"

class UploadError(Exception):
    """Custom exception for upload errors"""
    pass

class MultipartUploader:
    def __init__(self, config_path: str = CONFIG_FILE):
        """Initialize the uploader with configuration"""
        self.config = self._load_config(config_path)
        self.base_url = self.config["worker_url"].rstrip("/")
        self.jwt_secret = self.config["jwt_secret"]
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise UploadError(f"Configuration file {config_path} not found")
        except json.JSONDecodeError:
            raise UploadError(f"Invalid JSON in configuration file {config_path}")
    
    def _generate_backend_jwt(self, action: str) -> str:
        """Generate JWT token for backend operations"""
        payload = {
            "type": "backend",
            "action": action,
            "iat": int(time.time()),
            "exp": int(time.time()) + 3600  # 1 hour expiration
        }
        return jwt.encode(payload, self.jwt_secret, algorithm="HS256")
    
    def _get_file_info(self, file_path: str) -> tuple[int, str]:
        """Get file size and MIME type"""
        if not os.path.exists(file_path):
            raise UploadError(f"File not found: {file_path}")
        
        file_size = os.path.getsize(file_path)
        mime_type, _ = mimetypes.guess_type(file_path)
        
        if mime_type is None:
            mime_type = "application/octet-stream"
        
        return file_size, mime_type
    
    def _make_request(self, method: str, endpoint: str, token: str, **kwargs) -> requests.Response:
        """Make HTTP request with proper error handling"""
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {token}",
            **kwargs.get("headers", {})
        }
        kwargs["headers"] = headers
        
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            raise UploadError(f"Request failed: {e}")
    
    def create_upload(self, file_id: str, file_size: int, mime_type: str) -> Dict[str, Any]:
        """Create multipart upload on the server"""
        print(f"Creating upload for file ID: {file_id}")
        
        backend_token = self._generate_backend_jwt("create")
        
        payload = {
            "id": file_id,
            "fileSize": file_size,
            "mimeType": mime_type
        }
        
        response = self._make_request("POST", "/upload/create", backend_token, json=payload)
        data = response.json()
        
        if not data.get("success"):
            raise UploadError(f"Failed to create upload: {data.get('error', 'Unknown error')}")
        
        print(f"Upload created successfully. Upload ID: {data['uploadId']}")
        print(f"Total parts: {data['totalParts']}")
        
        return data
    
    def upload_file_parts(self, file_path: str, client_token: str, upload_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Upload file in chunks using the client token"""
        total_parts = upload_info["totalParts"]
        parts = []
        
        print(f"Starting upload of {total_parts} parts...")
        
        with open(file_path, 'rb') as f:
            for part_number in range(1, total_parts + 1):
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                
                is_last_part = part_number == total_parts
                print(f"Uploading part {part_number}/{total_parts} ({len(chunk)} bytes)")
                
                # Upload part
                endpoint = f"/upload/part/{part_number}"
                if is_last_part:
                    endpoint += "?isLast=true"
                
                response = self._make_request(
                    "PUT", 
                    endpoint, 
                    client_token,
                    data=chunk,
                    headers={"Content-Type": "application/octet-stream"}
                )
                
                part_data = response.json()
                if not part_data.get("success"):
                    raise UploadError(f"Failed to upload part {part_number}: {part_data.get('error')}")
                
                parts.append({
                    "partNumber": part_data["partNumber"],
                    "etag": part_data["etag"]
                })
                
                # Show progress
                progress = (part_data["uploadedBytes"] / part_data["totalBytes"]) * 100
                print(f"Progress: {progress:.1f}% ({part_data['uploadedBytes']}/{part_data['totalBytes']} bytes)")
        
        print("All parts uploaded successfully!")
        return parts
    
    def complete_upload(self, upload_id: str, file_id: str, parts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Complete the multipart upload"""
        print("Completing upload...")
        
        backend_token = self._generate_backend_jwt("complete")
        
        payload = {
            "uploadId": upload_id,
            "parts": parts
        }
        
        response = self._make_request(
            "POST", 
            f"/upload/complete?fileId={file_id}", 
            backend_token, 
            json=payload
        )
        
        data = response.json()
        if not data.get("success"):
            raise UploadError(f"Failed to complete upload: {data.get('error')}")
        
        print("Upload completed successfully!")
        return data
    
    def abort_upload(self, upload_id: str, file_id: str):
        """Abort the multipart upload"""
        print("Aborting upload...")
        
        backend_token = self._generate_backend_jwt("abort")
        
        try:
            self._make_request(
                "DELETE", 
                f"/upload/abort/{upload_id}?fileId={file_id}", 
                backend_token
            )
            print("Upload aborted successfully")
        except Exception as e:
            print(f"Warning: Failed to abort upload: {e}")
    
    def upload_file(self, file_path: str, resource_name: str) -> str:
        """Complete upload workflow"""
        start_time = time.time()
        
        try:
            # Get file information
            file_size, mime_type = self._get_file_info(file_path)
            print(f"File: {file_path}")
            print(f"Size: {file_size} bytes ({file_size / (1024*1024):.2f} MB)")
            print(f"MIME type: {mime_type}")
            
            # Create upload
            upload_info = self.create_upload(resource_name, file_size, mime_type)
            client_token = upload_info["clientToken"]
            upload_id = upload_info["uploadId"]
            
            try:
                # Upload parts
                parts = self.upload_file_parts(file_path, client_token, upload_info)
                
                # Complete upload
                completion_info = self.complete_upload(upload_id, resource_name, parts)
                
                # Generate file URL
                file_url = f"{self.base_url}/file/{resource_name}"
                
                elapsed_time = time.time() - start_time
                print(f"\n✅ Upload successful!")
                print(f"Resource name: {resource_name}")
                print(f"File URL: {file_url}")
                print(f"ETag: {completion_info['etag']}")
                print(f"Size: {completion_info['size']} bytes")
                print(f"Upload time: {elapsed_time:.2f} seconds")
                print(f"Average speed: {(file_size / elapsed_time) / (1024*1024):.2f} MB/s")
                
                return file_url
                
            except Exception as e:
                # Abort upload on error
                self.abort_upload(upload_id, resource_name)
                raise
                
        except UploadError:
            raise
        except Exception as e:
            raise UploadError(f"Unexpected error: {e}")

def create_sample_config():
    """Create a sample configuration file"""
    config = {
        "worker_url": "http://localhost:8787",
        "jwt_secret": "your-jwt-secret-here"
    }
    
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"Sample configuration created: {CONFIG_FILE}")
    print("Please edit the file with your actual worker URL and JWT secret.")

def main():
    """Main function"""
    if len(sys.argv) != 3:
        print("Usage: python upload.py <file_path> <resource_name>")
        print("Example: python upload.py ./image.jpg my-image")
        sys.exit(1)
    
    file_path = sys.argv[1]
    resource_name = sys.argv[2]
    
    # Check if config file exists
    if not os.path.exists(CONFIG_FILE):
        print(f"Configuration file {CONFIG_FILE} not found.")
        create_sample_config()
        sys.exit(1)
    
    try:
        uploader = MultipartUploader()
        file_url = uploader.upload_file(file_path, resource_name)
        print(f"\n🔗 Your file is available at: {file_url}")
        
    except UploadError as e:
        print(f"❌ Upload failed: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️  Upload cancelled by user")
        sys.exit(1)

if __name__ == "__main__":
    main()