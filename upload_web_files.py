#!/usr/bin/env python3

import os
import sys
import json
import http.client
from pathlib import Path

ESP32_IP = "192.168.12.14"   # Change to your ESP32's IP
ESP32_PORT = 8080
DATA_DIR = "data"


def escape_json_string(s):
    """Escape string for JSON"""
    return (
        s.replace('\\', '\\\\')
         .replace('"', '\\"')
         .replace('\n', '\\n')
         .replace('\r', '\\r')
         .replace('\t', '\\t')
    )


def upload_file(ip, port, filepath, web_path):
    """Upload a file to ESP32 via HTTP POST (no requests module)"""

    # -- Read file content
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # -- Escape content for JSON
    content_escaped = escape_json_string(content)

    # -- Build JSON payload
    payload = {
        "path": web_path,
        "content": content_escaped
    }

    body = json.dumps(payload).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Content-Length": str(len(body))
    }

    try:
        conn = http.client.HTTPConnection(ip, port, timeout=10)
        conn.request("POST", "/upload", body=body, headers=headers)

        response = conn.getresponse()
        response_body = response.read().decode("utf-8", errors="replace")

        if response.status == 200:
            try:
                result = json.loads(response_body)
                print(f"✓ {web_path} ({result.get('bytes', 0)} bytes)")
            except json.JSONDecodeError:
                print(f"✓ {web_path} (uploaded)")
            return True
        else:
            print(f"✗ {web_path} - HTTP {response.status}: {response_body}")
            return False

    except Exception as e:
        print(f"✗ {web_path} - Error: {e}")
        return False

    finally:
        try:
            conn.close()
        except Exception:
            pass


def main():
    esp_ip = sys.argv[1] if len(sys.argv) > 1 else ESP32_IP

    print("=" * 60)
    print(f"Uploading web files to ESP32 at {esp_ip}:{ESP32_PORT}")
    print("=" * 60)

    if not os.path.exists(DATA_DIR):
        print(f"Error: {DATA_DIR}/ directory not found!")
        sys.exit(1)

    # -- Find all files in data/ directory
    files = []
    for root, dirs, filenames in os.walk(DATA_DIR):
        for filename in filenames:
            if not filename.startswith('.'):
                files.append(os.path.join(root, filename))

    if not files:
        print(f"No files found in {DATA_DIR}/ directory")
        sys.exit(1)

    print(f"\nFound {len(files)} file(s) to upload:\n")

    success_count = 0
    fail_count = 0

    for filepath in files:
        rel_path = os.path.relpath(filepath, DATA_DIR)
        web_path = f"/web/{rel_path}"

        if upload_file(esp_ip, ESP32_PORT, filepath, web_path):
            success_count += 1
        else:
            fail_count += 1

    print("\n" + "=" * 60)
    print(f"Upload complete: {success_count} succeeded, {fail_count} failed")
    print("=" * 60)

    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()