#!/usr/bin/env python3
import sys
import re
import os
import subprocess
import argparse
from typing import List, Tuple

# Patterns to detect
PATTERNS = {
    "OpenAI Key": re.compile(r"sk-[a-zA-Z0-9]{20,}"),
    "Telegram Bot Token": re.compile(r"\d{8,}:[A-Za-z0-9_-]{35,}"),
    "HuggingFace Token": re.compile(r"hf_[a-zA-Z0-9]{20,}"),
    "Generic API Key": re.compile(r"(API_KEY|TOKEN|SECRET)\s*[:=]\s*['\"]?[a-zA-Z0-9_\-]{16,}['\"]?", re.IGNORECASE),
    "Private Key": re.compile(r"-----BEGIN [A-Z ]+ PRIVATE KEY-----"),
}

# Whitelist of files/patterns to ignore
IGNORE_FILES = {
    "scan_secrets.py", 
    ".env.example",
    "package-lock.json",
    "pnpm-lock.yaml"
}

def get_staged_files() -> List[str]:
    """Return list of files staged for commit."""
    try:
        output = subprocess.check_output(["git", "diff", "--name-only", "--cached"], text=True)
        return [f for f in output.splitlines() if f]
    except subprocess.CalledProcessError:
        return []

def get_tracked_files() -> List[str]:
    """Return list of all files tracked by git."""
    try:
        output = subprocess.check_output(["git", "ls-files"], text=True)
        return [f for f in output.splitlines() if f]
    except subprocess.CalledProcessError:
        return []

def scan_file(filepath: str) -> List[Tuple[int, str, str]]:
    """Scan a single file for secrets. Returns list of (line_num, pattern_name, match_snippet)."""
    if os.path.basename(filepath) in IGNORE_FILES:
        return []
    
    if not os.path.exists(filepath):
        return []
        
    findings = []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f, 1):
                # Skip detection if line has specific ignore comment
                if "# nosec" in line:
                    continue
                    
                for name, pattern in PATTERNS.items():
                    match = pattern.search(line)
                    if match:
                        # Mask the secret for display
                        found = match.group(0)
                        masked = found[:4] + "*" * (len(found) - 8) + found[-4:] if len(found) > 8 else "****"
                        findings.append((i, name, masked))
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        
    return findings

def main():
    parser = argparse.ArgumentParser(description="Scan for leaked secrets.")
    parser.add_argument("--staged-only", action="store_true", default=True, help="Scan only staged files (default)")
    parser.add_argument("--all-tracked", action="store_true", help="Scan all git-tracked files")
    parser.add_argument("--path", type=str, help="Scan a specific file or directory")
    
    args = parser.parse_args()
    
    files_to_scan = []
    
    if args.path:
        if os.path.isfile(args.path):
            files_to_scan = [args.path]
        elif os.path.isdir(args.path):
            for root, _, files in os.walk(args.path):
                for file in files:
                    files_to_scan.append(os.path.join(root, file))
    elif args.all_tracked:
        files_to_scan = get_tracked_files()
    else:
        files_to_scan = get_staged_files()
        
    findings_count = 0
    
    print(f"🔍 Scanning {len(files_to_scan)} files for secrets...")
    
    for filepath in files_to_scan:
        failures = scan_file(filepath)
        for line_num, name, masked in failures:
            print(f"❌ [SECRET DETECTED] {filepath}:{line_num}")
            print(f"   Rule: {name}")
            print(f"   Match: {masked}")
            findings_count += 1
            
    if findings_count > 0:
        print(f"\n🚫 BLOCKING: Found {findings_count} potential secrets.")
        print("   Move secrets to .env (which is gitignored).")
        print("   If this is a false positive, add '# nosec' to the line.")
        sys.exit(1)
    else:
        print("✅ No secrets found.")
        sys.exit(0)

if __name__ == "__main__":
    main()
