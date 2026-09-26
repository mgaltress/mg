"""Minimal client for the BEAM portal Secure API (SuiteDash)."""
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE_URL = "https://portal.beam805.com/secure-api"
DEFAULT_CREDS = Path(r"C:\Users\mgalt\Desktop\MG Docs\beam_api_key.txt")
DUMMY_CREDS = {"public_id": "00000000-0000-0000-0000-000000000000", "secret_key": "DummySecretKey"}


def load_creds(path=DEFAULT_CREDS):
    """Read Public ID and Secret Key from a text file.

    Accepts 'Label: value', 'Label:' followed by the value on the next line,
    or 'NAME=value'. Labels just need to contain 'public' or 'secret'.
    """
    path = Path(path)
    if not path.exists():
        sys.exit(f"Credentials file not found: {path}\n"
                 "Create it with your Public ID and Secret Key from Integrations > Secure API.")
    creds, pending = {}, None
    for line in (l.strip() for l in path.read_text(encoding="utf-8-sig").splitlines()):
        if not line:
            continue
        m = re.match(r"^([^:=]*?)\s*[:=]\s*(.*)$", line)
        label = (m.group(1) if m else "").lower()
        key = "public_id" if "public" in label else "secret_key" if "secret" in label else None
        if key:
            if m.group(2):
                creds[key] = m.group(2)
            else:
                pending = key
        elif pending:
            creds[pending], pending = line, None
    missing = {"public_id", "secret_key"} - creds.keys()
    if missing:
        sys.exit(f"Could not find {', '.join(sorted(missing))} in {path}. "
                 "Expected lines like 'Public ID: ...' and 'Secret Key: ...'.")
    return creds


class Client:
    def __init__(self, creds):
        self.creds = creds
        self.calls = 0

    def get(self, path, params=None):
        """Return (http status, parsed body). Never raises on HTTP errors."""
        url = BASE_URL + path + ("?" + urllib.parse.urlencode(params) if params else "")
        req = urllib.request.Request(url, headers={
            "X-Public-ID": self.creds["public_id"],
            "X-Secret-Key": self.creds["secret_key"],
            "Accept": "application/json",
        })
        self.calls += 1
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            try:
                return e.code, json.loads(body)
            except ValueError:
                return e.code, {"success": False, "message": body[:300]}

    def get_all(self, path):
        """Fetch every page of a list endpoint (10 records per page)."""
        records, page = [], 1
        while True:
            status, body = self.get(path, {"page": page, "orderBy": "created"})
            if status != 200:
                sys.exit(f"GET {path} page {page} failed (HTTP {status}): {body.get('message')}")
            records += body.get("data") or []
            pagination = (body.get("meta") or {}).get("pagination") or {}
            if page >= (pagination.get("totalPages") or 1):
                return records
            page += 1
