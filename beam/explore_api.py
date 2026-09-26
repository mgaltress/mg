"""
Explore the BEAM portal Secure API (SuiteDash) to see what data is available.

Makes a small, capped number of calls (the account has a monthly call limit),
saves every raw response to beam/raw/<timestamp>/ (git-ignored), and prints a
field inventory: field names, types, fill rates and custom-field definitions.
Record values are never printed, since they contain client PII.

Usage:
    python explore_api.py                 # real credentials
    python explore_api.py --dummy         # docs' dummy credentials, fake data, no quota used
    python explore_api.py --creds PATH    # credentials file somewhere else
"""
import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

from beam_api import DEFAULT_CREDS, DUMMY_CREDS, Client, load_creds

RAW_DIR = Path(__file__).parent / "raw"

# (name, path, query params) - one call each, list endpoints fetch page 1 only
CALLS = [
    ("contact_meta", "/contact/meta", {}),
    ("company_meta", "/company/meta", {}),
    ("project_meta", "/project/meta", {}),
    ("contacts_p1", "/contacts", {"orderBy": "created"}),
    ("companies_p1", "/companies", {"orderBy": "created"}),
    ("projects_p1", "/projects", {"orderBy": "created"}),
    ("project_latest", "/project/most_recent/true", {}),
    ("worlds_p1", "/worlds", {}),
]


def type_name(v):
    if v is None or v == "" or v == []:
        return "empty"
    return type(v).__name__


def describe_records(records, custom_field_names):
    """Print each field's type mix and fill rate across the sample, without values."""
    n = len(records)
    fields = {}
    for rec in records:
        for k, v in rec.items():
            if isinstance(v, dict) and k.endswith("custom_fields"):
                for cf_id, cf_v in v.items():
                    fields.setdefault(f"{k}.{custom_field_names.get(cf_id, cf_id)}", []).append(cf_v)
            else:
                fields.setdefault(k, []).append(v)
    for k, vals in fields.items():
        types = Counter(type_name(v) for v in vals)
        filled = n - types.pop("empty", 0)
        print(f"    {k:60} filled {filled:>3}/{n:<3} {dict(types)}")


def describe_meta(meta):
    """Print field definitions from a /meta response; return {custom field id: name}."""
    names = {}
    attrs = meta.get("data") if isinstance(meta.get("data"), dict) else meta
    for attr, spec in (attrs or {}).items():
        if not isinstance(spec, dict):
            continue
        if attr.endswith("custom_fields") and isinstance(spec.get("properties", spec), dict):
            for cf_id, cf in spec.get("properties", spec).items():
                if isinstance(cf, dict):
                    name = cf.get("field_name") or cf_id
                    names[cf_id] = name
                    allowed = cf.get("allowed_values")
                    print(f"    {attr}.{name:45} {cf.get('type', '?'):7} "
                          f"{('options=' + json.dumps(allowed)[:150]) if allowed else ''}")
        else:
            print(f"    {attr:60} {spec.get('type', '?'):7} {spec.get('field_name') or ''}")
    return names


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    ap.add_argument("--dummy", action="store_true", help="use the docs' dummy credentials (fake data)")
    ap.add_argument("--max-calls", type=int, default=len(CALLS), help="hard cap on API calls")
    args = ap.parse_args()

    client = Client(DUMMY_CREDS if args.dummy else load_creds(args.creds))
    out_dir = RAW_DIR / (datetime.now().strftime("%Y%m%d_%H%M%S") + ("_dummy" if args.dummy else ""))
    out_dir.mkdir(parents=True, exist_ok=True)

    custom_field_names = {}
    for i, (name, path, params) in enumerate(CALLS[:args.max_calls], 1):
        status, body = client.get(path, params)
        (out_dir / f"{name}.json").write_text(json.dumps(body, indent=2), encoding="utf-8")
        print(f"\n[{i}] GET {path} -> HTTP {status}")
        if status == 429:
            print("    Monthly API limit reached - stopping.")
            break
        if status != 200 or not body.get("success", True):
            print(f"    {body.get('message')}")
            continue

        pag = (body.get("meta") or {}).get("pagination")
        if pag:
            print(f"    total records: {pag.get('totalItems')}  pages: {pag.get('totalPages')}  "
                  f"page size: {pag.get('pageSize')}")
        if name.endswith("_meta"):
            custom_field_names.update(describe_meta(body))
        data = body.get("data")
        if isinstance(data, dict) and not name.endswith("_meta"):
            data = [data]
        if isinstance(data, list) and data:
            describe_records(data, custom_field_names)

    print(f"\nRaw responses saved to {out_dir}")


if __name__ == "__main__":
    main()
