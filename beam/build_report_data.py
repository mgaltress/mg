"""
Build the BEAM impact report dataset (beam/data/report.json).

Sources:
  1. Session Report form exports (CSV) in <private>/exports/     - session history
  2. Secure API: companies, contacts, projects                   - client profiles, requests, engagements
  3. Secure API capture: each company's "latest meeting" fields  - new sessions since the last export.
     The API has no session log, but the Session Report form copies each submission onto the
     company record, so every run saves any meeting it hasn't seen to <private>/captured_sessions.json.
     Run this often (every few hours) so meetings aren't overwritten between runs.
  4. beam/manual_inputs.json                                     - workshops and monthly notes

<private> defaults to beam/private/ (git-ignored); the scheduled GitHub Action in the
mg-private repo points it at that repo instead. The published JSON is de-identified:
no client names, emails, notes or company names, and companies are replaced by sequential ids.

Usage:
    python build_report_data.py
    python build_report_data.py --creds PATH
    python build_report_data.py --private-dir DIR --out FILE    (used by the scheduled job)
"""
import argparse
import csv
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

from beam_api import DEFAULT_CREDS, Client, load_creds

HERE = Path(__file__).parent
DEFAULT_PRIVATE = HERE / "private"
MANUAL_INPUTS = HERE / "manual_inputs.json"
DEFAULT_OUT = HERE / "data" / "report.json"

SCHEMA_VERSION = 1
# Mentor names are volunteers' real names; keep False to publish "Mentor 1", "Mentor 2", ...
PUBLISH_MENTOR_NAMES = False

# Spelling variants -> canonical name (compared case-insensitively)
PERSON_ALIASES = {
    "mike woods": "Michael Woods",
}
NO_PERSON = {"", "n/a", "na", "none", "no", "-"}

MEETING_TYPES = ["In Person", "Video Conference", "Phone", "Email"]
TOPICS = ["Accounting & Finance", "Business Startup", "Marketing", "Operations", "Technology"]
OWNERSHIP = ["Woman-owned", "Minority-owned", "Veteran-owned", "None of the above", "Prefer not to say"]

# CSV column names from the "BEAM Client Session Report" form export
COL_ID = "ID"
COL_SUBMITTED = "Created"
COL_EMAIL = "Current Email"
COL_MENTOR = "Mentor"
COL_CO_MENTOR = "Co Mentor"
COL_CO_TIME = "Co-Mentor Time"
COL_MEETING_DATE = "The date a Mentor meets with a Client"
COL_HOURS = "How many hours did you meet? (ie 1.5) hours"
COL_TYPE = "How did you meet?"


# ---------------------------------------------------------------- helpers

def parse_date(s):
    s = (s or "").strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def parse_hours(v):
    try:
        h = float(str(v).strip())
        return h if h >= 0 else None
    except (TypeError, ValueError):
        return None


def person(name):
    """Normalise a mentor name: trim, drop ' SME' suffix, fix case, apply aliases."""
    n = re.sub(r"\s+", " ", (name or "").strip())
    n = re.sub(r"\s+SME$", "", n, flags=re.I)
    if n.lower() in NO_PERSON:
        return None
    return PERSON_ALIASES.get(n.lower(), " ".join(w[:1].upper() + w[1:].lower() for w in n.split()))


def people(names):
    """Split a co-mentor field that may list several people."""
    if (names or "").strip().lower() in NO_PERSON:
        return []
    parts = re.split(r",|&|\band\b|/", names)
    return [p for p in (person(x) for x in parts) if p and len(p) > 2]


def as_list(v):
    """Multi-select fields come back as lists, or occasionally as JSON text."""
    if isinstance(v, list):
        return v
    if isinstance(v, str) and v.strip().startswith("["):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, list) else []
        except ValueError:
            return []
    return [v] if isinstance(v, str) and v.strip() else []


def norm_email(e):
    return (e or "").strip().lower()


# ---------------------------------------------------------------- API

def fetch_companies(client):
    """All companies, with custom fields keyed by their readable names under 'cf'."""
    status, meta = client.get("/company/meta")
    if status != 200:
        raise SystemExit(f"GET /company/meta failed (HTTP {status}): {meta.get('message')}")
    meta = meta.get("data", meta)
    fields = meta.get("target_custom_fields", {})
    fields = fields.get("properties", fields)
    field_names = {k: v.get("field_name") for k, v in fields.items() if isinstance(v, dict)}

    companies = client.get_all("/companies")
    for c in companies:
        c["cf"] = {field_names.get(k, k): v for k, v in (c.get("target_custom_fields") or {}).items()}
    return companies


def fetch_projects(client, cache_path):
    """Project list plus each project's client email.

    The list endpoint omits the client, so it takes one detail call per project. A project's
    client doesn't change, so emails are cached and only new projects cost a call.
    """
    cache = json.loads(cache_path.read_text(encoding="utf-8")) if cache_path.exists() else {}
    projects = client.get_all("/projects")
    for p in projects:
        if p["uid"] not in cache:
            status, body = client.get(f"/project/uid/{p['uid']}")
            if status != 200:
                continue
            cache[p["uid"]] = ((body.get("data") or {}).get("client") or {}).get("email")
        p["client"] = {"email": cache[p["uid"]]}
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, indent=1), encoding="utf-8")
    return projects


def email_to_company(companies, contacts):
    """Map any known client email to a company uid."""
    known = {c["uid"] for c in companies}
    lookup = {}
    for c in companies:
        e = norm_email((c.get("primaryContact") or {}).get("email"))
        if e:
            lookup[e] = c["uid"]
    for ct in contacts:
        for link in ct.get("companies") or []:
            if link.get("uid") in known:
                for e in (ct.get("email"), ct.get("work_email"), ct.get("home_email")):
                    if norm_email(e):
                        lookup.setdefault(norm_email(e), link["uid"])
                break
    return lookup


# ---------------------------------------------------------------- sessions

def sessions_from_exports(email_lookup, exports_dir):
    sessions, seen_ids, files = [], set(), sorted(exports_dir.glob("*.csv"))
    for f in files:
        with f.open(encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                if row.get(COL_ID) in seen_ids:
                    continue
                seen_ids.add(row.get(COL_ID))
                submitted = parse_date(row.get(COL_SUBMITTED))
                meeting = parse_date(row.get(COL_MEETING_DATE))
                sessions.append({
                    "date": meeting or submitted,
                    "date_estimated": meeting is None,
                    "company_uid": email_lookup.get(norm_email(row.get(COL_EMAIL))),
                    "mentor": person(row.get(COL_MENTOR)),
                    "co_mentors": people(row.get(COL_CO_MENTOR)),
                    "mentor_hours": parse_hours(row.get(COL_HOURS)),
                    "co_mentor_hours": parse_hours(row.get(COL_CO_TIME)),
                    "type": (row.get(COL_TYPE) or "").strip() or None,
                    "topics": [t for t in TOPICS if (row.get(f"{t} Discussed") or "").strip() == "1"],
                    "source": "export",
                })
    return [s for s in sessions if s["date"]], [f.name for f in files]


def capture_latest_meetings(companies, captured_path):
    """Record each company's current 'latest meeting' fields; return all captured sessions."""
    captured = json.loads(captured_path.read_text(encoding="utf-8")) if captured_path.exists() else []
    keys = {(s["company_uid"], s["date"]) for s in captured}
    today = date.today().isoformat()
    for c in companies:
        f = c["cf"]
        meeting = parse_date(f.get("Client Meeting Date"))
        if not meeting or (c["uid"], meeting.isoformat()) in keys:
            continue
        captured.append({
            "date": meeting.isoformat(),
            "captured_on": today,
            "company_uid": c["uid"],
            "mentor": person(f.get("Mentor")),
            "co_mentors": people(f.get("Co Mentor Name")),
            "mentor_hours": parse_hours(f.get("Length of Meeting")),
            "co_mentor_hours": parse_hours(f.get("Co-Mentor Time")),
            "type": f.get("Meeting Mentoring Type") or None,
            "topics": [t for t in TOPICS if f.get(f"{t} Discussed") == "checked"],
        })
        keys.add((c["uid"], meeting.isoformat()))
    captured_path.parent.mkdir(parents=True, exist_ok=True)
    captured_path.write_text(json.dumps(captured, indent=1), encoding="utf-8")
    return captured


def merge_sessions(exported, captured):
    """Exports are authoritative; captured meetings only fill in sessions the exports don't have."""
    exported_keys = {(s["company_uid"], s["date"].isoformat()) for s in exported}
    merged = list(exported)
    for s in captured:
        if (s["company_uid"], s["date"]) not in exported_keys:
            merged.append({**s, "date": parse_date(s["date"]), "date_estimated": False, "source": "api"})
    return sorted(merged, key=lambda s: s["date"])


# ---------------------------------------------------------------- output

def build(companies, contacts, projects, sessions, export_files):
    lookup = email_to_company(companies, contacts)

    # Sequential company ids, oldest first
    companies = sorted(companies, key=lambda c: c.get("created") or "")
    company_id = {c["uid"]: i + 1 for i, c in enumerate(companies)}

    # Mentor labels, most active first
    counts = {}
    for s in sessions:
        for p in [s["mentor"], *s["co_mentors"]]:
            if p:
                counts[p] = counts.get(p, 0) + 1
    ordered = sorted(counts, key=lambda p: (-counts[p], p))
    label = {p: (p if PUBLISH_MENTOR_NAMES else f"Mentor {i + 1}") for i, p in enumerate(ordered)}

    out_companies = []
    for c in companies:
        f = c["cf"]
        industry = as_list(f.get("Primary Industry")) or [((c.get("category") or {}).get("name"))]
        size = f.get("Number of Employees")
        out_companies.append({
            "id": company_id[c["uid"]],
            "created": (c.get("created") or "")[:7],
            "industry": industry[0] if industry and industry[0] else None,
            "ownership": [o for o in as_list(f.get("Ownership & Equity Context")) if o in OWNERSHIP],
            "employees": size if isinstance(size, str) and size.strip() else None,
            "referral": as_list(f.get("How did you hear about us?")),
            "areas_requested": [a for a in as_list(f.get("Areas of Mentorship Requested")) if a in TOPICS],
        })

    out_sessions = []
    for s in sessions:
        co_hours = s["co_mentor_hours"] or 0
        out_sessions.append({
            "date": s["date"].isoformat(),
            "date_estimated": s["date_estimated"],
            "company": company_id.get(s["company_uid"]),
            "mentor": label.get(s["mentor"]),
            "co_mentors": [label[p] for p in s["co_mentors"]],
            "co_mentoring": bool(s["co_mentors"]) or co_hours > 0,
            "mentor_hours": s["mentor_hours"],
            "co_mentor_hours": s["co_mentor_hours"],
            "type": s["type"] if s["type"] in MEETING_TYPES else None,
            "topics": s["topics"],
            "source": s["source"],
        })

    out_engagements = []
    for p in projects:
        uid = lookup.get(norm_email((p.get("client") or {}).get("email")))
        out_engagements.append({
            "created": (p.get("created") or "")[:10],
            "status": p.get("status"),
            "company": company_id.get(uid),
        })

    manual = json.loads(MANUAL_INPUTS.read_text(encoding="utf-8")) if MANUAL_INPUTS.exists() else {}
    session_dates = [s["date"] for s in out_sessions]
    return {
        "meta": {
            "schema_version": SCHEMA_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "data_from": min(session_dates, default=None),
            "data_through": max(session_dates, default=None),
            "sources": {
                "session_exports": export_files,
                "sessions_from_exports": sum(s["source"] == "export" for s in out_sessions),
                "sessions_from_api_capture": sum(s["source"] == "api" for s in out_sessions),
                "sessions_unlinked_to_company": sum(s["company"] is None for s in out_sessions),
                "companies": len(out_companies),
                "engagements": len(out_engagements),
            },
            "definitions": {
                "session": "One 'BEAM Client Session Report' submission, dated by meeting date "
                           "(submission date when the meeting date is missing: date_estimated=true).",
                "hours": "Total volunteer hours = mentor_hours + co_mentor_hours.",
                "co_mentoring": "A co-mentor is named or co-mentor time is logged.",
                "active_mentors": "Distinct lead mentors with a session in the period.",
                "volunteers": "Distinct lead mentors and co-mentors with a session in the period.",
                "new_request": "A company created in the portal (by month).",
                "engagement": "A portal project opened for a client.",
            },
        },
        "lookups": {
            "meeting_types": MEETING_TYPES,
            "topics": TOPICS,
            "ownership": OWNERSHIP,
        },
        "companies": out_companies,
        "sessions": out_sessions,
        "engagements": out_engagements,
        "workshops": manual.get("workshops", []),
        "notes": manual.get("notes", {}),
    }


def write_if_changed(report, out):
    """Write the report unless only generated_at would change. Returns True if written."""
    if out.exists():
        try:
            old = json.loads(out.read_text(encoding="utf-8"))
            old["meta"]["generated_at"] = report["meta"]["generated_at"]
            if old == report:
                return False
        except (ValueError, KeyError):
            pass
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=1), encoding="utf-8")
    return True


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    ap.add_argument("--private-dir", type=Path, default=DEFAULT_PRIVATE,
                    help="folder holding exports/, the captured session log and caches")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    exports_dir = args.private_dir / "exports"

    client = Client(load_creds(args.creds))
    companies = fetch_companies(client)
    contacts = client.get_all("/contacts")
    projects = fetch_projects(client, args.private_dir / "project_clients.json")

    lookup = email_to_company(companies, contacts)
    exported, export_files = sessions_from_exports(lookup, exports_dir)
    captured = capture_latest_meetings(companies, args.private_dir / "captured_sessions.json")
    sessions = merge_sessions(exported, captured)

    report = build(companies, contacts, projects, sessions, export_files)
    written = write_if_changed(report, args.out)

    src = report["meta"]["sources"]
    print(f"API calls used:          {client.calls}")
    print(f"Session exports read:    {', '.join(export_files) or f'none (put CSVs in {exports_dir})'}")
    print(f"Sessions:                {len(sessions)} "
          f"({src['sessions_from_exports']} from exports, {src['sessions_from_api_capture']} from API capture)")
    print(f"Sessions not linked:     {src['sessions_unlinked_to_company']}")
    print(f"Companies / engagements: {src['companies']} / {src['engagements']}")
    print(f"Data range:              {report['meta']['data_from']} to {report['meta']['data_through']}")
    print(f"{'Wrote' if written else 'No data changes, left'} {args.out}")


if __name__ == "__main__":
    main()
