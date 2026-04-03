import requests
import base64
import json
import os
import re
import time
from datetime import datetime
from difflib import SequenceMatcher
import gspread

# --- Configuration ---
PROPERTY_GROUP_ID = os.environ.get('PROPERTY_GROUP_ID') or '14'
WORKSHEET_NAME = os.environ.get('WORKSHEET_NAME') or 'Availability Report'

VACANCY_STATUSES = {
    "vacant-unrented": "Vacant-Unrented",
    "vacant-rented": "Vacant-Rented",
    "notice-unrented": "Notice-Unrented",
}

SECTION_ORDER = ["Vacant-Unrented", "Vacant-Rented", "Notice-Unrented"]

# Column H (NEED TO POST) dropdown options and colors
NEED_TO_POST_OPTIONS = ["Turn In Progress", "Need To Post", "Posted"]
NEED_TO_POST_COLORS = {
    "Turn In Progress": {
        "bg": {"red": 1.0,   "green": 0.898, "blue": 0.2},
        "fg": {"red": 0.0,   "green": 0.0,   "blue": 0.0},
    },
    "Need To Post": {
        "bg": {"red": 0.918, "green": 0.259, "blue": 0.208},
        "fg": {"red": 1.0,   "green": 1.0,   "blue": 1.0},
    },
    "Posted": {
        "bg": {"red": 0.204, "green": 0.659, "blue": 0.325},
        "fg": {"red": 1.0,   "green": 1.0,   "blue": 1.0},
    },
}


# ==========================================================================
# Helpers (patterns from WAPR main.py)
# ==========================================================================

def get_env_var(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Environment variable '{name}' not set.")
    return value


def build_auth_headers(client_id, client_secret, developer_id):
    auth_string = f"{client_id}:{client_secret}"
    encoded = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
    return {
        "Authorization": f"Basic {encoded}",
        "X-AppFolio-Developer-ID": developer_id,
        "Content-Type": "application/json",
    }


def fetch_report(report_name, extra_params, headers, base_url):
    body = {
        "properties": {
            "property_groups_ids": [PROPERTY_GROUP_ID],
            "properties_ids": [],
            "portfolios_ids": [],
            "owners_ids": [],
        },
    }
    body.update(extra_params)

    url = f"{base_url}/{report_name}.json"
    for attempt in range(5):
        response = requests.post(url, headers=headers, json=body)
        if response.status_code == 429:
            wait = 2 ** attempt
            print(f"  Rate limited (429). Retrying in {wait}s...")
            time.sleep(wait)
            continue
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return data
        return data.get("results", [])
    response.raise_for_status()


# ==========================================================================
# Phase 1: Fetch from AppFolio
# ==========================================================================

def fetch_tickler_move_ins(headers, base_url):
    """Fetch tenant_tickler and return {unit_id: move_in_date} for Move-in events."""
    print("Fetching tenant_tickler...")
    tickler = fetch_report("tenant_tickler", {}, headers, base_url)
    print(f"  Got {len(tickler)} rows")

    move_in_map = {}
    for row in tickler:
        if row.get("event") == "Move-in":
            uid = row.get("unit_id")
            mid = row.get("move_in_date") or ""
            if uid and mid:
                move_in_map[uid] = mid

    print(f"  Found {len(move_in_map)} upcoming move-in dates")
    return move_in_map


def fetch_availability_data(headers, base_url):
    """Fetch rent_roll + unit_directory + property_directory + tenant_tickler, join on unit_id."""
    today = datetime.now().strftime('%Y-%m-%d')

    print("Fetching rent_roll...")
    rent_roll = fetch_report("rent_roll", {"as_of_date": today}, headers, base_url)
    print(f"  Got {len(rent_roll)} rows")

    time.sleep(1)

    print("Fetching unit_directory...")
    unit_dir = fetch_report("unit_directory", {"unit_visibility": "active"}, headers, base_url)
    print(f"  Got {len(unit_dir)} rows")

    time.sleep(1)

    print("Fetching property_directory...")
    prop_dir = fetch_report("property_directory", {}, headers, base_url)
    print(f"  Got {len(prop_dir)} rows")

    time.sleep(1)

    # Fetch move-in dates from tenant tickler
    tickler_move_ins = fetch_tickler_move_ins(headers, base_url)

    # Build lookup maps
    ud_map = {u["unit_id"]: u for u in unit_dir if u.get("unit_id")}
    mgmt_map = {p["property_id"]: p.get("management_start_date") for p in prop_dir if p.get("property_id")}

    # Filter rent_roll to vacancy statuses and join with unit_directory
    units = []
    for row in rent_roll:
        status_raw = (row.get("status") or "").strip()
        status_key = status_raw.lower()
        section = VACANCY_STATUSES.get(status_key)
        if not section:
            continue

        uid = row.get("unit_id")
        ud_row = ud_map.get(uid, {})

        address = ud_row.get("unit_address") or ""
        if not address:
            # Fallback: construct from rent_roll fields
            prop_parts = (row.get("property") or "").split(" - ", 1)
            prop_addr = prop_parts[1] if len(prop_parts) > 1 else prop_parts[0]
            unit_name = row.get("unit") or ""
            if unit_name and unit_name not in prop_addr:
                address = f"{prop_addr} #{unit_name}"
            else:
                address = prop_addr

        bed_bath = row.get("bd_ba") or ""
        if bed_bath == "Commercial":
            bed_bath = "Commercial"

        # For Notice-Unrented, "Last Move Out" = expected move_out date
        # For Vacant-*, "Last Move Out" = last_move_out date, with fallback to
        # management_start_date (or unit created_on) for never-tenanted properties
        if section == "Notice-Unrented":
            last_move_out = row.get("move_out") or ""
        else:
            last_move_out = row.get("last_move_out") or ""
            if not last_move_out:
                last_move_out = (
                    mgmt_map.get(row.get("property_id"))
                    or ud_row.get("created_on")
                    or ""
                )

        posted = ud_row.get("posted_to_website") or ""

        # For Vacant-Rented, pull move-in from tickler if available
        move_in = ""
        if section == "Vacant-Rented":
            move_in = tickler_move_ins.get(uid, "")

        units.append({
            "address": address.strip(),
            "bed_bath": bed_bath,
            "last_move_out": last_move_out,
            "move_in": move_in,
            "posted_to_website": posted,
            "section": section,
            "unit_id": uid,
        })

    print(f"\nFiltered to {len(units)} vacancy units:")
    for s in SECTION_ORDER:
        count = sum(1 for u in units if u["section"] == s)
        print(f"  {s}: {count}")

    return units


# ==========================================================================
# Phase 2: Read existing sheet
# ==========================================================================

def normalize_address(addr):
    """Normalize address for matching: lowercase, strip punctuation, collapse whitespace."""
    if not addr:
        return ""
    addr = addr.lower().strip()
    addr = addr.replace(",", "").replace(".", "").replace("#", " ").replace("-", " ")
    addr = re.sub(r'\s+', ' ', addr)
    return addr


def extract_street_key(addr):
    """Extract street number + name for matching, ignoring city/state/zip."""
    norm = normalize_address(addr)
    # Try to strip "city, ST XXXXX" or "city ST XXXXX" pattern at end
    # Match: word(s) FL/fl + 5-digit zip at end
    stripped = re.sub(r'\s+[a-z]+\s+fl\s+\d{5}$', '', norm)
    return stripped


def parse_existing_sheet(all_values):
    """Parse existing sheet into {normalized_address: row_dict}."""
    existing = {}
    current_section = None
    section_names = {"vacant-unrented", "vacant-rented", "notice-unrented"}

    for i, row in enumerate(all_values):
        if i == 0:
            continue

        first_cell = (row[0] if row else "").strip()
        first_lower = first_cell.lower().replace(" ", "").replace("-", "")

        # Check section headers
        is_section = False
        for sn in section_names:
            if sn.replace("-", "") == first_lower:
                current_section = VACANCY_STATUSES[sn]
                is_section = True
                break

        if is_section or not first_cell:
            continue

        if current_section is None:
            continue

        def safe_get(idx):
            return row[idx].strip() if len(row) > idx and row[idx] else ""

        key = normalize_address(first_cell)
        existing[key] = {
            "address": first_cell,
            "bed_bath": safe_get(1),
            "days_vacant": safe_get(2),
            "last_move_out": safe_get(3),
            "move_in": safe_get(4),
            "posted_to_website": safe_get(5),
            "agent": safe_get(6),
            "need_to_post": safe_get(7),
            "notes": safe_get(8),
            "section": current_section,
            "street_key": extract_street_key(first_cell),
        }

    return existing


def find_existing_match(appfolio_address, existing_data):
    """Find a matching row in existing data for an AppFolio address."""
    norm = normalize_address(appfolio_address)
    # Exact normalized match
    if norm in existing_data:
        return existing_data[norm]

    # Street key match (ignoring city/state/zip differences)
    af_street = extract_street_key(appfolio_address)
    for key, row in existing_data.items():
        if row["street_key"] and af_street and row["street_key"] == af_street:
            return row

    # Fuzzy match as last resort
    best_match = None
    best_score = 0
    for key, row in existing_data.items():
        score = SequenceMatcher(None, af_street, row["street_key"]).ratio()
        if score > best_score:
            best_score = score
            best_match = row
    if best_score >= 0.85:
        return best_match

    return None


# ==========================================================================
# Phase 3: Merge
# ==========================================================================

def merge_data(appfolio_units, existing_data):
    """Merge AppFolio data with existing sheet data, preserving manual columns."""
    sections = {s: [] for s in SECTION_ORDER}

    matched_existing_keys = set()

    for unit in appfolio_units:
        existing = find_existing_match(unit["address"], existing_data)

        if existing:
            matched_existing_keys.add(normalize_address(existing["address"]))
            agent = existing["agent"]
            need_to_post = existing["need_to_post"]
            notes = existing["notes"]
            # For Vacant-Rented: tickler move-in takes priority, then sheet value as fallback
            if unit["section"] == "Vacant-Rented":
                move_in = unit["move_in"] or existing["move_in"]
            else:
                move_in = ""
        else:
            agent = ""
            need_to_post = ""
            notes = ""
            # For Vacant-Rented: use tickler move-in if available
            move_in = unit["move_in"] if unit["section"] == "Vacant-Rented" else ""

        merged = {
            "address": unit["address"],
            "bed_bath": unit["bed_bath"],
            "last_move_out": unit["last_move_out"],
            "move_in": move_in,
            "posted_to_website": unit["posted_to_website"],
            "agent": agent,
            "need_to_post": need_to_post,
            "notes": notes,
        }
        sections[unit["section"]].append(merged)

    # Sort each section by last_move_out ascending
    def sort_key(row):
        date_str = row["last_move_out"]
        if not date_str:
            return "9999-99-99"
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return date_str

    for section_name in SECTION_ORDER:
        sections[section_name].sort(key=sort_key)

    # Report changes
    unmatched_new = sum(
        1 for u in appfolio_units
        if find_existing_match(u["address"], existing_data) is None
    )
    removed = len(existing_data) - len(matched_existing_keys)
    print(f"\nMerge summary:")
    print(f"  Matched existing: {len(matched_existing_keys)}")
    print(f"  New properties: {unmatched_new}")
    print(f"  Removed properties: {removed}")

    # Debug: if 0 matches and sheet had data, print sample addresses to diagnose
    if len(matched_existing_keys) == 0 and len(existing_data) > 0:
        print("\nDEBUG - 0 matches found. Comparing addresses:")
        print("  AppFolio addresses (first 5, normalized):")
        for u in appfolio_units[:5]:
            print(f"    raw='{u['address']}' | norm='{normalize_address(u['address'])}'")
        print("  Sheet addresses (first 5, normalized):")
        for key in list(existing_data.keys())[:5]:
            print(f"    raw='{existing_data[key]['address']}' | norm='{key}'")

    return sections


# ==========================================================================
# Phase 4 & 5: Build output and write to sheet
# ==========================================================================

def format_date_for_sheet(date_str):
    """Convert YYYY-MM-DD to MM/DD/YYYY for display."""
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%m/%d/%Y")
    except ValueError:
        return date_str


def build_output_rows(sections):
    """Build the full list of rows to write to the sheet."""
    rows = []

    # Row 1: Headers
    rows.append(["Address", "Bed/Bath", "Days Vacant", "Last Move Out",
                  "Move In", "Posted To Website", "Agent", "NEED TO POST", "Notes"])

    for i, section_name in enumerate(SECTION_ORDER):
        # Section header
        rows.append([section_name, "", "", "", "", "", "", "", ""])

        # Data rows
        for unit in sections[section_name]:
            row_num = len(rows) + 1  # 1-indexed row for formula
            days_vacant_formula = f'=IF(D{row_num}="","",INT(TODAY()-D{row_num}))'

            move_out_display = format_date_for_sheet(unit["last_move_out"])
            move_in_display = unit["move_in"]
            # If move_in looks like YYYY-MM-DD, convert it
            if move_in_display and re.match(r'^\d{4}-\d{2}-\d{2}$', move_in_display):
                move_in_display = format_date_for_sheet(move_in_display)

            rows.append([
                unit["address"],
                unit["bed_bath"],
                days_vacant_formula,
                move_out_display,
                move_in_display,
                unit["posted_to_website"],
                unit["agent"],
                unit["need_to_post"],
                unit["notes"],
            ])

        # Blank separator (except after last section)
        if i < len(SECTION_ORDER) - 1:
            rows.append(["", "", "", "", "", "", "", "", ""])

    return rows


def update_google_sheet(output_rows):
    """Connect to Google Sheets and write the output."""
    print("\n=== Connecting to Google Sheets ===")
    temp_cred_path = "/tmp/google_credentials.json"
    try:
        with open(temp_cred_path, "w") as f:
            f.write(get_env_var('GOOGLE_CREDENTIALS_JSON_CONTENT'))

        gc = gspread.service_account(filename=temp_cred_path)
        sheet_name = get_env_var('GOOGLE_SHEET_NAME')
        spreadsheet = gc.open(sheet_name)

        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            print(f"ERROR: Worksheet '{WORKSHEET_NAME}' not found in '{sheet_name}'.")
            raise

        # Read existing data for merge
        existing_values = worksheet.get_all_values()
        print(f"Read {len(existing_values)} existing rows.")

        return spreadsheet, worksheet, existing_values

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"ERROR: Spreadsheet '{sheet_name}' not found. Check name and sharing.")
        raise
    finally:
        if os.path.exists(temp_cred_path):
            os.remove(temp_cred_path)


def write_to_sheet(worksheet, output_rows):
    """Clear and rewrite the sheet with updated data."""
    # Clear a generous range (values + formatting on Days Vacant column)
    max_clear = max(len(output_rows) + 50, 100)
    worksheet.batch_clear([f"A1:I{max_clear}"])

    # Reset number formatting on Days Vacant column to plain number
    worksheet.format(f"C1:C{max_clear}", {"numberFormat": {"type": "NUMBER", "pattern": "0"}})

    # Write all rows with raw input to preserve formulas
    worksheet.update(
        output_rows,
        f"A1:I{len(output_rows)}",
        value_input_option="USER_ENTERED",
    )
    print(f"Wrote {len(output_rows)} rows to sheet.")


def apply_need_to_post_chips(worksheet, sections, output_rows):
    """Apply dropdown + conditional formatting colors to column H for Vacant-Unrented rows only."""
    H_COL = 7  # 0-indexed
    sheet_id = worksheet.id
    vu_count = len(sections["Vacant-Unrented"])

    # output_rows layout: [0]=headers, [1]=VU section header, [2..2+vu_count-1]=VU data rows
    vu_start_idx = 2         # 0-indexed sheet row where VU data begins
    vu_end_idx   = 2 + vu_count  # exclusive

    max_rows = max(len(output_rows) + 50, 150)

    # Step A: delete existing conditional format rules on this sheet to avoid accumulation
    spreadsheet_meta = worksheet.spreadsheet.client.request(
        'GET',
        f'https://sheets.googleapis.com/v4/spreadsheets/{worksheet.spreadsheet.id}',
        params={'fields': 'sheets(properties/sheetId,conditionalFormats)'}
    ).json()

    existing_cf_count = 0
    for sheet in spreadsheet_meta.get('sheets', []):
        if sheet['properties']['sheetId'] == sheet_id:
            existing_cf_count = len(sheet.get('conditionalFormats', []))
            break

    if existing_cf_count > 0:
        delete_requests = [
            {"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": idx}}
            for idx in range(existing_cf_count - 1, -1, -1)
        ]
        worksheet.spreadsheet.batch_update({"requests": delete_requests})

    # Step B: validation + new conditional format rules
    requests = []

    # 1. Clear all data validation in column H
    requests.append({
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": max_rows,
                "startColumnIndex": H_COL,
                "endColumnIndex": H_COL + 1,
            }
            # No "rule" key = clears validation
        }
    })

    # 2. Apply dropdown to Vacant-Unrented data rows only
    if vu_count > 0:
        requests.append({
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": vu_start_idx,
                    "endRowIndex": vu_end_idx,
                    "startColumnIndex": H_COL,
                    "endColumnIndex": H_COL + 1,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": v} for v in NEED_TO_POST_OPTIONS],
                    },
                    "showCustomUi": True,
                    "strict": False,
                }
            }
        })

    # 3. Days Vacant conditional formatting (column C): >60 dark red, >30 light red
    #    NEED TO POST conditional formatting (column H): yellow/red/green by value
    #
    #    All rules inserted at index=0 in lowest→highest priority order, so the
    #    last-appended rule ends up at index 0 (highest priority) after the batch.
    #    Final order: [DV>60, DV>30, NTP_TurnInProgress, NTP_NeedToPost, NTP_Posted]

    DV_COL = 2  # 0-indexed — column C "Days Vacant"
    dv_range = {
        "sheetId": sheet_id,
        "startRowIndex": 1,      # skip header row
        "endRowIndex": max_rows,
        "startColumnIndex": DV_COL,
        "endColumnIndex": DV_COL + 1,
    }
    cf_range = {
        "sheetId": sheet_id,
        "startRowIndex": vu_start_idx,
        "endRowIndex": vu_end_idx,
        "startColumnIndex": H_COL,
        "endColumnIndex": H_COL + 1,
    }

    # NEED TO POST (lowest priority — added first, pushed down by DV rules)
    for option in reversed(NEED_TO_POST_OPTIONS):
        colors = NEED_TO_POST_COLORS[option]
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [cf_range],
                    "booleanRule": {
                        "condition": {
                            "type": "TEXT_EQ",
                            "values": [{"userEnteredValue": option}],
                        },
                        "format": {
                            "backgroundColor": colors["bg"],
                            "textFormat": {"foregroundColor": colors["fg"]},
                        },
                    },
                },
                "index": 0,
            }
        })

    # Days Vacant >30: light red (added before >60 so >60 ends up at index 0)
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [dv_range],
                "booleanRule": {
                    "condition": {
                        "type": "NUMBER_GREATER",
                        "values": [{"userEnteredValue": "30"}],
                    },
                    "format": {
                        "backgroundColor": {"red": 0.957, "green": 0.8, "blue": 0.8},
                    },
                },
            },
            "index": 0,
        }
    })

    # Days Vacant >60: dark red — inserted last, lands at index 0 (highest priority)
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [dv_range],
                "booleanRule": {
                    "condition": {
                        "type": "NUMBER_GREATER",
                        "values": [{"userEnteredValue": "60"}],
                    },
                    "format": {
                        "backgroundColor": {"red": 0.8, "green": 0.2, "blue": 0.2},
                        "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}},
                    },
                },
            },
            "index": 0,
        }
    })

    worksheet.spreadsheet.batch_update({"requests": requests})
    print(f"Applied conditional formatting: Days Vacant (>30 light red, >60 dark red) "
          f"+ NEED TO POST ({vu_count} VU rows).")


# ==========================================================================
# Main
# ==========================================================================

def main():
    reports_client_id = get_env_var('REPORTS_CLIENT_ID')
    reports_client_secret = get_env_var('REPORTS_CLIENT_SECRET')
    developer_id = get_env_var('DEVELOPER_ID')
    appfolio_database_name = get_env_var('APPFOLIO_DATABASE_NAME')

    print(f"=== Starting availability update ===")
    print(f"  PROPERTY_GROUP_ID = {PROPERTY_GROUP_ID!r}")
    print(f"  GOOGLE_SHEET_NAME = {os.environ.get('GOOGLE_SHEET_NAME', '(not set)')!r}")
    print(f"  GOOGLE_SHEET_ID   = {os.environ.get('GOOGLE_SHEET_ID', '(not set)')!r}")
    print(f"  WORKSHEET_NAME    = {WORKSHEET_NAME!r}")

    headers = build_auth_headers(reports_client_id, reports_client_secret, developer_id)
    base_url = f"https://{appfolio_database_name}.appfolio.com/api/v2/reports"

    # Step 1: Fetch from AppFolio
    appfolio_units = fetch_availability_data(headers, base_url)

    # Step 2: Read existing sheet
    temp_cred_path = "/tmp/google_credentials.json"
    try:
        with open(temp_cred_path, "w") as f:
            f.write(get_env_var('GOOGLE_CREDENTIALS_JSON_CONTENT'))
        gc = gspread.service_account(filename=temp_cred_path)
        sheet_id = os.environ.get('GOOGLE_SHEET_ID') or '1ZIdp9fIYSE80OqhLr12Ik7PqJli2SLTX7wQhYzo2OeY'
        spreadsheet = gc.open_by_key(sheet_id)
        print(f"Spreadsheet ID: {spreadsheet.id}")
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
            print(f"\nOpened existing worksheet '{WORKSHEET_NAME}'.")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=200, cols=9)
            print(f"\nCreated new worksheet '{WORKSHEET_NAME}'.")
        existing_values = worksheet.get_all_values()
        print(f"Read {len(existing_values)} existing rows from sheet.")
    except Exception as e:
        print(f"ERROR reading sheet: {e}")
        raise
    finally:
        if os.path.exists(temp_cred_path):
            os.remove(temp_cred_path)

    existing_data = parse_existing_sheet(existing_values)
    print(f"Parsed {len(existing_data)} existing properties.")

    # Step 3: Merge
    sections = merge_data(appfolio_units, existing_data)

    # Step 4: Build output
    output_rows = build_output_rows(sections)

    # Step 5: Write to sheet
    write_to_sheet(worksheet, output_rows)

    # Step 6: Apply NEED TO POST dropdown chips and colors
    apply_need_to_post_chips(worksheet, sections, output_rows)

    print("\nScript complete.")


if __name__ == "__main__":
    import sys

    # Load credentials from config.yaml for local runs
    try:
        import yaml
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        for key, val in cfg.items():
            if isinstance(val, str):
                os.environ[key] = val
            elif isinstance(val, dict):
                os.environ[key] = json.dumps(val)
            else:
                os.environ[key] = str(val)
    except ImportError:
        print("PyYAML not installed. Set environment variables manually or: pip install pyyaml")
        sys.exit(1)
    except FileNotFoundError:
        pass

    main()
