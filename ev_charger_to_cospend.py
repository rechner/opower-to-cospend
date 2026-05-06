"""EV Charger to Cospend: Sync EV charger usage from Google Sheets to Nextcloud Cospend."""

import os
import sys
import logging
import argparse
from dataclasses import dataclass
from datetime import date

import gspread

from cospend_client import CospendClient, resolve_by_name, resolve_project_ids

logger = logging.getLogger(__name__)

_REQUIRED_ENV_VARS = {
    "NEXTCLOUD_URL": "nextcloud_url",
    "COSPEND_PROJECT_ID": "cospend_project_id",
    "COSPEND_PROJECT_PASSWORD": "cospend_project_password",
    "COSPEND_PAYER": "cospend_payer",
    "GOOGLE_CREDENTIALS_FILE": "google_credentials_file",
    "GOOGLE_SHEET_ID": "google_sheet_id",
}


@dataclass(frozen=True)
class Config:
    """Configuration loaded from environment variables."""

    nextcloud_url: str
    cospend_project_id: str
    cospend_project_password: str
    cospend_payer: str
    cospend_category: str
    cospend_payment_mode: str
    google_credentials_file: str
    google_sheet_id: str
    dry_run: bool = False

    @classmethod
    def from_env(cls, dry_run: bool = False) -> "Config":
        """Load configuration from environment variables.

        Raises SystemExit with a message naming every missing variable.
        """
        missing = [var for var in _REQUIRED_ENV_VARS if var not in os.environ]
        if missing:
            raise SystemExit(
                f"Missing required environment variables: {', '.join(sorted(missing))}"
            )

        return cls(
            nextcloud_url=os.environ["NEXTCLOUD_URL"],
            cospend_project_id=os.environ["COSPEND_PROJECT_ID"],
            cospend_project_password=os.environ["COSPEND_PROJECT_PASSWORD"],
            cospend_payer=os.environ["COSPEND_PAYER"],
            cospend_category=os.environ.get("COSPEND_CATEGORY", ""),
            cospend_payment_mode=os.environ.get("COSPEND_PAYMENT_MODE", ""),
            google_credentials_file=os.environ["GOOGLE_CREDENTIALS_FILE"],
            google_sheet_id=os.environ["GOOGLE_SHEET_ID"],
            dry_run=dry_run,
        )

    def __repr__(self) -> str:
        return (
            f"Config("
            f"nextcloud_url={self.nextcloud_url!r}, "
            f"cospend_project_id={self.cospend_project_id!r}, "
            f"cospend_project_password='***', "
            f"cospend_payer={self.cospend_payer!r}, "
            f"cospend_category={self.cospend_category!r}, "
            f"cospend_payment_mode={self.cospend_payment_mode!r}, "
            f"google_credentials_file='***', "
            f"google_sheet_id={self.google_sheet_id!r}, "
            f"dry_run={self.dry_run!r})"
        )


def read_totals(sheet: gspread.Worksheet) -> list[dict]:
    """Read columns H and I to get per-person totals.

    Returns list of dicts: [{"name": str, "amount": float}, ...]
    Only includes rows where amount > 0.
    """
    # Get all values from columns H and I, using unformatted values
    # so currency-formatted cells return raw numbers instead of "$31.53"
    col_h = sheet.col_values(8)  # Column H = names
    col_i = sheet.col_values(9, value_render_option=gspread.utils.ValueRenderOption.unformatted)  # Column I = amounts
    results = []
    for i, name in enumerate(col_h):
        if not name or not name.strip():
            continue
        if name.strip().lower() == "total":
            continue
        # Get corresponding amount (col_i may be shorter)
        amount_val = col_i[i] if i < len(col_i) else ""
        if amount_val == "" or amount_val is None:
            continue
        try:
            amount = float(amount_val)
        except (ValueError, TypeError):
            continue
        if amount > 0:
            results.append({"name": name.strip(), "amount": amount})

    return results


def record_payment(
    sheet: gspread.Worksheet, name: str, date_str: str, amount: float
) -> None:
    """Append a payment row to columns K-M (starting at row 2).

    Writes: [name, date_str, amount] to the next empty row in columns K:M.
    """
    # Find the next empty row in column K
    col_k = sheet.col_values(11)  # Column K
    next_row = len(col_k) + 1

    # Update cells K, L, M in the next row
    sheet.update_cell(next_row, 11, name)
    sheet.update_cell(next_row, 12, date_str)
    sheet.update_cell(next_row, 13, amount)


def match_name_to_member(name: str, members: list[dict]) -> int | None:
    """Match a person name to a Cospend member ID.

    Compares name (case-insensitive) against the first word of each member's
    display name (the 'name' field in the member dict).

    Returns the member's 'id' (int) or None if no match found.
    """
    name_lower = name.strip().lower()
    for member in members:
        display_name = member.get("name", "")
        first_word = display_name.split()[0] if display_name.strip() else ""
        if first_word.lower() == name_lower:
            return member["id"]
    return None


def build_ev_bill_payload(
    name: str,
    amount: float,
    payer_id: int,
    member_id: int,
    category_id: int | None,
    payment_mode_id: int | None,
) -> dict:
    """Build a Cospend bill payload for an EV charging charge.

    The 'what' field format: "EV Charging - {name} - {YYYY-MM-DD}"
    """
    today = date.today().isoformat()
    payload = {
        "amount": amount,
        "what": f"EV Charging - {name} - {today}",
        "payer": payer_id,
        "payed_for": str(member_id),
        "timestamp": int(date.today().strftime("%s")),
        "comment": "EV charger usage billed from Google Sheet",
    }
    if category_id is not None:
        payload["categoryid"] = category_id
    if payment_mode_id is not None:
        payload["paymentmodeid"] = payment_mode_id
    return payload


def is_duplicate(existing_bills: list[dict], what: str) -> bool:
    """Return True if any existing bill has a matching 'what' field."""
    return any(bill.get("what") == what for bill in existing_bills)


def main() -> None:
    """CLI entry point: sync EV charger usage from Google Sheet to Cospend."""
    parser = argparse.ArgumentParser(
        description="Sync EV charger usage from Google Sheet to Cospend"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Log actions without making changes"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        config = Config.from_env(dry_run=args.dry_run)
        logger.info("Configuration loaded successfully (dry_run=%s)", config.dry_run)

        # Authenticate with Google Sheets
        logger.info("Authenticating with Google Sheets API...")
        try:
            gc = gspread.service_account(filename=config.google_credentials_file)
        except Exception as exc:
            raise SystemExit(
                f"Failed to authenticate with Google Sheets: {exc}"
            )

        # Open the sheet
        logger.info("Opening Google Sheet...")
        try:
            spreadsheet = gc.open_by_key(config.google_sheet_id)
        except Exception as exc:
            raise SystemExit(
                f"Cannot open Google Sheet with ID: {config.google_sheet_id} — {exc}"
            )

        sheet = spreadsheet.worksheet("ChargePoint Home")

        # Read totals
        logger.info("Reading totals from sheet...")
        totals = read_totals(sheet)
        if not totals:
            logger.info("No outstanding amounts found. Nothing to do.")
            sys.exit(0)
        logger.info("Found %d people with outstanding amounts", len(totals))

        # Set up Cospend client
        logger.info("Connecting to Cospend...")
        client = CospendClient(
            config.nextcloud_url, config.cospend_project_id, config.cospend_project_password
        )

        project_info = client.get_project_info()
        existing_bills = client.get_bills()
        logger.info("Fetched %d existing bills from Cospend", len(existing_bills))

        # Resolve payer ID
        ids = resolve_project_ids(
            project_info,
            payer_userid=config.cospend_payer,
            category_name=config.cospend_category,
            payment_mode_name=config.cospend_payment_mode,
        )
        payer_id = ids["payer_id"]
        category_id = ids["category_id"]
        payment_mode_id = ids["payment_mode_id"]

        members = project_info.get("members", [])

        # Process each person
        for entry in totals:
            name = entry["name"]
            amount = entry["amount"]

            # Match name to member
            member_id = match_name_to_member(name, members)
            if member_id is None:
                logger.warning(
                    "Could not match '%s' to any Cospend member — skipping", name
                )
                continue

            # Build bill payload
            payload = build_ev_bill_payload(
                name, amount, payer_id, member_id, category_id, payment_mode_id
            )

            # Check for duplicate
            if is_duplicate(existing_bills, payload["what"]):
                logger.info(
                    "Bill already exists: '%s' — skipping", payload["what"]
                )
                continue

            # Create bill
            if config.dry_run:
                logger.info(
                    "Dry run: would create bill '%s' for $%.2f", payload["what"], amount
                )
            else:
                logger.info("Creating bill: '%s' for $%.2f", payload["what"], amount)
                client.create_bill(payload)
                logger.info("Bill created successfully")

                # Record payment in sheet
                today_str = date.today().isoformat()
                try:
                    record_payment(sheet, name, today_str, amount)
                    logger.info("Payment recorded in sheet for %s", name)
                except Exception as exc:
                    logger.error(
                        "Failed to record payment in sheet for %s: %s", name, exc
                    )

        logger.info("Done.")
        sys.exit(0)

    except SystemExit:
        raise


if __name__ == "__main__":
    main()
