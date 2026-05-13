"""PG&E to Cospend: Sync PG&E electric and gas bills to Nextcloud Cospend."""

import json
import os
import sys
import logging
import argparse
import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import sentry_sdk
import requests
import aiohttp
import gspread
from opower import Opower, MfaChallenge, MeterType, AggregateType, create_cookie_jar

from cospend_client import CospendClient, resolve_by_name, resolve_project_ids
from ev_charger_to_cospend import (
    read_totals as read_ev_totals,
    record_payment as record_ev_payment,
    match_name_to_member,
    build_ev_bill_payload,
)

logger = logging.getLogger(__name__)

_REQUIRED_ENV_VARS = {
    "PGE_USERNAME": "pge_username",
    "PGE_PASSWORD": "pge_password",
    "NEXTCLOUD_URL": "nextcloud_url",
    "COSPEND_PROJECT_ID": "cospend_project_id",
    "COSPEND_PROJECT_PASSWORD": "cospend_project_password",
    "COSPEND_PAYER": "cospend_payer",
}


@dataclass(frozen=True)
class Config:
    """Configuration loaded from environment variables."""

    pge_username: str
    pge_password: str
    nextcloud_url: str
    cospend_project_id: str
    cospend_project_password: str
    cospend_payer: str
    cospend_payed_for: str
    cospend_category: str
    cospend_payment_mode: str
    login_data_path: str = ".pge_login_data.json"
    dry_run: bool = False

    @classmethod
    def from_env(cls, dry_run: bool = False) -> "Config":
        """Load configuration from environment variables.

        Raises SystemExit with a message naming every missing variable.
        """
        missing = [
            var for var in _REQUIRED_ENV_VARS if var not in os.environ
        ]
        if missing:
            raise SystemExit(
                f"Missing required environment variables: {', '.join(sorted(missing))}"
            )

        return cls(
            pge_username=os.environ["PGE_USERNAME"],
            pge_password=os.environ["PGE_PASSWORD"],
            nextcloud_url=os.environ["NEXTCLOUD_URL"],
            cospend_project_id=os.environ["COSPEND_PROJECT_ID"],
            cospend_project_password=os.environ["COSPEND_PROJECT_PASSWORD"],
            cospend_payer=os.environ["COSPEND_PAYER"],
            cospend_payed_for=os.environ.get("COSPEND_PAYED_FOR", ""),
            cospend_category=os.environ.get("COSPEND_CATEGORY", ""),
            cospend_payment_mode=os.environ.get("COSPEND_PAYMENT_MODE", ""),
            login_data_path=os.environ.get("PGE_LOGIN_DATA_PATH", ".pge_login_data.json"),
            dry_run=dry_run,
        )

    def __repr__(self) -> str:
        return (
            f"Config("
            f"pge_username={self.pge_username!r}, "
            f"pge_password='***', "
            f"nextcloud_url={self.nextcloud_url!r}, "
            f"cospend_project_id={self.cospend_project_id!r}, "
            f"cospend_project_password='***', "
            f"cospend_payer={self.cospend_payer!r}, "
            f"login_data_path={self.login_data_path!r}, "
            f"dry_run={self.dry_run!r})"
        )


_METER_LABELS = {
    MeterType.ELEC: ("Electric", "kWh"),
    MeterType.GAS: ("Gas", "therms"),
}


# Peninsula Clean Energy generation rates for E-ELEC schedule ($/kWh).
# Updated: 2025-02-01. Check https://www.peninsulacleanenergy.com/residential/rates-billing/residential-rates/e-elec
_PCE_ECOPLUS_RATES = {
    "summer": {  # June 1 - September 30
        "on_peak": 0.19013,       # 4pm-9pm every day
        "partial_peak": 0.10093,  # 3pm-4pm and 9pm-12am every day
        "off_peak": 0.06034,      # All other hours
    },
    "winter": {  # October 1 - May 31
        "on_peak": 0.04422,       # 4pm-9pm every day
        "partial_peak": 0.02624,  # 3pm-4pm and 9pm-12am every day
        "off_peak": 0.01423,      # All other hours
    },
}

# ECO100 surcharge on top of ECOplus ($/kWh), applied uniformly to all periods.
_PCE_ECO100_SURCHARGE = 0.01

# Energy Commission Surcharge ($/kWh), a small regulatory fee added by PCE.
# Current rate: https://cdtfa.ca.gov/taxes-and-fees/special-taxes-and-fees-tax-rates/#energysurcharge
_PCE_ENERGY_COMMISSION_SURCHARGE = 0.00030


def _get_e_elec_season(dt: datetime) -> str:
    """Return 'summer' or 'winter' for the E-ELEC rate schedule."""
    # Summer: June 1 - September 30
    if 6 <= dt.month <= 9:
        return "summer"
    return "winter"


def _get_e_elec_tou_period(dt: datetime) -> str:
    """Classify an hour into E-ELEC TOU period based on start hour.

    E-ELEC schedule:
      On-peak: 4pm-9pm (hours 16-20)
      Partial-peak: 3pm-4pm (hour 15) and 9pm-12am (hours 21-23)
      Off-peak: all other hours (0-14)
    """
    hour = dt.hour
    if 16 <= hour <= 20:
        return "on_peak"
    elif hour == 15 or 21 <= hour <= 23:
        return "partial_peak"
    else:
        return "off_peak"


def calculate_pce_generation_cost(hourly_reads: list) -> tuple[float, float, float, dict]:
    """Calculate PCE generation charges from hourly consumption data.

    Returns (ecoplus_cost, eco100_surcharge, energy_commission_surcharge, breakdown) where:
      - ecoplus_cost is the base generation charge
      - eco100_surcharge is the flat per-kWh ECO100 premium
      - energy_commission_surcharge is the regulatory fee
      - breakdown is a dict with per-period kWh for logging
    """
    breakdown = {
        "summer": {"on_peak_kwh": 0.0, "partial_peak_kwh": 0.0, "off_peak_kwh": 0.0},
        "winter": {"on_peak_kwh": 0.0, "partial_peak_kwh": 0.0, "off_peak_kwh": 0.0},
    }

    total_kwh = 0.0
    for read in hourly_reads:
        season = _get_e_elec_season(read.start_time)
        period = _get_e_elec_tou_period(read.start_time)
        breakdown[season][f"{period}_kwh"] += read.consumption
        total_kwh += read.consumption

    ecoplus_cost = 0.0
    for season in ("summer", "winter"):
        rates = _PCE_ECOPLUS_RATES[season]
        for period in ("on_peak", "partial_peak", "off_peak"):
            kwh = breakdown[season][f"{period}_kwh"]
            ecoplus_cost += kwh * rates[period]

    eco100_surcharge = total_kwh * _PCE_ECO100_SURCHARGE
    energy_commission_surcharge = total_kwh * _PCE_ENERGY_COMMISSION_SURCHARGE

    return ecoplus_cost, eco100_surcharge, energy_commission_surcharge, breakdown


def build_bill_payload(
    cost_read,
    meter_type: MeterType,
    payer_id: int,
    payed_for_ids: str,
    category_id: int | None = None,
    payment_mode_id: int | None = None,
) -> dict:
    """Convert a CostRead into a Cospend bill creation payload dict."""
    start_date = cost_read.start_time.strftime("%Y-%m-%d")
    end_date = cost_read.end_time.strftime("%Y-%m-%d")
    label, unit = _METER_LABELS.get(meter_type, (str(meter_type), "units"))

    payload = {
        "amount": cost_read.provided_cost,
        "what": f"PG&E {label} {start_date} - {end_date}",
        "payer": payer_id,
        "payed_for": payed_for_ids,
        "timestamp": int(cost_read.end_time.timestamp()),
        "comment": (
            f"Billing period: {start_date} - {end_date}\n"
            f"Consumption: {cost_read.consumption:.1f} {unit}\n"
            f"Cost: ${cost_read.provided_cost:.2f}"
        ),
    }
    if category_id is not None:
        payload["categoryid"] = category_id
    if payment_mode_id is not None:
        payload["paymentmodeid"] = payment_mode_id
    return payload


def is_duplicate(existing_bills: list[dict], what: str) -> bool:
    """Return True if any existing bill has a matching 'what' field."""
    return any(bill.get("what") == what for bill in existing_bills)




def _load_login_data(path: str) -> dict | None:
    """Load saved login data from disk, or return None if not available."""
    p = Path(path)
    if p.exists():
        try:
            data = json.loads(p.read_text())
            logger.info("Loaded saved login data from %s", path)
            return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read login data from %s: %s", path, exc)
    return None


def _save_login_data(path: str, login_data: dict) -> None:
    """Persist login data to disk for future runs."""
    try:
        Path(path).write_text(json.dumps(login_data))
        logger.info("Saved login data to %s", path)
    except OSError as exc:
        logger.warning("Could not save login data to %s: %s", path, exc)


async def _handle_mfa(handler) -> dict:
    """Interactively handle MFA challenge via stdin/stdout.

    Returns the login_data dict from the MFA handler.
    """
    options = await handler.async_get_mfa_options()

    if options:
        print("\nMFA required. Available verification methods:")
        option_ids = list(options.keys())
        for i, (opt_id, opt_label) in enumerate(options.items(), 1):
            print(f"  {i}. {opt_label}")

        while True:
            choice = input(f"Select method (1-{len(option_ids)}): ").strip()
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(option_ids):
                    break
            except ValueError:
                pass
            print(f"Please enter a number between 1 and {len(option_ids)}")

        selected = option_ids[idx]
        logger.info("Requesting MFA code via: %s", options[selected])
        await handler.async_select_mfa_option(selected)
    else:
        print("\nMFA required. A verification code has been sent.")

    code = input("Enter verification code: ").strip()
    login_data = await handler.async_submit_mfa_code(code)
    return login_data


async def fetch_latest_bills(config: Config, target_date: datetime | None = None) -> tuple[list[tuple[MeterType, object]], list | None]:
    """Authenticate with PG&E via opower and return bills for each meter type.

    If target_date is provided, selects the bill whose period contains that date.
    Otherwise selects the most recent bill.

    Returns (bills, hourly_elec_reads) where:
      - bills is a list of (MeterType, CostRead) tuples
      - hourly_elec_reads is a list of hourly CostReads for the electric billing period
        (used for PCE generation calculation), or None if no electric account
    """
    login_data = _load_login_data(config.login_data_path)

    async with aiohttp.ClientSession(cookie_jar=create_cookie_jar()) as session:
        opower_client = Opower(
            session, "Pacific Gas and Electric Company (PG&E)",
            config.pge_username, config.pge_password,
            login_data=login_data,
        )

        logger.info("Authenticating with PG&E via opower...")
        try:
            await opower_client.async_login()
        except MfaChallenge as exc:
            logger.info("MFA challenge received, starting interactive verification...")
            login_data = await _handle_mfa(exc.handler)
            _save_login_data(config.login_data_path, login_data)
            # Retry login with the new login_data
            opower_client = Opower(
                session, "Pacific Gas and Electric Company (PG&E)",
                config.pge_username, config.pge_password,
                login_data=login_data,
            )
            try:
                await opower_client.async_login()
            except Exception as exc2:
                raise SystemExit(
                    f"Failed to authenticate with PG&E after MFA: {exc2}"
                )
        except Exception as exc:
            raise SystemExit(
                f"Failed to authenticate with PG&E via opower: {exc}"
            )

        logger.info("Fetching accounts...")
        accounts = await opower_client.async_get_accounts()
        if not accounts:
            raise SystemExit("No accounts found in opower")

        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)

        results = []
        hourly_elec_reads = None
        elec_account = None

        for meter_type in (MeterType.ELEC, MeterType.GAS):
            matching = [a for a in accounts if a.meter_type == meter_type]
            if not matching:
                label = _METER_LABELS.get(meter_type, (str(meter_type),))[0]
                logger.info("No %s account found, skipping", label)
                continue

            account = matching[0]
            label = _METER_LABELS.get(meter_type, (str(meter_type),))[0]
            logger.info("Using %s account: %s", label.lower(), account.utility_account_id)

            logger.info("Fetching %s bill-level cost reads for the last 12 months...", label.lower())
            cost_reads = await opower_client.async_get_cost_reads(
                account, AggregateType.BILL, start_date, end_date
            )

            if not cost_reads:
                logger.warning("No %s billing data available for the last 12 months", label.lower())
                continue

            sorted_reads = sorted(cost_reads, key=lambda r: r.end_time, reverse=True)

            if target_date:
                # Find the bill whose period contains the target date
                selected = None
                for cr in sorted_reads:
                    # Compare as naive datetimes (strip tz from opower data)
                    start = cr.start_time.replace(tzinfo=None)
                    end = cr.end_time.replace(tzinfo=None)
                    if start <= target_date < end:
                        selected = cr
                        break
                if not selected:
                    available = ", ".join(
                        f"{cr.start_time.strftime('%Y-%m-%d')} to {cr.end_time.strftime('%Y-%m-%d')}"
                        for cr in sorted_reads[:5]
                    )
                    logger.warning(
                        "No %s bill contains date %s. Recent periods: %s",
                        label.lower(), target_date.strftime("%Y-%m-%d"), available,
                    )
                    continue
            else:
                selected = sorted_reads[0]

            logger.info(
                "Selected %s bill: %s - %s, cost=$%.2f",
                label.lower(),
                selected.start_time.strftime("%Y-%m-%d"),
                selected.end_time.strftime("%Y-%m-%d"),
                selected.provided_cost,
            )
            results.append((meter_type, selected))

            if meter_type == MeterType.ELEC:
                elec_account = account

        # Fetch hourly electric data for PCE generation calculation
        if elec_account and results:
            elec_bill = next((cr for mt, cr in results if mt == MeterType.ELEC), None)
            if elec_bill:
                logger.info("Fetching hourly electric data for PCE generation calculation...")
                raw_hourly = await opower_client.async_get_cost_reads(
                    elec_account, AggregateType.HOUR,
                    elec_bill.start_time, elec_bill.end_time,
                )
                # Filter to only hours within the billing period
                # (opower may return extra hours at the boundaries)
                if raw_hourly:
                    hourly_elec_reads = [
                        r for r in raw_hourly
                        if r.start_time >= elec_bill.start_time and r.end_time <= elec_bill.end_time
                    ]
                    logger.info(
                        "Fetched %d hourly reads (%d after filtering to billing period)",
                        len(raw_hourly), len(hourly_elec_reads),
                    )

        if not results:
            raise SystemExit("No billing data available for any meter type")

        return results, hourly_elec_reads





def main() -> None:
    """CLI entry point: sync the latest PG&E bill to Cospend."""
    sentry_dsn = os.environ.get("SENTRY_DSN", "")
    if sentry_dsn:
        sentry_sdk.init(dsn=sentry_dsn, traces_sample_rate=1.0, auto_session_tracking=False)

    parser = argparse.ArgumentParser(description="Sync PG&E bill to Cospend")
    parser.add_argument(
        "--dry-run", action="store_true", help="Log actions without creating bills"
    )
    parser.add_argument(
        "--period", type=str, default=None,
        help="Select bill containing this date (YYYY-MM-DD) instead of the most recent. "
             "Useful for backfilling previous statements."
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Parse --period date if provided
    target_date = None
    if args.period:
        try:
            target_date = datetime.strptime(args.period, "%Y-%m-%d")
        except ValueError:
            raise SystemExit(f"Invalid --period date format: {args.period!r} (expected YYYY-MM-DD)")

    # Determine whether to include EV charging bills
    ev_charging_enabled = os.environ.get("EV_CHARGING_ENABLED", "true").lower() in ("true", "1", "yes")
    include_ev_charging = ev_charging_enabled and target_date is None

    try:
        config = Config.from_env(dry_run=args.dry_run)
        logger.info("Configuration loaded successfully (dry_run=%s)", config.dry_run)

        bills, hourly_elec_reads = asyncio.run(fetch_latest_bills(config, target_date=target_date))

        client = CospendClient(
            config.nextcloud_url, config.cospend_project_id, config.cospend_project_password
        )

        project_info = client.get_project_info()
        ids = resolve_project_ids(
            project_info,
            payer_userid=config.cospend_payer,
            payed_for_userids=config.cospend_payed_for,
            category_name=config.cospend_category,
            payment_mode_name=config.cospend_payment_mode,
        )
        logger.info("Resolved payer ID: %d, payed_for: %s", ids["payer_id"], ids["payed_for_str"])

        existing_bills = client.get_bills()
        logger.info("Fetched %d existing bills from Cospend", len(existing_bills))

        # --- EV Charging: read totals from Google Sheet if enabled ---
        ev_totals = []
        ev_total_cost = 0.0
        ev_sheet = None
        if include_ev_charging:
            google_creds = os.environ.get("GOOGLE_CREDENTIALS_FILE", "")
            google_sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
            if not google_creds or not google_sheet_id:
                logger.warning(
                    "EV_CHARGING_ENABLED is true but GOOGLE_CREDENTIALS_FILE or "
                    "GOOGLE_SHEET_ID not set — skipping EV charging"
                )
                include_ev_charging = False
            else:
                try:
                    gc = gspread.service_account(filename=google_creds)
                    spreadsheet = gc.open_by_key(google_sheet_id)
                    ev_sheet = spreadsheet.worksheet("ChargePoint Home")
                    ev_totals = read_ev_totals(ev_sheet)
                    ev_total_cost = sum(e["amount"] for e in ev_totals)
                    if ev_total_cost <= 0:
                        logger.info("EV charging total is $0 — skipping EV bills")
                        include_ev_charging = False
                    else:
                        logger.info(
                            "EV charging: %d people, total=$%.2f",
                            len(ev_totals), ev_total_cost,
                        )
                except Exception as exc:
                    logger.warning("Failed to read EV charging data: %s — skipping", exc)
                    include_ev_charging = False

        # Collect all payloads to create
        payloads = []

        for meter_type, cost_read in bills:
            if meter_type == MeterType.ELEC and hourly_elec_reads:
                # Combine PG&E delivery + PCE generation into one electric bill
                ecoplus_cost, eco100_surcharge, ec_surcharge, breakdown = calculate_pce_generation_cost(hourly_elec_reads)
                pce_total = ecoplus_cost + eco100_surcharge + ec_surcharge
                total_cost = cost_read.provided_cost + pce_total

                # Subtract EV charging from the electric bill
                if include_ev_charging and ev_total_cost > 0:
                    total_cost -= ev_total_cost
                    logger.info(
                        "Electric total: delivery=$%.2f + PCE=$%.2f - EV=$%.2f = $%.2f",
                        cost_read.provided_cost, pce_total, ev_total_cost, total_cost,
                    )
                else:
                    logger.info(
                        "Electric total: delivery=$%.2f + PCE generation=$%.2f = $%.2f",
                        cost_read.provided_cost, pce_total, total_cost,
                    )

                start_date = cost_read.start_time.strftime("%Y-%m-%d")
                end_date = cost_read.end_time.strftime("%Y-%m-%d")

                comment_lines = [
                    f"Billing: {start_date} - {end_date}",
                    f"{cost_read.consumption:.1f} kWh",
                    "",
                    f"PG&E Delivery: ${cost_read.provided_cost:.2f}",
                    f"PCE Generation: ${pce_total:.2f}",
                    f"  PCE Generation: ${ecoplus_cost:.2f}",
                    f"  PCE ECO100: ${eco100_surcharge:.2f}",
                    f"  EC Surcharge: ${ec_surcharge:.2f}",
                ]
                if include_ev_charging and ev_total_cost > 0:
                    comment_lines.append(f"EV Charging: -${ev_total_cost:.2f}")
                comment_lines.append(f"Total: ${total_cost:.2f}")

                for season in ("summer", "winter"):
                    on = breakdown[season]["on_peak_kwh"]
                    partial = breakdown[season]["partial_peak_kwh"]
                    off = breakdown[season]["off_peak_kwh"]
                    if on or partial or off:
                        comment_lines.append("")
                        comment_lines.append(f"{season.capitalize()}:")
                        comment_lines.append(f"  On-peak: {on:.1f} kWh")
                        comment_lines.append(f"  Partial-peak: {partial:.1f} kWh")
                        comment_lines.append(f"  Off-peak: {off:.1f} kWh")

                comment = "\n".join(comment_lines)
                if len(comment) > 300:
                    comment = comment[:297] + "..."

                payload = {
                    "amount": round(total_cost, 2),
                    "what": f"PG&E Electric {start_date} - {end_date}",
                    "payer": ids["payer_id"],
                    "payed_for": ids["payed_for_str"],
                    "timestamp": int(cost_read.end_time.timestamp()),
                    "comment": comment,
                }
                if ids["category_id"] is not None:
                    payload["categoryid"] = ids["category_id"]
                if ids["payment_mode_id"] is not None:
                    payload["paymentmodeid"] = ids["payment_mode_id"]
                payloads.append(payload)
            else:
                payload = build_bill_payload(
                    cost_read, meter_type,
                    ids["payer_id"], ids["payed_for_str"],
                    ids["category_id"], ids["payment_mode_id"],
                )
                payloads.append(payload)

        # --- EV Charging: build individual bills per person ---
        ev_payloads = []
        if include_ev_charging and ev_totals and ev_total_cost > 0:
            members = project_info.get("members", [])
            for entry in ev_totals:
                name = entry["name"]
                amount = entry["amount"]
                member_id = match_name_to_member(name, members)
                if member_id is None:
                    logger.warning(
                        "Could not match EV name '%s' to any Cospend member — skipping", name
                    )
                    continue
                ev_payload = build_ev_bill_payload(
                    name, amount, ids["payer_id"], member_id,
                    ids["category_id"], ids["payment_mode_id"],
                )
                ev_payloads.append((entry, ev_payload))

        # Create all PG&E bills
        electric_bill_created = False
        for payload in payloads:
            logger.info("Built bill payload: %s", payload["what"])

            if is_duplicate(existing_bills, payload["what"]):
                logger.info(
                    "Bill already exists: %s — skipping creation", payload["what"]
                )
                continue

            if config.dry_run:
                logger.info(
                    "Dry run: would create bill '%s' for $%.2f",
                    payload["what"],
                    payload["amount"],
                )
                if "Electric" in payload["what"]:
                    electric_bill_created = True
                continue

            bill_id = client.create_bill(payload)
            logger.info("Created Cospend bill with ID: %s", bill_id)
            if "Electric" in payload["what"]:
                electric_bill_created = True

        # Create EV charging bills only when the electric bill was newly created
        if not electric_bill_created and ev_payloads:
            logger.info(
                "Electric bill already existed — skipping EV charging bills"
            )
        for entry, ev_payload in ev_payloads:
            if not electric_bill_created:
                break

            if is_duplicate(existing_bills, ev_payload["what"]):
                logger.info(
                    "EV bill already exists: '%s' — skipping", ev_payload["what"]
                )
                continue

            if config.dry_run:
                logger.info(
                    "Dry run: would create EV bill '%s' for $%.2f",
                    ev_payload["what"], ev_payload["amount"],
                )
                continue

            logger.info("Creating EV bill: '%s' for $%.2f", ev_payload["what"], ev_payload["amount"])
            client.create_bill(ev_payload)

            # Record payment in the Google Sheet
            if ev_sheet:
                today_str = date.today().isoformat()
                try:
                    record_ev_payment(ev_sheet, entry["name"], today_str, entry["amount"])
                    logger.info("Payment recorded in sheet for %s", entry["name"])
                except Exception as exc:
                    logger.error(
                        "Failed to record payment in sheet for %s: %s", entry["name"], exc
                    )

        sys.exit(0)

    except SystemExit:
        raise
    #except Exception as exc:
    #    logger.error("Unexpected error: %s", exc)
    #    sys.exit(1)


if __name__ == "__main__":
    main()
