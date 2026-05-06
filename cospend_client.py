"""Shared Cospend client library for Nextcloud Cospend REST API."""

import logging
import sys

import requests

logger = logging.getLogger(__name__)


class CospendClient:
    """Synchronous client for the Nextcloud Cospend REST API."""

    def __init__(self, nextcloud_url: str, project_id: str, project_password: str):
        self._base_url = (
            f"{nextcloud_url}/index.php/apps/cospend/api/projects"
            f"/{project_id}/{project_password}"
        )

    def get_project_info(self) -> dict:
        """GET project info including members, categories, payment modes."""
        response = requests.get(self._base_url)
        response.raise_for_status()
        return response.json()

    def get_bills(self) -> list[dict]:
        """GET existing bills from the project."""
        url = f"{self._base_url}/bills"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def create_bill(self, payload: dict) -> int:
        """POST a new bill. Returns the created bill ID."""
        url = f"{self._base_url}/bills"
        response = requests.post(url, json=payload)
        if not response.ok:
            logging.error(
                "Cospend API error %d: %s", response.status_code, response.text
            )
            sys.exit(1)
        return response.json()


def resolve_by_name(items: dict, name: str, label: str) -> int:
    """Resolve a Cospend entity (category or payment mode) by case-insensitive name.

    items is the dict from the project API (keyed by string ID, each value has 'id' and 'name').
    Returns the integer ID, or raises SystemExit if not found.
    """
    name_lower = name.strip().lower()
    for item in items.values():
        if item["name"].lower() == name_lower:
            return item["id"]
    available = ", ".join(sorted(item["name"] for item in items.values()))
    raise SystemExit(
        f"{label} '{name}' not found in project. Available: {available}"
    )


def resolve_project_ids(
    project_info: dict,
    payer_userid: str,
    payed_for_userids: str = "",
    category_name: str = "",
    payment_mode_name: str = "",
) -> dict:
    """Resolve userid/name strings to Cospend integer IDs.

    Returns a dict with keys: payer_id, payed_for_str, category_id, payment_mode_id.
    category_id and payment_mode_id may be None if not configured.
    """
    members = project_info.get("members", [])
    userid_to_id = {m["userid"]: m["id"] for m in members}
    active_ids = [m["id"] for m in project_info.get("active_members", [])]

    # Resolve payer
    if payer_userid not in userid_to_id:
        available = ", ".join(sorted(userid_to_id.keys()))
        raise SystemExit(
            f"Payer userid '{payer_userid}' not found in project. "
            f"Available userids: {available}"
        )
    payer_id = userid_to_id[payer_userid]

    # Resolve payed_for: blank means all active members
    if not payed_for_userids.strip():
        payed_for_ids = active_ids
        logger.info(
            "COSPEND_PAYED_FOR is blank, using all active members: %s",
            ",".join(str(i) for i in payed_for_ids),
        )
    else:
        userids = [u.strip() for u in payed_for_userids.split(",") if u.strip()]
        payed_for_ids = []
        for uid in userids:
            if uid not in userid_to_id:
                available = ", ".join(sorted(userid_to_id.keys()))
                raise SystemExit(
                    f"Payed-for userid '{uid}' not found in project. "
                    f"Available userids: {available}"
                )
            payed_for_ids.append(userid_to_id[uid])

    payed_for_str = ",".join(str(i) for i in payed_for_ids)

    # Resolve category (optional)
    category_id = None
    if category_name.strip():
        categories = project_info.get("categories", {})
        category_id = resolve_by_name(categories, category_name, "Category")
        logger.info("Resolved category '%s' to ID %d", category_name, category_id)

    # Resolve payment mode (optional)
    payment_mode_id = None
    if payment_mode_name.strip():
        modes = project_info.get("paymentmodes", {})
        payment_mode_id = resolve_by_name(modes, payment_mode_name, "Payment mode")
        logger.info("Resolved payment mode '%s' to ID %d", payment_mode_name, payment_mode_id)

    return {
        "payer_id": payer_id,
        "payed_for_str": payed_for_str,
        "category_id": category_id,
        "payment_mode_id": payment_mode_id,
    }
