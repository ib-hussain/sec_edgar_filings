"""Address normalisation helpers for SEC submission headers."""

ADDRESS_FIELDS = {
    "STREET 1": "street1",
    "STREET 2": "street2",
    "CITY": "city",
    "STATE": "state",
    "ZIP": "zip",
    "BUSINESS PHONE": "phone",
}


def normalize_address_fields(values: dict[str, str]) -> dict[str, str | None]:
    result: dict[str, str | None] = {
        "street1": None, "street2": None, "city": None,
        "state": None, "zip": None, "phone": None,
    }
    for key, value in values.items():
        mapped = ADDRESS_FIELDS.get(key.upper().strip())
        if mapped:
            result[mapped] = value.strip() or None
    return result
