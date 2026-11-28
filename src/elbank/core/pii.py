"""PII protection for logs and audit events.

- :func:`mask_phone`: ``+79991234567`` → ``+7***4567``.
- :func:`mask_pan`: ``2200123456781234`` → ``**** 1234``.
- :func:`redact`: recursively cleans a structure before it is logged or audited:
  secrets and documents (password, OTP codes, tokens, CVV, passport data, PAN) are
  replaced with ``[REDACTED]`` by key; phone-like keys are masked; any string is
  scrubbed of phone numbers (``+7``/``%2B7``/``7``/``8`` + 10 digits) and card numbers
  (16-19 digits; 13-15 digits passing the Luhn check), so PII inside free text (an uvicorn access
  log line with a query string, an exception message) is masked too.

Domain Kafka events are not covered: their consumers need real values (an SMS
needs the phone number); they never reach Elasticsearch.
"""

import re
from collections.abc import Mapping
from typing import Any

REDACTED = "[REDACTED]"

# Compared after lower-casing and dropping "_"/"-": "cardNumber", "card_number" match.
_SECRET_KEYS = frozenset(
    {
        "password",
        "oldpassword",
        "newpassword",
        "passwordhash",
        "otp",
        "otpcode",
        "oldcode",
        "newcode",
        "otptoken",
        "token",
        "accesstoken",
        "refreshtoken",
        "authorization",
        "secret",
        "pan",
        "cardnumber",
        "pannumber",
        "panhash",
        "cvv",
        "cvv2",
        "cvc",
        "cvc2",
        "passport",
        "passportseries",
        "passportnumber",
        "birthdate",
    }
)
_PHONE_KEYS = frozenset({"phone", "newphone", "oldphone", "holderphone", "recipientphone"})

# +7 / %2B7 (URL-encoded) / 7 / 8, then 10 digits with optional separators.
_PHONE_RE = re.compile(
    r"(?<![\w+])(?:\+7|%2B7|7|8)[\s(-]*\d{3}[\s)-]*\d{3}[\s-]*\d{2}[\s-]*\d{2}(?!\d)",
    re.IGNORECASE,
)
# Contiguous 13-19 digits, or 4-digit groups separated by spaces/dashes ("2200 1234 ...").
_CARD_RE = re.compile(r"(?<!\d)(?:\d{13,19}|\d{4}(?:[ -]\d{4}){2}[ -]\d{1,7})(?!\d)")
_MIN_PHONE_DIGITS = 4
_MAX_DEPTH = 20


def _norm(key: str) -> str:
    return key.lower().replace("_", "").replace("-", "")


def mask_phone(phone: str) -> str:
    """``+79991234567`` → ``+7***4567``; anything too short → ``***``."""
    digits = re.sub(r"\D", "", re.sub(r"^%2B", "", phone.strip(), flags=re.IGNORECASE))
    if len(digits) <= _MIN_PHONE_DIGITS:
        return "***"
    country = "+7" if digits[0] in "78" and len(digits) == 11 else f"+{digits[0]}"  # noqa: PLR2004
    return f"{country}***{digits[-4:]}"


def mask_pan(pan: str) -> str:
    digits = re.sub(r"\D", "", pan)
    return f"**** {digits[-4:]}" if len(digits) >= 4 else "****"  # noqa: PLR2004


def _luhn_ok(digits: str) -> bool:
    total = 0
    for index, char in enumerate(reversed(digits)):
        value = int(char)
        if index % 2:
            value *= 2
            if value > 9:  # noqa: PLR2004
                value -= 9
        total += value
    return total % 10 == 0


_ALWAYS_PAN_DIGITS = 16  # 16-19 digits are masked always, 13-15 only if Luhn-valid


def _mask_card(match: re.Match[str]) -> str:
    digits = re.sub(r"\D", "", match.group())
    if len(digits) >= _ALWAYS_PAN_DIGITS or _luhn_ok(digits):
        return mask_pan(digits)
    return match.group()


def scrub_text(text: str) -> str:
    """Mask phone and card numbers inside free text."""
    text = _PHONE_RE.sub(lambda m: mask_phone(m.group()), text)
    return _CARD_RE.sub(_mask_card, text)


def redact(value: Any, *, _depth: int = 0) -> Any:
    """A copy of ``value`` safe for logs/audit (mappings, sequences and strings are cleaned)."""
    if _depth > _MAX_DEPTH:
        return REDACTED
    if isinstance(value, str):
        return scrub_text(value)
    if isinstance(value, Mapping):
        cleaned: dict[Any, Any] = {}
        for key, item in value.items():
            norm = _norm(str(key))
            if norm in _SECRET_KEYS:
                cleaned[key] = REDACTED if item is not None else None
            elif norm in _PHONE_KEYS and isinstance(item, str):
                cleaned[key] = mask_phone(item)
            else:
                cleaned[key] = redact(item, _depth=_depth + 1)
        return cleaned
    if isinstance(value, list | tuple):
        return [redact(item, _depth=_depth + 1) for item in value]
    return value
