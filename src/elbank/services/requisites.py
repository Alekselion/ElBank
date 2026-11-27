"""Account numbers and bank requisites (Bank of Russia rules; IBAN is a pseudo one).

Account number (20 digits), Bank of Russia Regulation 579-P::

    AAAAA  CCC  K  BBBB  NNNNNNN
    |      |    |  |     └ personal account (7 digits, random)
    |      |    |  └ branch code
    |      |    └ control key
    |      └ currency code (ISO 4217 numeric; rouble accounts use 810)
    └ balance (ledger) account: 40817 individual's current, 42301 demand deposit

The control key is computed over 23 digits — the last 3 BIC digits followed by the
account number (for a correspondent account: ``0`` + BIC digits 5-6) — with
weights 7,1,3 repeated: the sum of the last digits of the products must end in 0.
"""

import secrets
from dataclasses import dataclass

from elbank.core.config import BankSettings
from elbank.db.models import AccountType, Currency

_ACCOUNT_NUMBER_LENGTH = 20
_CHECKED_LENGTH = 3 + _ACCOUNT_NUMBER_LENGTH  # 3-digit BIC part + account number
_WEIGHTS = ((7, 1, 3) * 8)[:_CHECKED_LENGTH]
_KEY_POSITION = 8  # zero-based position of the control key in a 20-digit account number

_BALANCE_ACCOUNT: dict[AccountType, str] = {
    AccountType.CHECKING: "40817",  # current account of an individual
    AccountType.SAVINGS: "42301",  # demand deposit of an individual
    AccountType.FOREIGN: "40817",  # same ledger account, foreign currency code
}

_CURRENCY_CODE: dict[Currency, str] = {
    Currency.RUB: "810",
    Currency.USD: "840",
    Currency.EUR: "978",
    Currency.CNY: "156",
}

_CORR_ACCOUNT_LEDGER = "30101"  # correspondent account at the Bank of Russia


def _checksum(digits: str) -> int:
    return sum(int(d) * w % 10 for d, w in zip(digits, _WEIGHTS, strict=True)) % 10


def _with_control_key(prefix: str, number: str) -> str:
    """Return ``number`` with the control key placed at position 9."""
    zeroed = number[:_KEY_POSITION] + "0" + number[_KEY_POSITION + 1 :]
    key = _checksum(prefix + zeroed) * 3 % 10  # weight 3 at the key: 3k ≡ -sum (mod 10)
    return number[:_KEY_POSITION] + str(key) + number[_KEY_POSITION + 1 :]


def is_valid_account_number(number: str, bic: str) -> bool:
    if len(number) != _ACCOUNT_NUMBER_LENGTH or not number.isdigit():
        return False
    return _checksum(bic[-3:] + number) == 0


def generate_account_number(
    account_type: AccountType, currency: Currency, bank: BankSettings
) -> str:
    """A new random account number with a valid control key for the bank's BIC."""
    personal = f"{secrets.randbelow(10**7):07d}"
    draft = (
        f"{_BALANCE_ACCOUNT[account_type]}{_CURRENCY_CODE[currency]}0{bank.branch_code}{personal}"
    )
    return _with_control_key(bank.bic[-3:], draft)


def correspondent_account(bic: str) -> str:
    """The bank's correspondent account at the Bank of Russia, derived from its BIC."""
    draft = f"{_CORR_ACCOUNT_LEDGER}{_CURRENCY_CODE[Currency.RUB]}0{"0" * 8}{bic[-3:]}"
    return _with_control_key("0" + bic[4:6], draft)


def pseudo_iban(account_number: str, bic: str) -> str:
    """ISO 13616-style IBAN ``RU kk <BIC> <account>`` with valid mod-97 check digits.

    Russia does not use IBAN domestically; this is for display in the emulator only.
    """
    bban = bic + account_number
    # Move "RU00" to the end and replace letters with numbers: R=27, U=30.
    check = 98 - int(bban + "273000") % 97
    return f"RU{check:02d}{bban}"


@dataclass(frozen=True, slots=True)
class Requisites:
    recipient: str | None  # NULL until the legal name is known (KYC)
    account_number: str
    currency: Currency
    bank_name: str
    bic: str
    correspondent_account: str
    bank_inn: str
    bank_kpp: str
    swift: str
    iban: str


def build_requisites(
    recipient: str | None, account_number: str, currency: Currency, bank: BankSettings
) -> Requisites:
    return Requisites(
        recipient=recipient,
        account_number=account_number,
        currency=currency,
        bank_name=bank.name,
        bic=bank.bic,
        correspondent_account=correspondent_account(bank.bic),
        bank_inn=bank.inn,
        bank_kpp=bank.kpp,
        swift=bank.swift,
        iban=pseudo_iban(account_number, bank.bic),
    )
