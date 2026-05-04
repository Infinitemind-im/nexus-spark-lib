"""FX rate models for Stage 1 currency normalisation.

Currency normalisation converts monetary fields to the tenant's base currency
using the rate at the record's source_ts (not the processing time).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass
class FxRateEntry:
    """One exchange rate: from_currency → to_currency at a point in time."""

    from_currency: str             # ISO 4217 e.g. "USD"
    to_currency: str               # ISO 4217 e.g. "EUR"
    rate: Decimal
    valid_from: datetime
    valid_until: datetime | None   # None = currently active rate
    is_approximate: bool = False   # True when this is a fallback (exact rate unavailable)


@dataclass
class FxRates:
    """Full FX rates snapshot used as a Spark broadcast variable.

    Rates are stored as a nested dict for O(1) lookup:
        rates[from_currency][to_currency] = list[FxRateEntry] (sorted by valid_from DESC)

    FXService.convert() walks the list to find the entry whose valid_from ≤ source_ts.
    When no exact match is found, the most recent entry is used with is_approximate=True.
    """

    rates: dict[str, dict[str, list[FxRateEntry]]] = field(default_factory=dict)
    snapshot_ts: datetime = field(default_factory=datetime.utcnow)

    def convert(
        self,
        amount: Decimal,
        from_currency: str,
        to_currency: str,
        at: datetime,
    ) -> "FxConversionResult":
        """Convert amount from_currency → to_currency at the given timestamp."""
        if from_currency == to_currency:
            return FxConversionResult(
                amount=amount,
                currency=to_currency,
                original_amount=amount,
                original_currency=from_currency,
                rate=Decimal("1"),
                rate_approximate=False,
            )

        entries = self.rates.get(from_currency, {}).get(to_currency, [])
        if not entries:
            # Try inverse rate
            inv_entries = self.rates.get(to_currency, {}).get(from_currency, [])
            if inv_entries:
                entry = self._find_entry(inv_entries, at)
                rate = Decimal("1") / entry.rate
                return FxConversionResult(
                    amount=(amount * rate).quantize(Decimal("0.00000001")),
                    currency=to_currency,
                    original_amount=amount,
                    original_currency=from_currency,
                    rate=rate,
                    rate_approximate=entry.is_approximate,
                )
            raise ValueError(
                f"No FX rate available for {from_currency} → {to_currency}"
            )

        entry = self._find_entry(entries, at)
        converted = (amount * entry.rate).quantize(Decimal("0.00000001"))
        return FxConversionResult(
            amount=converted,
            currency=to_currency,
            original_amount=amount,
            original_currency=from_currency,
            rate=entry.rate,
            rate_approximate=entry.is_approximate,
        )

    @staticmethod
    def _find_entry(entries: list[FxRateEntry], at: datetime) -> FxRateEntry:
        """Find the rate entry valid at the given datetime (sorted DESC)."""
        for entry in entries:  # entries sorted valid_from DESC
            if entry.valid_from <= at:
                return entry
        # All entries are newer than at — use oldest available (most approximate)
        oldest = entries[-1]
        oldest.is_approximate = True
        return oldest


@dataclass
class FxConversionResult:
    """Result of a single FX conversion."""

    amount: Decimal                # Converted amount in to_currency
    currency: str                  # to_currency
    original_amount: Decimal       # Original amount in from_currency
    original_currency: str         # from_currency
    rate: Decimal                  # Exchange rate applied
    rate_approximate: bool         # True when exact historical rate was unavailable
