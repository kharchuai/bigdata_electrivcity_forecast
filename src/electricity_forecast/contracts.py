"""Shared event contract: source clock without a time zone, power in kW."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import math
from typing import Any, Mapping

READING_KEY = ("run_id", "customer_id", "reading_end")
HOURLY_KEY = ("run_id", "customer_id", "hour_start")
FEATURE_KEY = ("run_id", "customer_id", "forecast_origin")
PREDICTION_KEY = ("run_id", "customer_id", "forecast_origin", "target_start", "model_version")


class ContractError(ValueError):
    pass


def parse_timestamp(value: str | datetime) -> datetime:
    """Keep the supplied clock value; reject inputs carrying a time zone."""
    try:
        result = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(result, datetime) or result.tzinfo is not None:
            raise ValueError("time zone must be omitted")
        return result
    except (ValueError, TypeError, OverflowError) as exc:
        raise ContractError("Timestamp must be ISO-8601 without a time zone (no Z or offset)") from exc


@dataclass(frozen=True)
class ReadingEvent:
    run_id: str
    customer_id: str
    interval_start: datetime
    reading_end: datetime
    power_kw: float
    source: str
    producer_sent_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("run_id", "customer_id", "source"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ContractError(f"{name} must be a nonempty string")
        start, end = parse_timestamp(self.interval_start), parse_timestamp(self.reading_end)
        if end - start != timedelta(minutes=15):
            raise ContractError("A reading must cover exactly 15 minutes")
        if start.minute % 15 or start.second or start.microsecond:
            raise ContractError("Reading intervals must align to the 15-minute grid")
        if isinstance(self.power_kw, bool):
            raise ContractError("power_kw must be a finite number >= 0")
        try:
            power = float(self.power_kw)
        except (TypeError, ValueError) as exc:
            raise ContractError("power_kw must be a finite number >= 0") from exc
        if not math.isfinite(power) or power < 0:
            raise ContractError("power_kw must be a finite number >= 0")
        object.__setattr__(self, "interval_start", start)
        object.__setattr__(self, "reading_end", end)
        object.__setattr__(self, "power_kw", power)
        if self.producer_sent_at is not None:
            object.__setattr__(self, "producer_sent_at", parse_timestamp(self.producer_sent_at))

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> ReadingEvent:
        required = ("run_id", "customer_id", "interval_start", "reading_end", "power_kw", "source")
        missing = [key for key in required if key not in row]
        if missing:
            raise ContractError("Missing event fields: " + ", ".join(missing))
        return cls(**{key: row[key] for key in required}, producer_sent_at=row.get("producer_sent_at") or None)

    @property
    def key(self) -> tuple[str, str, datetime]:
        return self.run_id, self.customer_id, self.reading_end

    @property
    def hour_start(self) -> datetime:
        # A reading ending at 09:00 still belongs to the 08:00--09:00 hour.
        return self.interval_start.replace(minute=0, second=0, microsecond=0)

    def as_dict(self) -> dict[str, Any]:
        result = dict(self.__dict__)
        for name in ("interval_start", "reading_end", "producer_sent_at"):
            if result[name] is not None:
                result[name] = result[name].isoformat()
        return result


def target_window(forecast_origin: str | datetime, *, delay_minutes: int = 60,
                  duration_minutes: int = 60) -> tuple[datetime, datetime]:
    origin = parse_timestamp(forecast_origin)
    if origin.minute or origin.second or origin.microsecond:
        raise ContractError("forecast_origin must align to a whole hour")
    if type(delay_minutes) is not int or type(duration_minutes) is not int or delay_minutes < 0 or duration_minutes <= 0:
        raise ContractError("Forecast delay/duration must be integer minutes with delay >= 0 and duration > 0")
    start = origin + timedelta(minutes=delay_minutes)
    return start, start + timedelta(minutes=duration_minutes)
