"""Pure bounded event admission. Video reads are never activation events."""
from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timezone
import hashlib
import json
import math
import time


def timestamp(value):
    if type(value) in (int, float):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, str) and len(value) <= 64:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.timestamp() if parsed.tzinfo is not None else None
        except (ValueError, OverflowError, OSError):
            pass
    return None


class EventAdmission:
    """Flat exact AND/OR matching, fresh timestamps, bounded replay prevention."""

    def __init__(self, event_filter, at_field, *, clock=time.time, max_age=10):
        self._filter = event_filter
        self._at_field = at_field
        self._clock = clock
        self._max_age = max_age
        self._started = clock()
        self._seen = OrderedDict()
        self.last_age = 0.0

    def accept(self, data):
        if not isinstance(data, dict) or len(data) > 64:
            return False
        for key, values in self._filter.items():
            if key not in data:
                return False
            members = values if isinstance(values, list) else [values]
            # Bool/int/string are distinct. Missing keys must not match None.
            if not any(type(data[key]) is type(v) and data[key] == v for v in members):
                return False
        occurred = timestamp(data.get(self._at_field))
        now = self._clock()
        if occurred is None or occurred < self._started or not 0 <= now - occurred < self._max_age:
            return False
        try:
            encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        except (ValueError, TypeError, UnicodeError):
            return False
        if len(encoded) > 8192:
            return False
        key = hashlib.sha256(encoded).digest()
        while self._seen and next(iter(self._seen.values())) < now - self._max_age:
            self._seen.popitem(last=False)
        if key in self._seen:
            return False
        if len(self._seen) >= 512:
            # Saturation rejects rather than making recent replays valid again.
            return False
        self._seen[key] = now
        self.last_age = now - occurred
        return True
