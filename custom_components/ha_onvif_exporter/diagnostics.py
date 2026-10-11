"""Allowlisted runtime counts for Home Assistant's admin diagnostics endpoint.

Never include config entry data/options, settings, URLs, client addresses, virtual
IDs, subscription tokens, XML, media payloads, error strings or credentials.
HA adds its own framework metadata outside this integration's `data` result.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

_STATUSES = {"disabled", "standby", "starting", "live", "stopping", "fault"}
_MAX_COUNT = (1 << 53) - 1


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, config_entry: ConfigEntry
) -> dict[str, Any]:
    """Read only the loaded manager, on HA's event loop; no I/O or activation."""
    manager = getattr(config_entry, "runtime_data", None)
    if manager is None:
        return {"loaded": False}
    # No entry.as_dict(), data/options, hass.data, registries or filesystem reads.
    status = manager.status if manager.status in _STATUSES else "unavailable"
    frames = manager.engine.frames
    frames = frames if type(frames) is int and 0 <= frames <= _MAX_COUNT else 0
    return {
        "loaded": True,
        "manager": {
            "status": status,
            "is_motion": manager.motion is True,
            "exported_frames": frames,
        },
        "onvif": manager.onvif.diagnostics_snapshot(),
    }
