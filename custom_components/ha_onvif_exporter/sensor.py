"""Safe export status, never source URLs, SDP or credentials."""
from homeassistant.components.sensor import SensorEntity, SensorDeviceClass
from homeassistant.helpers.entity import EntityCategory
from .entity import ExportEntity


async def async_setup_entry(hass, entry, async_add_entities):
    async_add_entities([ExportStatus(entry, "status", "Export status")])


class ExportStatus(ExportEntity, SensorEntity):
    _attr_device_class = SensorDeviceClass.ENUM
    _attr_options = ["disabled", "standby", "starting", "live", "stopping", "fault"]
    _attr_icon = "mdi:video-outline"
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    @property
    def native_value(self):
        return self.manager.status

    @property
    def extra_state_attributes(self):
        return {"error_code": self.manager.error_code, "window_seconds": self.manager.window_seconds,
                "exported_frames": self.manager.engine.frames}
