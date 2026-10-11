"""Motion means confirmed real exported video, not ring/Unlock anticipation."""
from homeassistant.components.binary_sensor import BinarySensorEntity, BinarySensorDeviceClass
from .entity import ExportEntity


async def async_setup_entry(hass, entry, async_add_entities):
    async_add_entities([ExportMotion(entry, "motion", "Video window")])


class ExportMotion(ExportEntity, BinarySensorEntity):
    _attr_device_class = BinarySensorDeviceClass.MOTION

    @property
    def is_on(self):
        return self.manager.motion
