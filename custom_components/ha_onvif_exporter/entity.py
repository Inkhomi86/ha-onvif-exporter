"""Shared non-polling device state; no passwords in entity attributes."""
from homeassistant.helpers.entity import Entity
from .const import CONF_NAME, CONF_VIRTUAL_ID, DOMAIN, VERSION


class ExportEntity(Entity):
    _attr_should_poll = False
    _attr_has_entity_name = True

    def __init__(self, entry, key, name):
        self.manager = entry.runtime_data
        self._attr_unique_id = f"{entry.data[CONF_VIRTUAL_ID]}_{key}"
        self._attr_name = name
        self._attr_device_info = {
            "identifiers": {(DOMAIN, entry.data[CONF_VIRTUAL_ID])},
            "name": self.manager.settings[CONF_NAME],
            "manufacturer": "HA ONVIF Exporter",
            "model": "Virtual camera",
            "sw_version": VERSION,
        }

    async def async_added_to_hass(self):
        await super().async_added_to_hass()
        self.async_on_remove(self.manager.listen(self.async_write_ha_state))
