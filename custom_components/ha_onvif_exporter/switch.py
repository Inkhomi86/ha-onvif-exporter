"""Enable activations without exposing camera-native controls."""
from homeassistant.components.switch import SwitchEntity
from homeassistant.helpers.entity import EntityCategory
from .entity import ExportEntity


async def async_setup_entry(hass, entry, async_add_entities):
    async_add_entities([ExportSwitch(entry, "enabled", "Export enabled")])


class ExportSwitch(ExportEntity, SwitchEntity):
    _attr_icon = "mdi:video-switch"
    _attr_entity_category = EntityCategory.CONFIG

    @property
    def is_on(self):
        return self.manager.enabled

    async def async_turn_on(self, **kwargs):
        await self.manager.async_set_enabled(True)

    async def async_turn_off(self, **kwargs):
        await self.manager.async_set_enabled(False)
