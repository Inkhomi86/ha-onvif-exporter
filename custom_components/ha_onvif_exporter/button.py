"""Export controls; these buttons do not unlock any door."""
from homeassistant.components.button import ButtonEntity
from .entity import ExportEntity


async def async_setup_entry(hass, entry, async_add_entities):
    async_add_entities([ExportButton(entry, "activate", "Activate video"), ExportButton(entry, "deactivate", "Stop export")])


class ExportButton(ExportEntity, ButtonEntity):
    def __init__(self, entry, key, name):
        super().__init__(entry, key, name)
        self.key = key
        self._attr_icon = "mdi:video" if key == "activate" else "mdi:stop-circle-outline"

    async def async_press(self):
        if self.key == "activate":
            from homeassistant.exceptions import HomeAssistantError
            if not await self.manager.async_activate():
                raise HomeAssistantError("Exporter is disabled or unavailable")
        else:
            await self.manager.async_deactivate()
