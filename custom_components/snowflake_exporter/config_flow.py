"""Config flow for Snowflake Exporter."""

from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.helpers.selector import (
    EntitySelector,
    EntitySelectorConfig,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from .const import (
    CONF_ENTITIES,
    CONF_SYNC_MODE,
    DOMAIN,
    SYNC_MODE_ALL,
    SYNC_MODE_SELECTED,
)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required("account"): str,
        vol.Required("user"): str,
        vol.Required("password"): str,
        vol.Required("database"): str,
        vol.Required("schema", default="PUBLIC"): str,
        vol.Required("warehouse"): str,
    }
)

STEP_SYNC_MODE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_SYNC_MODE, default=SYNC_MODE_ALL): SelectSelector(
            SelectSelectorConfig(
                options=[
                    {"value": SYNC_MODE_ALL, "label": "All entities"},
                    {"value": SYNC_MODE_SELECTED, "label": "Select specific entities"},
                ],
                mode=SelectSelectorMode.DROPDOWN,
            )
        ),
    }
)

STEP_SELECT_ENTITIES_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_ENTITIES): EntitySelector(
            EntitySelectorConfig(multiple=True)
        ),
    }
)


class SnowflakeExporterConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    def __init__(self) -> None:
        self._user_input: dict[str, Any] = {}

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            await self.async_set_unique_id(
                f"{user_input['account']}_{user_input['database']}"
            )
            self._abort_if_unique_id_configured()
            self._user_input = user_input
            return await self.async_step_sync_mode()

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )

    async def async_step_sync_mode(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if user_input is not None:
            self._user_input[CONF_SYNC_MODE] = user_input[CONF_SYNC_MODE]
            if user_input[CONF_SYNC_MODE] == SYNC_MODE_SELECTED:
                return await self.async_step_select_entities()
            return self.async_create_entry(
                title=f"Snowflake ({self._user_input['account']})",
                data=self._user_input,
            )

        return self.async_show_form(
            step_id="sync_mode",
            data_schema=STEP_SYNC_MODE_SCHEMA,
        )

    async def async_step_select_entities(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if user_input is not None:
            self._user_input[CONF_ENTITIES] = user_input[CONF_ENTITIES]
            return self.async_create_entry(
                title=f"Snowflake ({self._user_input['account']})",
                data=self._user_input,
            )

        return self.async_show_form(
            step_id="select_entities",
            data_schema=STEP_SELECT_ENTITIES_SCHEMA,
        )
