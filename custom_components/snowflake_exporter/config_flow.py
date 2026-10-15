"""Config flow for Snowflake Exporter."""

from __future__ import annotations

import logging
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import snowflake.connector
import voluptuous as vol

from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.helpers.selector import (
    EntitySelector,
    EntitySelectorConfig,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from .const import (
    CONF_ACCOUNT_LOCATOR,
    CONF_ENTITIES,
    CONF_PRIVATE_KEY,
    CONF_SYNC_INTERVAL,
    CONF_SYNC_MODE,
    DEFAULT_SYNC_INTERVAL,
    DOMAIN,
    SYNC_MODE_ALL,
    SYNC_MODE_SELECTED,
)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required("account"): str,
        vol.Required("user"): str,
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

_LOGGER = logging.getLogger(__name__)


class SnowflakeExporterConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    @staticmethod
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        return SnowflakeExporterOptionsFlow(config_entry)

    def __init__(self) -> None:
        self._user_input: dict[str, Any] = {}
        self._public_key_raw: str = ""

    def _generate_key_pair(self) -> None:
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048
        )
        private_pem = private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode()
        public_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        ).decode()
        self._public_key_raw = (
            public_pem
            .replace("-----BEGIN PUBLIC KEY-----", "")
            .replace("-----END PUBLIC KEY-----", "")
            .replace("\n", "")
        )
        self._user_input[CONF_PRIVATE_KEY] = private_pem

    async def _test_connection(self, data: dict[str, Any]) -> tuple[str | None, str | None]:
        """Test connection. Returns (error, account_locator)."""
        try:

            def _connect() -> str:
                from cryptography.hazmat.primitives.serialization import (
                    load_pem_private_key,
                )

                pk = load_pem_private_key(
                    data[CONF_PRIVATE_KEY].encode(), password=None
                )
                pk_bytes = pk.private_bytes(
                    serialization.Encoding.DER,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                )
                conn = snowflake.connector.connect(
                    account=data["account"],
                    user=data["user"],
                    private_key=pk_bytes,
                    database=data["database"],
                    schema=data["schema"],
                    warehouse=data["warehouse"],
                    login_timeout=10,
                )
                cur = conn.cursor()
                cur.execute("SELECT CURRENT_ACCOUNT()")
                locator = cur.fetchone()[0]
                cur.close()
                conn.close()
                return locator

            locator = await self.hass.async_add_executor_job(_connect)
            return None, locator
        except snowflake.connector.errors.DatabaseError as err:
            _LOGGER.debug("Snowflake connection failed: %s", err)
            msg = str(err)
            if "JWT token" in msg or "authentication" in msg.lower():
                return "invalid_auth", None
            return "cannot_connect", None
        except Exception:
            _LOGGER.exception("Unexpected error connecting to Snowflake")
            return "unknown", None

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if user_input is not None:
            await self.async_set_unique_id(
                f"{user_input['account']}_{user_input['database']}"
            )
            self._abort_if_unique_id_configured()
            self._user_input = user_input
            self._generate_key_pair()
            return await self.async_step_show_public_key()

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
        )

    async def async_step_show_public_key(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            error, locator = await self._test_connection(self._user_input)
            if error:
                errors["base"] = error
            else:
                self._user_input[CONF_ACCOUNT_LOCATOR] = locator
                return await self.async_step_sync_mode()

        alter_sql = (
            f"ALTER USER {self._user_input['user']} "
            f"SET RSA_PUBLIC_KEY='{self._public_key_raw}';"
        )
        return self.async_show_form(
            step_id="show_public_key",
            data_schema=vol.Schema({}),
            description_placeholders={
                "alter_sql": alter_sql,
                "public_key": self._public_key_raw,
            },
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


class SnowflakeExporterOptionsFlow(OptionsFlow):

    def __init__(self, config_entry: ConfigEntry) -> None:
        self._config_entry = config_entry
        self._updated_data: dict[str, Any] = {}

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            # Build updated data with changed connection params
            self._updated_data = {**self._config_entry.data, **user_input}
            # Test connection with updated params + existing private key
            error = await self._test_connection(self._updated_data)
            if error:
                errors["base"] = error
            else:
                if user_input.get(CONF_SYNC_MODE) == SYNC_MODE_SELECTED:
                    return await self.async_step_select_entities()
                # "All" mode — clear any previously selected entity list
                self._updated_data.pop(CONF_ENTITIES, None)
                self.hass.config_entries.async_update_entry(
                    self._config_entry,
                    data=self._updated_data,
                )
                return self.async_create_entry(data={})

        data = self._config_entry.data
        schema = vol.Schema(
            {
                vol.Required("user", default=data.get("user", "")): str,
                vol.Required("database", default=data.get("database", "")): str,
                vol.Required("schema", default=data.get("schema", "PUBLIC")): str,
                vol.Required("warehouse", default=data.get("warehouse", "")): str,
                vol.Required(
                    CONF_SYNC_INTERVAL,
                    default=data.get(CONF_SYNC_INTERVAL, DEFAULT_SYNC_INTERVAL),
                ): vol.All(int, vol.Range(min=1)),
                vol.Required(
                    CONF_SYNC_MODE,
                    default=data.get(CONF_SYNC_MODE, SYNC_MODE_ALL),
                ): SelectSelector(
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

        return self.async_show_form(
            step_id="init",
            data_schema=schema,
            errors=errors,
        )

    async def async_step_select_entities(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if user_input is not None:
            self._updated_data[CONF_ENTITIES] = user_input[CONF_ENTITIES]
            self.hass.config_entries.async_update_entry(
                self._config_entry,
                data=self._updated_data,
            )
            return self.async_create_entry(data={})

        current_entities = self._config_entry.data.get(CONF_ENTITIES, [])
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_ENTITIES, default=current_entities
                ): EntitySelector(EntitySelectorConfig(multiple=True)),
            }
        )
        return self.async_show_form(
            step_id="select_entities",
            data_schema=schema,
        )

    async def _test_connection(self, data: dict[str, Any]) -> str | None:
        try:

            def _connect() -> None:
                from cryptography.hazmat.primitives.serialization import (
                    load_pem_private_key,
                )

                pk = load_pem_private_key(
                    data[CONF_PRIVATE_KEY].encode(), password=None
                )
                pk_bytes = pk.private_bytes(
                    serialization.Encoding.DER,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                )
                conn = snowflake.connector.connect(
                    account=data["account"],
                    user=data["user"],
                    private_key=pk_bytes,
                    database=data["database"],
                    schema=data["schema"],
                    warehouse=data["warehouse"],
                    login_timeout=10,
                )
                conn.close()

            await self.hass.async_add_executor_job(_connect)
        except snowflake.connector.errors.DatabaseError as err:
            _LOGGER.debug("Snowflake connection failed: %s", err)
            msg = str(err)
            if "JWT token" in msg or "authentication" in msg.lower():
                return "invalid_auth"
            return "cannot_connect"
        except Exception:
            _LOGGER.exception("Unexpected error connecting to Snowflake")
            return "unknown"
        return None
