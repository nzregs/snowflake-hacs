DOMAIN = "snowflake_exporter"

CONF_PRIVATE_KEY = "private_key"
CONF_ACCOUNT_LOCATOR = "account_locator"
CONF_SYNC_MODE = "sync_mode"
CONF_SYNC_INTERVAL = "sync_interval"
CONF_ENTITIES = "entities"

DEFAULT_SYNC_INTERVAL = 5

SYNC_MODE_ALL = "all"
SYNC_MODE_SELECTED = "selected"

# State attributes never exported to Snowflake (they grant access to HA resources)
ATTRIBUTE_DENYLIST = frozenset({"access_token", "entity_picture"})

IDENTIFIER_FIELDS = ("database", "schema", "warehouse")
IDENTIFIER_PATTERN = r"^[A-Za-z_][A-Za-z0-9_$]*$"
