"""Configuration contract for HA ONVIF Exporter."""

DOMAIN = "ha_onvif_exporter"
VERSION = "0.1.1"

CONF_CAMERA_ENTITY = "camera_entity"
CONF_NAME = "name"
CONF_WINDOW_SECONDS = "window_seconds"
CONF_BIND_IP = "bind_ip"
CONF_ONVIF_PORT = "onvif_port"
CONF_RTSP_PORT = "rtsp_port"
CONF_USERNAME = "username"
CONF_PASSWORD = "password"
CONF_ALLOWED_CLIENTS = "allowed_clients"
CONF_TRIGGER_MODE = "trigger_mode"
CONF_EVENT_TYPE = "event_type"
CONF_EVENT_FILTER = "event_filter"
CONF_EVENT_AT_FIELD = "event_at_field"
CONF_BINARY_SENSOR = "binary_sensor"
CONF_VIRTUAL_ID = "virtual_id"

TRIGGER_EVENTS = "events"
TRIGGER_BINARY_SENSOR = "binary_sensor"
TRIGGER_MANUAL = "manual"
TRIGGER_MODES = (TRIGGER_EVENTS, TRIGGER_BINARY_SENSOR, TRIGGER_MANUAL)

DEFAULT_NAME = "HA ONVIF Camera"
DEFAULT_WINDOW_SECONDS = 30
# Safe fallback only; the user flow suggests HA's detected local IPv4 address.
DEFAULT_BIND_IP = "127.0.0.1"
# Staging avoids the existing exporter endpoints. Production ports are optional.
DEFAULT_ONVIF_PORT = 1986
DEFAULT_RTSP_PORT = 18557
PRODUCTION_ONVIF_PORT = 1985
PRODUCTION_RTSP_PORT = 18556
DEFAULT_USERNAME = "onvif_exporter"
# No personal entry IDs or credentials are embedded in the distributable code.
DEFAULT_ALLOWED_CLIENTS = ()
DEFAULT_TRIGGER_MODE = TRIGGER_MANUAL
DEFAULT_EVENT_TYPE = "bticino_c300x_agent_event_received"
DEFAULT_EVENT_AT_FIELD = "event_at"

MIN_WINDOW_SECONDS = 1
MAX_WINDOW_SECONDS = 300
MIN_PORT = 1024
MAX_PORT = 65535
MAX_ALLOWED_CLIENTS = 32
MAX_EVENT_FILTER_FIELDS = 16
MAX_EVENT_FILTER_VALUES = 16
MAX_EVENT_FILTER_BYTES = 4096
EVENT_MAX_AGE_SECONDS = 10
ONVIF_PROFILE_TOKEN = "standby"

# Mutable settings may reside in entry.options. Identity only resides in data.
CONFIG_FIELDS = frozenset(
    {
        CONF_CAMERA_ENTITY,
        CONF_NAME,
        CONF_WINDOW_SECONDS,
        CONF_BIND_IP,
        CONF_ONVIF_PORT,
        CONF_RTSP_PORT,
        CONF_USERNAME,
        CONF_PASSWORD,
        CONF_ALLOWED_CLIENTS,
        CONF_TRIGGER_MODE,
        CONF_EVENT_TYPE,
        CONF_EVENT_FILTER,
        CONF_EVENT_AT_FIELD,
        CONF_BINARY_SENSOR,
    }
)
