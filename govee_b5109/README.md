# Govee B5109 MQTT Bridge

Home Assistant add-on that bridges a single **Govee H5109 / B5109** sensor
to MQTT.

The B5109 is not supported by the public Govee Developer API or by community
Govee integrations. This addon polls the private cloud endpoint the Govee
mobile app uses for live readings:

```
POST https://app2.govee.com/device/rest/devices/v1/list
```

using a bearer token + clientId captured from a real app session. The call
takes no body; it returns your full Govee account device list, and the addon
matches the one entry whose `sku` and `device` (MAC) match the add-on's
options, then republishes its temperature to MQTT with Home Assistant
discovery.

## Install (Local Add-on)

1. Copy `govee_b5109/` to `/addons/govee_b5109/` on your Home Assistant host.
2. Settings -> Add-ons -> Add-on Store -> menu -> **Check for updates**.
3. Local add-ons -> **Govee B5109 MQTT Bridge** -> Install.

## Capture credentials and device ID

1. Install Proxyman on iOS (or equivalent on Android).
2. SSL-proxy `app2.govee.com`, install the cert, then open the Govee app
   and view the H5109 sensor so the app fetches a reading.
3. In Proxyman find the `POST` request to:
   `https://app2.govee.com/device/rest/devices/v1/list`
4. From its request headers, copy:
   - `Authorization: Bearer <token>` -> `bearer_token`
   - `clientId: <id>` -> `client_id`
5. From its JSON response body, find the entry for your sensor and copy:
   - `device` (the MAC) -> `device`
     Example: `AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99`
   - `sku` -> `sku` (defaults to `H5109`)

The bearer token expires (~57 days). Set `email` + `password` and the addon
logs in by itself: at startup if `bearer_token` is empty, and automatically
whenever the current token starts getting rejected. With credentials set you
never need to recapture; `bearer_token` becomes optional (only `client_id`
still has to come from a capture, once).

## Configuration

```yaml
bearer_token: "eyJ..."             # optional if email+password are set
email: ""                          # Govee account, enables auto-login
password: ""
client_id: "abc123..."
device: "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99"
sku: "H5109"
app_version: "7.5.30"              # mirrors the Govee app version sent in request headers
friendly_name: "Govee B5109"      # shown in HA as the device name
poll_interval: 60                  # seconds, 10-3600
unit: "F"                          # C or F
timezone: "UTC"                    # mirrored to the timezone header
country: "US"                      # mirrored to the country header
mqtt_host: "core-mosquitto"
mqtt_port: 1883
mqtt_username: ""
mqtt_password: ""
mqtt_discovery_prefix: "homeassistant"
log_level: "info"
```

## MQTT topics

Slug = `friendly_name` lower-cased, non-alphanumeric -> `_`.

| Topic | Purpose |
|---|---|
| `homeassistant/sensor/govee_b5109_<slug>/temperature/config` | HA discovery (retained) |
| `homeassistant/sensor/govee_b5109_<slug>/last_seen/config` | HA discovery (retained), diagnostic timestamp entity |
| `govee_b5109/<slug>/state` | JSON state (retained) |
| `govee_b5109/<slug>/availability` | `online` / `offline` (LWT) |

There is no battery entity: the addon publishes a retained-empty message to
the old `.../battery/config` discovery topic on startup, which removes any
battery entity created by earlier versions.

State payload:

```json
{"temperature": 78.4, "temperature_c": 25.8, "last_seen": "2026-09-27T12:00:00+00:00"}
```

`temperature` honours the `unit` setting. `temperature_c` is always Celsius
so historical graphs stay continuous if you flip units. `last_seen` is the
device's own last-reading timestamp (UTC, ISO 8601), not the poll time.

## Notes

- The parser reads `deviceExt.lastDeviceData` (a JSON string embedded in the
  device list entry) for two fields: `tem` (centi-Celsius, divided by 100) and
  `lastTime` (milliseconds since epoch, converted to UTC).
- The bridge logs the full `lastDeviceData` on its first successful poll
  (look for `First poll matched device=...` in the add-on log), and logs a
  warning if `tem` is missing from it.
- Errors from the Govee API come back as HTTP 200 with the real status in
  the response body: `401` means the bearer token was rejected (triggers
  auto re-login if `email` + `password` are set), `400` usually means
  `app_version` is too low for the endpoint.
- F = C * 9/5 + 32.

## Standalone (no HA addon)

```
pip install -r requirements.txt
BEARER_TOKEN=... CLIENT_ID=... \
  DEVICE="AA:BB:CC:DD:EE:FF:...:99" SKU=H5109 \
  MQTT_HOST=... MQTT_USERNAME=... MQTT_PASSWORD=... \
  python3 govee_b5109.py
```
