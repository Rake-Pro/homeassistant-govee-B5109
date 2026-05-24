# Govee B5109 MQTT Bridge

Home Assistant add-on that bridges a single **Govee B5109** sensor to MQTT.

Unlike most Govee devices, the B5109 does not work with the public Govee
Developer API and is not supported by community Govee integrations. This
addon polls the same private cloud endpoint the Govee mobile app uses
(`app2.govee.com/device/rest/devices/v1/list`) using a bearer token and
clientId captured from a real app session, parses the B5109's temperature
from `deviceExt.lastDeviceData.tem`, and republishes to MQTT with Home
Assistant discovery.

Approach borrowed from [clong/govee_h5042_sensor](https://github.com/clong/govee_h5042_sensor),
but scoped to the B5109 and delivered as an MQTT bridge instead of REST sensors.

## Install (Local Add-on)

1. Copy `govee_b5109/` to `/addons/govee_b5109/` on your Home Assistant host.
2. Settings -> Add-ons -> Add-on Store -> menu -> **Check for updates**.
3. "Local add-ons" -> **Govee B5109 MQTT Bridge** -> Install.

## Capture credentials

1. Install Proxyman on iOS (or equivalent on Android).
2. SSL-proxy `app2.govee.com`, install the cert, run the Govee app.
3. Find a `POST https://app2.govee.com/device/rest/devices/v1/list` request.
4. From its headers, copy:
   - `Authorization: Bearer <token>` -> `bearer_token`
   - `clientId: <id>` -> `client_id`

Tokens expire. When polling starts returning 401, recapture and update.

## Configuration

```yaml
bearer_token: "eyJ..."
client_id: "abc123..."
device_name: "Pool"             # exact name of the B5109 in the Govee app
poll_interval: 60               # seconds, 10-3600
unit: "F"                       # C or F (publishes temperature in this unit)
mqtt_host: "core-mosquitto"
mqtt_port: 1883
mqtt_username: ""
mqtt_password: ""
mqtt_discovery_prefix: "homeassistant"
log_level: "info"
```

## MQTT topics

Slug = `device_name` lower-cased, non-alphanumeric -> `_`.

| Topic | Purpose |
|---|---|
| `homeassistant/sensor/govee_b5109_<slug>/temperature/config` | HA discovery (retained) |
| `homeassistant/sensor/govee_b5109_<slug>/battery/config` | HA discovery (retained) |
| `govee_b5109/<slug>/state` | JSON state (retained) |
| `govee_b5109/<slug>/availability` | `online` / `offline` |

State payload:

```json
{"temperature": 78.4, "battery": 87, "temperature_c": 25.8}
```

`temperature` honours the `unit` setting. `temperature_c` is always Celsius
for graphing across unit changes. `battery` is best-effort; see Notes.

## Notes

- The reference shell script only parses temperature, so the exact battery
  field name in `lastDeviceData` is unconfirmed. The bridge tries
  `battery`, `batteryLevel`, `bat`, `power`, `electricity` and logs the full
  `lastDeviceData` JSON on the first successful poll. If the battery sensor
  stays `null`, check the log for the real key.
- `tem` is centi-Celsius (`2580` = 25.80 degC). F = C * 9/5 + 32.

## Standalone (no HA addon)

```
pip install -r requirements.txt
BEARER_TOKEN=... CLIENT_ID=... DEVICE_NAME="Pool" \
  MQTT_HOST=... MQTT_USERNAME=... MQTT_PASSWORD=... \
  python3 govee_b5109.py
```
