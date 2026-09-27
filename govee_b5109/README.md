# Govee B5109 MQTT Bridge

Home Assistant add-on that bridges a single **Govee H5109 / B5109** sensor
to MQTT.

The B5109 is not supported by the public Govee Developer API or by community
Govee integrations. This addon polls the private cloud endpoint the Govee
mobile app uses for live readings:

```
GET https://app2.govee.com/th/rest/devices/v1/multi-datas
        ?currentTime=<ms>&device=<MAC>&sku=H5109
```

using a bearer token + clientId captured from a real app session, and
republishes temperature to MQTT with Home Assistant discovery (plus
best-effort battery — see Notes).

## Install (Local Add-on)

1. Copy `govee_b5109/` to `/addons/govee_b5109/` on your Home Assistant host.
2. Settings -> Add-ons -> Add-on Store -> menu -> **Check for updates**.
3. Local add-ons -> **Govee B5109 MQTT Bridge** -> Install.

## Capture credentials and device ID

1. Install Proxyman on iOS (or equivalent on Android).
2. SSL-proxy `app2.govee.com`, install the cert, then open the Govee app
   and view the H5109 sensor so the app fetches a reading.
3. In Proxyman find a request to:
   `GET https://app2.govee.com/th/rest/devices/v1/multi-datas?...`
4. From its headers and URL, copy:
   - `Authorization: Bearer <token>` -> `bearer_token`
   - `clientId: <id>` -> `client_id`
   - URL param `device=<MAC>` (URL-decoded - Proxyman shows it decoded) -> `device`
     Example: `AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99`
   - URL param `sku=<SKU>` -> `sku` (defaults to `H5109`)

The bearer token expires (~57 days). Set `email` + `password` and the addon
logs in by itself — at startup if `bearer_token` is empty, and automatically
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
| `homeassistant/sensor/govee_b5109_<slug>/battery/config` | HA discovery (retained) |
| `govee_b5109/<slug>/state` | JSON state (retained) |
| `govee_b5109/<slug>/availability` | `online` / `offline` (LWT) |

State payload:

```json
{"temperature": 78.4, "battery": 87, "temperature_c": 25.8}
```

`temperature` honours the `unit` setting. `temperature_c` is always Celsius
so historical graphs stay continuous if you flip units.

## Notes

- The response shape of `/multi-datas` is not yet hard-coded in the parser.
  The bridge logs the full response on its first successful poll (look for
  `First poll response` in the add-on log). Paste that back so the parser
  can be tightened to the exact JSON path.
- Until then the parser walks the response and grabs the first numeric key
  named `tem` / `temperature` / `temCur` (assumed centi-Celsius, divided by
  100), and for battery: `battery` / `batteryLevel` / `bat` / `power` /
  `electricity`.
- F = C * 9/5 + 32.

## Standalone (no HA addon)

```
pip install -r requirements.txt
BEARER_TOKEN=... CLIENT_ID=... \
  DEVICE="AA:BB:CC:DD:EE:FF:...:99" SKU=H5109 \
  MQTT_HOST=... MQTT_USERNAME=... MQTT_PASSWORD=... \
  python3 govee_b5109.py
```
