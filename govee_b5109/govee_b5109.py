#!/usr/bin/env python3
"""Bridge a single Govee B5109 sensor from the Govee mobile-app cloud to MQTT.

The B5109 reports through the same app2.govee.com device-list endpoint the
Govee iOS/Android app uses. Authentication is a bearer token + clientId
captured from a real app request (Proxyman or similar). This addon polls
that endpoint, locates the configured B5109 by device name, and republishes
its temperature - plus best-effort battery - to MQTT with Home Assistant
discovery.

Config source: /data/options.json (HA addon), else environment variables of
the same uppercase names for standalone use.
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import sys
import time
from dataclasses import dataclass
from typing import Optional

import paho.mqtt.client as mqtt
import requests

GOVEE_URL = "https://app2.govee.com/device/rest/devices/v1/list"
OPTIONS_PATH = "/data/options.json"
TOPIC_ROOT = "govee_b5109"

DEFAULTS = {
    "bearer_token": "",
    "client_id": "",
    "device_name": "",
    "poll_interval": 60,
    "unit": "F",
    "mqtt_host": "core-mosquitto",
    "mqtt_port": 1883,
    "mqtt_username": "",
    "mqtt_password": "",
    "mqtt_discovery_prefix": "homeassistant",
    "log_level": "info",
}


def load_options() -> dict:
    if os.path.exists(OPTIONS_PATH):
        with open(OPTIONS_PATH, "r", encoding="utf-8") as fh:
            opts = json.load(fh)
    else:
        opts = {}
        for key in DEFAULTS:
            env_val = os.environ.get(key.upper())
            if env_val is None:
                continue
            if key in ("poll_interval", "mqtt_port"):
                opts[key] = int(env_val)
            else:
                opts[key] = env_val
    return {**DEFAULTS, **opts}


@dataclass
class Reading:
    device_name: str
    device_id: str
    sku: str
    temp_c: Optional[float]
    battery: Optional[float]
    raw_last_device_data: dict


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", text.strip()).strip("_").lower()
    return slug or "b5109"


def fetch_devices(bearer: str, client_id: str) -> dict:
    timestamp = f"{time.time():.6f}"
    headers = {
        "Host": "app2.govee.com",
        "Authorization": f"Bearer {bearer}",
        "Accept": "*/*",
        "timestamp": timestamp,
        "envId": "0",
        "clientId": client_id,
        "appVersion": "6.4.11",
        "Accept-Language": "en",
        "clientType": "1",
        "User-Agent": "GoveeHome/6.4.11 (com.ihoment.GoVeeSensor; build:2; iOS 18.1.1) Alamofire/5.6.4",
        "iotVersion": "0",
        "Content-Type": "application/json",
    }
    resp = requests.post(GOVEE_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def find_b5109(payload: dict, device_name: str) -> Optional[Reading]:
    target = device_name.strip().lower()
    for dev in payload.get("devices", []):
        name = (dev.get("deviceName") or "").strip()
        if name.lower() != target:
            continue
        device_id = dev.get("device") or dev.get("deviceId") or name
        sku = dev.get("sku") or dev.get("goodsType") or "B5109"
        ext = dev.get("deviceExt") or {}
        ldd_raw = ext.get("lastDeviceData") or "{}"
        try:
            ldd = json.loads(ldd_raw) if isinstance(ldd_raw, str) else ldd_raw
        except json.JSONDecodeError:
            ldd = {}
        tem = ldd.get("tem")
        temp_c = float(tem) / 100.0 if isinstance(tem, (int, float)) else None
        battery: Optional[float] = None
        for key in ("battery", "batteryLevel", "bat", "power", "electricity"):
            v = ldd.get(key)
            if isinstance(v, (int, float)):
                battery = float(v)
                break
        return Reading(
            device_name=name,
            device_id=str(device_id),
            sku=str(sku),
            temp_c=temp_c,
            battery=battery,
            raw_last_device_data=ldd,
        )
    return None


class MqttPublisher:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        discovery_prefix: str,
        unit: str,
    ):
        self.discovery_prefix = discovery_prefix.rstrip("/")
        self.unit = unit
        self._discovery_sent = False
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"{TOPIC_ROOT}-{int(time.time())}",
        )
        if username:
            self.client.username_pw_set(username, password)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.will_set(f"{TOPIC_ROOT}/availability", "offline", retain=True)
        self.client.connect(host, port, keepalive=60)
        self.client.loop_start()

    def _on_connect(self, _client, _userdata, _flags, reason_code, _properties=None):
        logging.info("MQTT connected: %s", reason_code)

    def _on_disconnect(self, _client, _userdata, *_a, **_kw):
        logging.warning("MQTT disconnected")

    def _publish_discovery(self, reading: Reading, slug: str) -> None:
        if self._discovery_sent:
            return
        state_topic = f"{TOPIC_ROOT}/{slug}/state"
        avail_topic = f"{TOPIC_ROOT}/{slug}/availability"
        device_block = {
            "identifiers": [f"{TOPIC_ROOT}_{reading.device_id}"],
            "name": reading.device_name,
            "manufacturer": "Govee",
            "model": "B5109",
        }
        unit_symbol = "°F" if self.unit.upper() == "F" else "°C"
        temp_cfg = {
            "name": f"{reading.device_name} Temperature",
            "unique_id": f"{TOPIC_ROOT}_{reading.device_id}_temperature",
            "state_topic": state_topic,
            "value_template": "{{ value_json.temperature }}",
            "unit_of_measurement": unit_symbol,
            "device_class": "temperature",
            "state_class": "measurement",
            "availability_topic": avail_topic,
            "device": device_block,
        }
        battery_cfg = {
            "name": f"{reading.device_name} Battery",
            "unique_id": f"{TOPIC_ROOT}_{reading.device_id}_battery",
            "state_topic": state_topic,
            "value_template": "{{ value_json.battery }}",
            "unit_of_measurement": "%",
            "device_class": "battery",
            "state_class": "measurement",
            "entity_category": "diagnostic",
            "availability_topic": avail_topic,
            "device": device_block,
        }
        self.client.publish(
            f"{self.discovery_prefix}/sensor/{TOPIC_ROOT}_{slug}/temperature/config",
            json.dumps(temp_cfg),
            retain=True,
        )
        self.client.publish(
            f"{self.discovery_prefix}/sensor/{TOPIC_ROOT}_{slug}/battery/config",
            json.dumps(battery_cfg),
            retain=True,
        )
        self._discovery_sent = True
        logging.info("Published HA discovery for %s", slug)

    def publish_reading(self, reading: Reading) -> None:
        slug = slugify(reading.device_name or reading.device_id)
        self._publish_discovery(reading, slug)
        if reading.temp_c is None:
            temp_value = None
        elif self.unit.upper() == "F":
            temp_value = round((reading.temp_c * 9 / 5) + 32, 2)
        else:
            temp_value = round(reading.temp_c, 2)
        payload = {
            "temperature": temp_value,
            "battery": reading.battery,
            "temperature_c": round(reading.temp_c, 2) if reading.temp_c is not None else None,
        }
        self.client.publish(f"{TOPIC_ROOT}/{slug}/state", json.dumps(payload), retain=True)
        self.client.publish(f"{TOPIC_ROOT}/{slug}/availability", "online", retain=True)

    def stop(self) -> None:
        self.client.loop_stop()
        try:
            self.client.disconnect()
        except Exception:
            pass


_RUN = True


def _handle_sig(_signum, _frame):
    global _RUN
    _RUN = False


def main() -> int:
    opts = load_options()
    level = getattr(logging, opts["log_level"].upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")

    if not opts["bearer_token"] or not opts["client_id"]:
        logging.error("bearer_token and client_id are required")
        return 2
    if not opts["device_name"]:
        logging.error("device_name is required (must match the B5109 name in the Govee app)")
        return 2

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    publisher = MqttPublisher(
        host=opts["mqtt_host"],
        port=int(opts["mqtt_port"]),
        username=opts["mqtt_username"],
        password=opts["mqtt_password"],
        discovery_prefix=opts["mqtt_discovery_prefix"],
        unit=opts["unit"],
    )

    first_dump_logged = False
    poll = int(opts["poll_interval"])
    consecutive_errors = 0

    while _RUN:
        try:
            payload = fetch_devices(opts["bearer_token"], opts["client_id"])
            reading = find_b5109(payload, opts["device_name"])
            if reading is None:
                logging.warning(
                    "No device named %r in Govee device list; available: %s",
                    opts["device_name"],
                    [d.get("deviceName") for d in payload.get("devices", [])],
                )
            else:
                if not first_dump_logged:
                    logging.info(
                        "First poll: device=%s id=%s sku=%s lastDeviceData=%s",
                        reading.device_name,
                        reading.device_id,
                        reading.sku,
                        json.dumps(reading.raw_last_device_data),
                    )
                    first_dump_logged = True
                publisher.publish_reading(reading)
                logging.debug(
                    "Published temp_c=%s battery=%s", reading.temp_c, reading.battery
                )
            consecutive_errors = 0
        except requests.HTTPError as e:
            consecutive_errors += 1
            logging.error(
                "Govee HTTP error: %s body=%s",
                e,
                getattr(e.response, "text", ""),
            )
        except Exception as e:
            consecutive_errors += 1
            logging.exception("Poll error: %s", e)

        sleep_for = poll * min(consecutive_errors + 1, 5) if consecutive_errors else poll
        for _ in range(sleep_for):
            if not _RUN:
                break
            time.sleep(1)

    publisher.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
