#!/usr/bin/env python3
"""Bridge a single Govee H5109 / B5109 sensor to MQTT.

Polls the Govee mobile-app endpoint
    GET https://app2.govee.com/th/rest/devices/v1/multi-datas
            ?currentTime=<ms>&device=<MAC>&sku=<SKU>

using a bearer token and clientId captured from a real Govee app request.
Publishes temperature (and best-effort battery) to MQTT with Home Assistant
discovery.

Config source: /data/options.json (HA addon) or environment variables of the
same uppercase names for standalone use.
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
from typing import Any, Optional

import paho.mqtt.client as mqtt
import requests

GOVEE_URL = "https://app2.govee.com/th/rest/devices/v1/multi-datas"
OPTIONS_PATH = "/data/options.json"
TOPIC_ROOT = "govee_b5109"

DEFAULTS = {
    "bearer_token": "",
    "client_id": "",
    "device": "",
    "sku": "H5109",
    "friendly_name": "Govee B5109",
    "poll_interval": 60,
    "unit": "F",
    "timezone": "America/Los_Angeles",
    "country": "US",
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
    temp_c: Optional[float]
    battery: Optional[float]
    raw_response: dict


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", text.strip()).strip("_").lower()
    return slug or "b5109"


def fetch_device(opts: dict) -> dict:
    now_ms = int(time.time() * 1000)
    timestamp_header = f"{time.time() * 1000:.6f}"
    headers = {
        "Host": "app2.govee.com",
        "Authorization": f"Bearer {opts['bearer_token']}",
        "Accept": "*/*",
        "timestamp": timestamp_header,
        "envId": "0",
        "clientId": opts["client_id"],
        "appVersion": "7.4.40",
        "Accept-Language": "en",
        "Content-Type": "application/json",
        "sysVersion": "26.4",
        "clientType": "1",
        "User-Agent": "GoveeHome/7.4.40 (com.ihoment.GoVeeSensor; build:8; iOS 26.4.0) Alamofire/5.11.0",
        "timezone": opts["timezone"],
        "country": opts["country"],
        "iotVersion": "0",
    }
    params = {
        "currentTime": str(now_ms),
        "device": opts["device"],
        "sku": opts["sku"],
    }
    resp = requests.get(GOVEE_URL, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _walk_for_key(obj: Any, candidates: tuple[str, ...]) -> Optional[float]:
    """Depth-first search for a numeric value under any key in candidates.

    The /multi-datas response shape is not yet documented in this project;
    walking the tree avoids hard-coding a path that might be wrong. The
    first numeric hit wins.
    """
    if isinstance(obj, dict):
        for key in candidates:
            if key in obj:
                v = obj[key]
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    return float(v)
                if isinstance(v, str):
                    try:
                        return float(v)
                    except ValueError:
                        pass
        for v in obj.values():
            found = _walk_for_key(v, candidates)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _walk_for_key(item, candidates)
            if found is not None:
                return found
    return None


def parse_reading(payload: dict) -> Reading:
    # /multi-datas response shape unconfirmed; we search the tree for the
    # temperature key (centi-celsius in every Govee endpoint observed so
    # far) and a battery-like key. First-poll log dumps the full payload
    # so the parser can be tightened.
    raw_tem = _walk_for_key(payload, ("tem", "temperature", "temCur"))
    temp_c = raw_tem / 100.0 if raw_tem is not None else None
    battery = _walk_for_key(
        payload, ("battery", "batteryLevel", "bat", "power", "electricity")
    )
    return Reading(temp_c=temp_c, battery=battery, raw_response=payload)


class MqttPublisher:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        discovery_prefix: str,
        unit: str,
        friendly_name: str,
        device_id_hash: str,
    ):
        self.discovery_prefix = discovery_prefix.rstrip("/")
        self.unit = unit
        self.friendly_name = friendly_name
        self.slug = slugify(friendly_name)
        self.uid = device_id_hash
        self._discovery_sent = False
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"{TOPIC_ROOT}-{int(time.time())}",
        )
        if username:
            self.client.username_pw_set(username, password)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.will_set(
            f"{TOPIC_ROOT}/{self.slug}/availability", "offline", retain=True
        )
        self.client.connect(host, port, keepalive=60)
        self.client.loop_start()

    def _on_connect(self, _c, _u, _f, reason_code, _p=None):
        logging.info("MQTT connected: %s", reason_code)

    def _on_disconnect(self, _c, _u, *_a, **_kw):
        logging.warning("MQTT disconnected")

    def _state_topic(self) -> str:
        return f"{TOPIC_ROOT}/{self.slug}/state"

    def _avail_topic(self) -> str:
        return f"{TOPIC_ROOT}/{self.slug}/availability"

    def _publish_discovery(self) -> None:
        if self._discovery_sent:
            return
        device_block = {
            "identifiers": [f"{TOPIC_ROOT}_{self.uid}"],
            "name": self.friendly_name,
            "manufacturer": "Govee",
            "model": "H5109 / B5109",
        }
        unit_symbol = "°F" if self.unit.upper() == "F" else "°C"
        temp_cfg = {
            "name": "Temperature",
            "unique_id": f"{TOPIC_ROOT}_{self.uid}_temperature",
            "state_topic": self._state_topic(),
            "value_template": "{{ value_json.temperature }}",
            "unit_of_measurement": unit_symbol,
            "device_class": "temperature",
            "state_class": "measurement",
            "availability_topic": self._avail_topic(),
            "device": device_block,
        }
        battery_cfg = {
            "name": "Battery",
            "unique_id": f"{TOPIC_ROOT}_{self.uid}_battery",
            "state_topic": self._state_topic(),
            "value_template": "{{ value_json.battery }}",
            "unit_of_measurement": "%",
            "device_class": "battery",
            "state_class": "measurement",
            "entity_category": "diagnostic",
            "availability_topic": self._avail_topic(),
            "device": device_block,
        }
        self.client.publish(
            f"{self.discovery_prefix}/sensor/{TOPIC_ROOT}_{self.slug}/temperature/config",
            json.dumps(temp_cfg),
            retain=True,
        )
        self.client.publish(
            f"{self.discovery_prefix}/sensor/{TOPIC_ROOT}_{self.slug}/battery/config",
            json.dumps(battery_cfg),
            retain=True,
        )
        self._discovery_sent = True
        logging.info("Published HA discovery for %s", self.slug)

    def publish_reading(self, reading: Reading) -> None:
        self._publish_discovery()
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
        self.client.publish(self._state_topic(), json.dumps(payload), retain=True)
        self.client.publish(self._avail_topic(), "online", retain=True)

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

    missing = [k for k in ("bearer_token", "client_id", "device", "sku") if not opts.get(k)]
    if missing:
        logging.error("missing required options: %s", ", ".join(missing))
        return 2

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    uid = slugify(opts["device"]) or slugify(opts["sku"])
    publisher = MqttPublisher(
        host=opts["mqtt_host"],
        port=int(opts["mqtt_port"]),
        username=opts["mqtt_username"],
        password=opts["mqtt_password"],
        discovery_prefix=opts["mqtt_discovery_prefix"],
        unit=opts["unit"],
        friendly_name=opts["friendly_name"],
        device_id_hash=uid,
    )

    first_dump_logged = False
    poll = int(opts["poll_interval"])
    consecutive_errors = 0

    while _RUN:
        try:
            payload = fetch_device(opts)
            reading = parse_reading(payload)
            if not first_dump_logged:
                logging.info(
                    "First poll response (paste this back to tighten the parser): %s",
                    json.dumps(reading.raw_response),
                )
                first_dump_logged = True
            if reading.temp_c is None:
                logging.warning(
                    "No temperature key found in response; raw=%s",
                    json.dumps(reading.raw_response),
                )
            publisher.publish_reading(reading)
            logging.debug(
                "Published temp_c=%s battery=%s", reading.temp_c, reading.battery
            )
            consecutive_errors = 0
        except requests.HTTPError as e:
            consecutive_errors += 1
            logging.error(
                "Govee HTTP error: %s body=%s", e, getattr(e.response, "text", "")
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
