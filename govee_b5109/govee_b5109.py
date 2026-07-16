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
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
import requests

GOVEE_URL = "https://app2.govee.com/device/rest/devices/v1/list"
LOGIN_URL = "https://app2.govee.com/account/rest/account/v1/login"
OPTIONS_PATH = "/data/options.json"
TOPIC_ROOT = "govee_b5109"

DEFAULTS = {
    "bearer_token": "",
    "email": "",
    "password": "",
    "client_id": "",
    "device": "",
    "sku": "H5109",
    "app_version": "7.5.30",
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
    last_seen: Optional[str]
    raw_response: dict


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", text.strip()).strip("_").lower()
    return slug or "b5109"


class AuthError(Exception):
    pass


def _app_headers(opts: dict, token: Optional[str] = None) -> dict:
    headers = {
        "Host": "app2.govee.com",
        "Accept": "*/*",
        "timestamp": f"{time.time() * 1000:.6f}",
        "envId": "0",
        "clientId": opts["client_id"],
        "appVersion": opts["app_version"],
        "Accept-Language": "en",
        "Content-Type": "application/json",
        "sysVersion": "26.4",
        "clientType": "1",
        "User-Agent": f"GoveeHome/{opts['app_version']} (com.ihoment.GoVeeSensor; build:8; iOS 26.4.0) Alamofire/5.11.0",
        "timezone": opts["timezone"],
        "country": opts["country"],
        "iotVersion": "0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def govee_login(opts: dict) -> str:
    logging.info("Logging in to Govee for a new token")
    body = {
        "email": opts["email"],
        "password": opts["password"],
        "client": opts["client_id"],
    }
    resp = requests.post(
        LOGIN_URL, headers=_app_headers(opts), json=body, timeout=15
    )
    resp.raise_for_status()
    data = resp.json()
    # Success payload nests the session under "client" (some deployments
    # have used "data"); anything without a token is a failed login.
    blk = data.get("client") or data.get("data") or {}
    token = blk.get("token") if isinstance(blk, dict) else None
    if not token:
        raise AuthError(
            f"login failed: status={data.get('status')} message={data.get('message')!r}"
        )
    logging.info(
        "Govee login OK (tokenExpireCycle=%s)", blk.get("tokenExpireCycle")
    )
    return token


def fetch_device_list(opts: dict, token: str) -> dict:
    resp = requests.post(GOVEE_URL, headers=_app_headers(opts, token), timeout=15)
    if resp.status_code == 401:
        raise AuthError("HTTP 401 from device list")
    resp.raise_for_status()
    payload = resp.json()
    # Errors come back as HTTP 200 with the real status in the body
    # (401 = expired token, 400 = appVersion too low, ...).
    if isinstance(payload, dict):
        status = payload.get("status")
        if status == 401:
            raise AuthError(f"token rejected: {payload.get('message')!r}")
        if status is not None and status != 200:
            raise RuntimeError(
                f"Govee API status {status}: {payload.get('message')!r}"
            )
    return payload


def find_device(payload: dict, sku: str, device_id: str) -> Optional[dict]:
    """Return the device dict matching SKU (and optional device MAC) from /list."""
    sku_l = sku.strip().lower()
    dev_l = device_id.strip().lower() if device_id else ""
    matches = []
    for dev in payload.get("devices", []):
        if (dev.get("sku") or "").strip().lower() != sku_l:
            continue
        if dev_l and (dev.get("device") or "").strip().lower() != dev_l:
            continue
        matches.append(dev)
    if not matches:
        return None
    return matches[0]


def parse_reading(device: dict) -> Reading:
    ext = device.get("deviceExt") or {}
    ldd_raw = ext.get("lastDeviceData") or "{}"
    try:
        ldd = json.loads(ldd_raw) if isinstance(ldd_raw, str) else ldd_raw
    except json.JSONDecodeError:
        ldd = {}
    tem = ldd.get("tem")
    temp_c = float(tem) / 100.0 if isinstance(tem, (int, float)) else None
    last_time_ms = ldd.get("lastTime")
    last_seen: Optional[str] = None
    if isinstance(last_time_ms, (int, float)) and last_time_ms > 0:
        last_seen = datetime.fromtimestamp(
            last_time_ms / 1000.0, tz=timezone.utc
        ).isoformat()
    return Reading(temp_c=temp_c, last_seen=last_seen, raw_response=ldd)


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
        last_seen_cfg = {
            "name": "Last Seen",
            "unique_id": f"{TOPIC_ROOT}_{self.uid}_last_seen",
            "state_topic": self._state_topic(),
            "value_template": "{{ value_json.last_seen }}",
            "device_class": "timestamp",
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
            f"{self.discovery_prefix}/sensor/{TOPIC_ROOT}_{self.slug}/last_seen/config",
            json.dumps(last_seen_cfg),
            retain=True,
        )
        # Retained-empty publish to evict any stale battery discovery from
        # earlier versions of the addon.
        self.client.publish(
            f"{self.discovery_prefix}/sensor/{TOPIC_ROOT}_{self.slug}/battery/config",
            "",
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
            "temperature_c": round(reading.temp_c, 2) if reading.temp_c is not None else None,
            "last_seen": reading.last_seen,
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

    missing = [k for k in ("client_id", "sku") if not opts.get(k)]
    can_login = bool(opts.get("email") and opts.get("password"))
    if not opts.get("bearer_token") and not can_login:
        missing.append("bearer_token (or email + password)")
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
    token: Optional[str] = opts["bearer_token"] or None

    while _RUN:
        try:
            if token is None:
                token = govee_login(opts)
            payload = fetch_device_list(opts, token)
            device = find_device(payload, opts["sku"], opts["device"])
            if device is None:
                skus_seen = sorted(
                    {(d.get("sku") or "") for d in payload.get("devices", [])}
                )
                logging.warning(
                    "No device matched sku=%s device=%r; SKUs in account: %s",
                    opts["sku"],
                    opts["device"],
                    skus_seen,
                )
                consecutive_errors += 1
            else:
                reading = parse_reading(device)
                if not first_dump_logged:
                    logging.info(
                        "First poll matched device=%s lastDeviceData=%s",
                        device.get("device"),
                        json.dumps(reading.raw_response),
                    )
                    first_dump_logged = True
                if reading.temp_c is None:
                    logging.warning(
                        "No 'tem' in lastDeviceData; raw=%s",
                        json.dumps(reading.raw_response),
                    )
                publisher.publish_reading(reading)
                logging.debug("Published temp_c=%s", reading.temp_c)
                consecutive_errors = 0
        except AuthError as e:
            consecutive_errors += 1
            if can_login:
                logging.warning(
                    "Auth failed (%s); re-login in %ds",
                    e,
                    poll * min(consecutive_errors + 1, 5),
                )
                token = None
            else:
                logging.error(
                    "Auth failed (%s) and no email/password configured; "
                    "recapture bearer_token or set email + password",
                    e,
                )
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
