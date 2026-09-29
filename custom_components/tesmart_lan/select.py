"""
TESMart 8x8 HDMI matrix — per-output routing as `select` entities.

One select entity is created per configured output. Its options are the
configured inputs; choosing an option sends the ASCII matrix command
`MT00SW XXYY NT` (input XX -> output YY) over TCP, per the official
"8X8 HDMI matrix communication protocol".

Optional periodic polling sends `MT00RD0000NT` and parses the response
format `LINK:O<out>I<in>;END...;END` so the states stay truthful when
routing is changed from the physical panel or another controller.

Example configuration.yaml:

select:
  - platform: tesmart_lan
    matrixes:
      hdmi_matrix:
        friendly_name: HDMI Matrix
        host: !secret hdmi_matrix_host
        port: 5000
        poll_interval: 30          # seconds, 0 disables polling
        inputs:
          1: Xbox One
          2: PS5
          3: Mini PC
          4: Camera Feed
        outputs:
          1: Beamer Leinwand
          2: TV Wohnzimmer
          3: Monitor Schreibtisch
          4: TV Kueche
          5: HDMI 5
          6: HDMI 6
          7: HDMI 7
          8: HDMI 8
"""

import asyncio
import logging

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.select import (
    PLATFORM_SCHEMA,
    SelectEntity,
)
from homeassistant.const import ATTR_FRIENDLY_NAME
from homeassistant.core import callback
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.reload import async_setup_reload_service

from .const import DOMAIN, PLATFORMS

_LOGGER = logging.getLogger(__name__)

CONF_MATRIXES = "matrixes"
CONF_HOST = "host"
CONF_PORT = "port"
CONF_INPUTS = "inputs"
CONF_OUTPUTS = "outputs"
CONF_POLL_INTERVAL = "poll_interval"

MAX_PORTS = 16  # supports 4x4 up to 16x16 matrices

MATRIX_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST): cv.string,
        vol.Optional(CONF_PORT, default=5000): cv.positive_int,
        vol.Optional(ATTR_FRIENDLY_NAME): cv.string,
        vol.Required(CONF_INPUTS): vol.Any(
            vol.Schema(  # 1: Xbox One
                {vol.All(vol.Coerce(int), vol.Range(min=1, max=MAX_PORTS)): cv.string}
            ),
            vol.All(cv.ensure_list, [cv.string]),  # "1: Xbox One"
        ),
        vol.Required(CONF_OUTPUTS): vol.Any(
            vol.Schema(
                {vol.All(vol.Coerce(int), vol.Range(min=1, max=MAX_PORTS)): cv.string}
            ),
            vol.All(cv.ensure_list, [cv.string]),
        ),
        vol.Optional(CONF_POLL_INTERVAL, default=30): vol.All(
            vol.Coerce(int), vol.Range(min=0, max=3600)
        ),
    }
)

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {vol.Required(CONF_MATRIXES): cv.schema_with_slug_keys(MATRIX_SCHEMA)}
)

PROTOCOL_FRAME_START = b"MT00SW"
PROTOCOL_FRAME_END = b"NT"


def _port_str(value):
    """Validate a port number given as int or '01'-style string; return 2-digit str."""
    value = str(value).strip()
    if value.isdigit() and 1 <= int(value) <= MAX_PORTS:
        return f"{int(value):02d}"
    raise vol.Invalid(f"Port must be a number 1-{MAX_PORTS}, got {value!r}")


def _parse_ports(config, key, what):
    """Turn a dict {port: name} or a list of "port: name" strings into {port_int: name}."""
    mapping = {}
    raw = config[key]
    if isinstance(raw, dict):
        for port, name in raw.items():
            mapping[int(_port_str(port))] = str(name).strip()
        return mapping
    for entry in raw:
        entry = str(entry).strip()
        if ":" in entry:
            port, name = entry.split(":", 1)
            mapping[int(_port_str(port))] = name.strip()
        else:
            mapping[int(_port_str(entry))] = f"{what} {int(_port_str(entry))}"
    return mapping


class TesmartMatrixClient:
    """Shared TCP client + routing state for one matrix host."""

    def __init__(self, hass, host, port):
        self.hass = hass
        self.host = host
        self.port = port
        # {output_int: input_int}
        self.routing = {}
        self.listeners = []
        self._unsub_poll = None
        self._lock = asyncio.Lock()

    async def _send_receive(self, payload, expect_len, timeout=5):
        """Connect, send bytes, read response; return bytes or None.

        expect_len > 0: read exactly that many bytes.
        expect_len == 0: no read (fire-and-forget commands).
        expect_len is None: read whatever the device sends (up to 4096 bytes).
        """
        reader = writer = None
        try:
            async with self._lock:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port), timeout=timeout
                )
                if payload is not None:
                    writer.write(payload)
                    await writer.drain()
                if expect_len == 0:
                    data = b""
                elif expect_len is None:
                    data = await asyncio.wait_for(
                        reader.read(4096), timeout=timeout
                    )
                else:
                    data = await asyncio.wait_for(
                        reader.readexactly(expect_len), timeout=timeout
                    )
                return data
        except Exception:  # noqa: BLE001 — matrix may be powered off / offline
            _LOGGER.exception(
                "Communication with %s:%s failed", self.host, self.port
            )
            return None
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:  # noqa: BLE001
                    pass

    async def route(self, input_no, output_no):
        """Route input to output; returns True if command sent successfully."""
        payload = (
            PROTOCOL_FRAME_START
            + f"{input_no:02d}{output_no:02d}".encode()
            + PROTOCOL_FRAME_END
        )
        ok = await self._send_receive(payload, 0) is not None
        if ok:
            self.routing[output_no] = input_no
            self._notify_listeners()
        return ok

    async def refresh(self):
        """Query current routing table from the matrix."""
        data = await self._send_receive(b"MT00RD0000NT", None, timeout=4)
        if data is None:
            return False
        # Responses arrive as concatenated LINK:O<o>I<i>;END frames.
        try:
            text = data.decode("ascii", errors="ignore")
            new_routing = {}
            for chunk in text.split(";END"):
                chunk = chunk.strip()
                if not chunk.startswith("LINK:"):
                    continue
                body = chunk[len("LINK:"):]
                out = body[body.find("O") + 1 : body.find("I")]
                inp = body[body.find("I") + 1 :]
                new_routing[int(out)] = int(inp)
            if new_routing:
                self.routing = new_routing
                self._notify_listeners()
            return True
        except Exception:  # noqa: BLE001
            _LOGGER.exception("Could not parse status from %s:%s", self.host, self.port)
            return False

    @callback
    def _notify_listeners(self):
        for listener in self.listeners:
            listener()

    @callback
    def register(self, listener):
        self.listeners.append(listener)

    @callback
    def start_polling(self, interval):
        if self._unsub_poll is None and interval > 0:
            self._unsub_poll = async_track_time_interval(
                self.hass, self._async_poll_tick, __import__("datetime").timedelta(seconds=interval)
            )

    @callback
    def stop_polling(self):
        if self._unsub_poll is not None:
            self._unsub_poll()
            self._unsub_poll = None

    async def _async_poll_tick(self, now):
        await self.refresh()


def _get_clients(hass):
    return hass.data.setdefault(DOMAIN, {}).setdefault("matrix_clients", {})


async def async_setup_platform(hass, config, async_add_entities, discovery_info=None):
    """Set up the Tesmart matrix select entities."""
    await async_setup_reload_service(hass, DOMAIN, PLATFORMS + ["select"])

    clients = _get_clients(hass)
    entities = []

    for device_id, device_config in config[CONF_MATRIXES].items():
        host = device_config[CONF_HOST]
        port = device_config[CONF_PORT]
        key = f"{host}:{port}"
        if key not in clients:
            clients[key] = TesmartMatrixClient(hass, host, port)
        client = clients[key]

        inputs = _parse_ports(device_config, CONF_INPUTS, "HDMI")
        outputs = _parse_ports(device_config, CONF_OUTPUTS, "Output")
        base_name = device_config.get(ATTR_FRIENDLY_NAME, device_id)

        for output_no, output_name in sorted(outputs.items()):
            entities.append(
                TesmartMatrixOutputSelect(
                    client,
                    device_id,
                    base_name,
                    output_no,
                    output_name,
                    inputs,
                )
            )
        client.start_polling(device_config[CONF_POLL_INTERVAL])
        # initial state fetch, non-blocking for setup
        asyncio.create_task(client.refresh())

    async_add_entities(entities)


class TesmartMatrixOutputSelect(SelectEntity):
    """One output of the matrix; current option = input routed to it."""

    _attr_should_poll = False
    _attr_icon = "mdi:video-input-hdmi"

    def __init__(self, client, device_id, base_name, output_no, output_name, inputs):
        self._client = client
        self._device_id = device_id
        self._base_name = base_name
        self._output_no = output_no
        self._output_name = output_name
        self._inputs = inputs  # {port_int: friendly_name}

        self._attr_name = f"{base_name} {output_name}"
        self._attr_unique_id = f"{device_id}_output_{output_no}"
        self._attr_options = sorted(inputs.values())

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self._device_id)},
            "name": self._base_name,
            "manufacturer": "Tesmart",
            "model": "HDMI Matrix (8x8 control panel V2 protocol)",
        }

    @property
    def current_option(self):
        input_no = self._client.routing.get(self._output_no)
        if input_no is None:
            return None
        return self._inputs.get(input_no)

    async def async_select_option(self, option):
        input_no = next(
            (port for port, name in self._inputs.items() if name == option), None
        )
        if input_no is None:
            _LOGGER.error("Unknown input %r for %s", option, self._attr_name)
            return
        await self._client.route(input_no, self._output_no)

    async def async_added_to_hass(self):
        await super().async_added_to_hass()
        self._client.register(self._async_handle_update)

    async def async_will_remove_from_hass(self):
        self._client.listeners.remove(self._async_handle_update)
        if not self._client.listeners:
            self._client.stop_polling()
        await super().async_will_remove_from_hass()

    @callback
    def _async_handle_update(self):
        self.async_write_ha_state()
