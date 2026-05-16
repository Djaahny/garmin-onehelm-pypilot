from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import asyncio, threading, time, json, socket as tcp, math, os

app = FastAPI()

# Start with PYPILOT_MOCK=1 for GUI development without hardware
DEV_MOCK = os.environ.get('PYPILOT_MOCK', '0') == '1'

PYPILOT_HOST = os.environ.get('PYPILOT_HOST', '192.168.1.5')
PYPILOT_PORT = int(os.environ.get('PYPILOT_PORT', '23322'))

# ── Mode mapping ──────────────────────────────────────────────────────────────
# pypilot wire values  →  UI mode ids
MODE_FROM_PYPILOT = {
    'compass':   'compass',
    'gps':       'gps',
    'nav':       'nav',
    'wind':      'wind',
    'true wind': 'level',   # UI label "TRUE WIND"
}
# UI mode ids  →  pypilot wire values
MODE_TO_PYPILOT = {
    'compass': 'compass',
    'gps':     'gps',
    'nav':     'nav',
    'wind':    'wind',
    'level':   'true wind',
}

GAIN_NAMES    = ['P', 'I', 'D', 'DD', 'FF', 'PR']
GAIN_DEFAULTS = {
    'P':  {'min': 0.0, 'max': 3.0},
    'I':  {'min': 0.0, 'max': 1.0},
    'D':  {'min': 0.0, 'max': 5.0},
    'DD': {'min': 0.0, 'max': 5.0},
    'FF': {'min': 0.0, 'max': 3.0},
    'PR': {'min': 0.0, 'max': 5.0},
}
MOCK_GAIN_VALUES = {'P': 1.0, 'I': 0.1, 'D': 0.5, 'DD': 0.3, 'FF': 1.0, 'PR': 0.0}

# Keys to subscribe to on connection.
# Values are update intervals in seconds (0 = every change).
BASE_WATCH = {
    # Core autopilot
    'ap.enabled':           0.25,
    'ap.heading':           0.25,
    'ap.heading_command':   0.25,
    'ap.mode':              0.5,
    'ap.modes':             2.0,
    'ap.pilot':             2.0,
    'ap.version':           10.0,
    # Tacking — pypilot expects tack.direction then tack.state="begin"
    'ap.tack.state':        0.25,
    'ap.tack.direction':    2.0,
    # Profiles
    'profile':              2.0,
    'profiles':             5.0,
    # Gains — both the dict form and individual float sub-keys
    'ap.gains':             1.0,
    **{f'ap.gains.{n}': 1.0 for n in GAIN_NAMES},
    # Wind
    'wind.direction':       0.5,
    'wind.speed':           1.0,
    # Rudder feedback
    'rudder.angle':         0.25,
    # Servo / motor controller
    'servo.state':          1.0,
    'servo.controller':     5.0,
    'servo.voltage':        2.0,
    'servo.watts':          1.0,
    'servo.flags':          2.0,
    'servo.amp_hours':      5.0,
    # IMU / attitude
    'imu.pitch':            0.5,
    'imu.roll':             0.5,
    'imu.heading_offset':   5.0,
    'imu.uptime':           1.0,   # doubles as heartbeat
    'imu.error':            2.0,
    # Navigation (OpenCPN waypoint data forwarded by pypilot_pi plugin)
    'ap.dtw':               1.0,   # distance to waypoint (nautical miles)
    'ap.btw':               1.0,   # bearing to waypoint (degrees)
    'ap.xte':               1.0,   # cross-track error (nautical miles, signed)
}


# ── Real pypilot client ───────────────────────────────────────────────────────

class PypilotClient:
    def __init__(self):
        self._state = {
            # Autopilot
            'heading':             0.0,
            'course':              0.0,
            'mode':                'compass',
            'modes':               list(MODE_FROM_PYPILOT.values()),
            'engaged':             False,
            'pilot':               '',
            'version':             '',
            # Tacking
            'tack_state':          'none',
            'tack_direction':      '',
            # Profiles
            'profile':             'default',
            'profiles':            ['default'],
            # Wind
            'wind_angle':          None,
            'wind_speed':          None,
            # Rudder
            'rudder_angle':        None,
            # Servo / motor
            'servo_state':         '',
            'servo_controller':    '',
            'servo_voltage':       None,
            'servo_watts':         None,
            'servo_flags':         '',
            'servo_amp_hours':     None,
            # IMU
            'imu_pitch':           None,
            'imu_roll':            None,
            'imu_heading_offset':  0.0,
            'imu_error':           '',
            'imu_uptime':          '',
            # Navigation
            'nav_dtw':             None,
            'nav_btw':             None,
            'nav_xte':             None,
            # Gains
            'gains': {
                n: {'value': None,
                    'min': GAIN_DEFAULTS[n]['min'],
                    'max': GAIN_DEFAULTS[n]['max']}
                for n in GAIN_NAMES
            },
            'message': 'Connecting to pypilot…',
        }
        self._state_lock = threading.Lock()
        self._sock       = None
        self._sock_lock  = threading.Lock()
        self._last_rx    = 0.0
        threading.Thread(target=self._run, daemon=True).start()

    # ── Connection loop ───────────────────────────────────────────────────
    def _run(self):
        while True:
            try:
                self._connect()
            except Exception as e:
                print(f'[pypilot] error: {e}')
            with self._state_lock:
                self._state['message'] = 'Reconnecting to pypilot…'
            time.sleep(3)

    def _connect(self):
        s = tcp.socket(tcp.AF_INET, tcp.SOCK_STREAM)
        s.settimeout(10)
        s.connect((PYPILOT_HOST, PYPILOT_PORT))
        with self._sock_lock:
            self._sock = s
        with self._state_lock:
            self._state['message'] = 'Connected to pypilot'
        self._last_rx = time.time()

        watch_msg = 'watch=' + json.dumps(BASE_WATCH) + '\n'
        print(f'[pypilot] SEND: {watch_msg.strip()}')
        s.sendall(watch_msg.encode())

        buf = ''
        while True:
            if time.time() - self._last_rx > 35:
                print('[pypilot] heartbeat timeout — reconnecting')
                break
            s.settimeout(5)
            try:
                chunk = s.recv(4096).decode('utf-8', errors='ignore')
            except tcp.timeout:
                continue
            if not chunk:
                break
            self._last_rx = time.time()
            buf += chunk
            while '\n' in buf:
                line, buf = buf.split('\n', 1)
                line = line.strip()
                if line:
                    self._handle(line)

        with self._sock_lock:
            self._sock = None

    # ── Protocol parsing ──────────────────────────────────────────────────
    def _handle(self, line: str):
        if '=' not in line:
            return
        key, _, raw = line.partition('=')
        key = key.strip()
        raw = raw.strip()
        try:
            val = json.loads(raw)
        except Exception:
            val = raw       # fall back to plain string
        print(f'[pypilot] RECV: {key}={repr(val)}')
        self._apply(key, val, raw)

    def _apply(self, key: str, val, raw: str):
        with self._state_lock:
            s = self._state
            try:
                if key == 'ap.heading':
                    s['heading'] = float(val)

                elif key == 'ap.heading_command':
                    s['course'] = float(val)

                elif key == 'ap.enabled':
                    # pypilot sends JSON true/false
                    s['engaged'] = (val is True) or str(val).lower() in ('true', '1')

                elif key == 'ap.mode':
                    m = str(val).strip('"\'')
                    s['mode'] = MODE_FROM_PYPILOT.get(m, m)

                elif key == 'ap.modes':
                    # Returns JSON array of available mode strings
                    if isinstance(val, list):
                        s['modes'] = [MODE_FROM_PYPILOT.get(m, m) for m in val]

                elif key == 'ap.pilot':
                    s['pilot'] = str(val).strip('"\'')

                elif key == 'ap.version':
                    s['version'] = str(val).strip('"\'')

                elif key == 'ap.tack.state':
                    s['tack_state'] = str(val).strip('"\'')

                elif key == 'ap.tack.direction':
                    s['tack_direction'] = str(val).strip('"\'')

                elif key == 'profile':
                    s['profile'] = str(val).strip('"\'')

                elif key == 'profiles':
                    if isinstance(val, list):
                        s['profiles'] = [str(p).strip('"\'') for p in val]

                elif key == 'wind.direction':
                    sv = str(val).lower()
                    s['wind_angle'] = None if sv in ('false', 'none', 'null', '') else float(val)

                elif key == 'wind.speed':
                    sv = str(val).lower()
                    s['wind_speed'] = None if sv in ('false', 'none', 'null', '') else float(val)

                elif key == 'rudder.angle':
                    sv = str(val).lower()
                    s['rudder_angle'] = None if sv in ('false', 'none', 'null', '') else float(val)

                elif key == 'servo.state':
                    s['servo_state'] = str(val).strip('"\'')

                elif key == 'servo.controller':
                    s['servo_controller'] = str(val).strip('"\'')

                elif key == 'servo.voltage':
                    sv = str(val).lower()
                    s['servo_voltage'] = None if sv in ('false', 'none') else float(val)

                elif key == 'servo.watts':
                    sv = str(val).lower()
                    s['servo_watts'] = None if sv in ('false', 'none') else float(val)

                elif key == 'servo.flags':
                    s['servo_flags'] = str(val).strip('"\'')

                elif key == 'servo.amp_hours':
                    sv = str(val).lower()
                    s['servo_amp_hours'] = None if sv in ('false', 'none') else float(val)

                elif key == 'imu.pitch':
                    sv = str(val).lower()
                    s['imu_pitch'] = None if sv in ('false', 'none') else float(val)

                elif key == 'imu.roll':
                    sv = str(val).lower()
                    s['imu_roll'] = None if sv in ('false', 'none') else float(val)

                elif key == 'imu.heading_offset':
                    s['imu_heading_offset'] = float(val)

                elif key == 'imu.uptime':
                    s['imu_uptime'] = str(val).strip('"\'')

                elif key == 'imu.error':
                    s['imu_error'] = str(val).strip('"\'') if val else ''

                elif key == 'ap.dtw':
                    sv = str(val).lower()
                    s['nav_dtw'] = None if sv in ('false', 'none', 'null', '') else float(val)

                elif key == 'ap.btw':
                    sv = str(val).lower()
                    s['nav_btw'] = None if sv in ('false', 'none', 'null', '') else float(val)

                elif key == 'ap.xte':
                    sv = str(val).lower()
                    s['nav_xte'] = None if sv in ('false', 'none', 'null', '') else float(val)

                elif key == 'ap.gains':
                    # Dict form: {"P": 0.5, "I": 0.1, ...}
                    if isinstance(val, dict):
                        for name, v in val.items():
                            uname = name.upper()
                            if uname in s['gains']:
                                s['gains'][uname]['value'] = float(v)

                elif key.startswith('ap.gains.'):
                    # Individual form: ap.gains.P or ap.gains.P.value etc.
                    parts = key.split('.')
                    if len(parts) >= 3:
                        gname = parts[2].upper()
                        if gname in s['gains']:
                            if isinstance(val, dict):
                                # {"value": x, "min": y, "max": z}
                                if 'value' in val:
                                    s['gains'][gname]['value'] = float(val['value'])
                                if 'min' in val:
                                    s['gains'][gname]['min'] = float(val['min'])
                                if 'max' in val:
                                    s['gains'][gname]['max'] = float(val['max'])
                            else:
                                s['gains'][gname]['value'] = float(val)

            except (ValueError, TypeError, KeyError) as e:
                print(f'[pypilot] parse error key={key!r} raw={raw!r}: {e}')

    # ── Outgoing ──────────────────────────────────────────────────────────
    def _send_raw(self, msg: str):
        with self._sock_lock:
            if self._sock:
                try:
                    self._sock.sendall(msg.encode('utf-8'))
                except Exception as e:
                    print(f'[pypilot] send error: {e}')

    def set(self, name: str, value):
        """Send  name=json_value\\n  to pypilot. Strings are JSON-encoded (quoted)."""
        if isinstance(value, bool):
            wire = 'true' if value else 'false'
        elif isinstance(value, str):
            wire = json.dumps(value)      # adds surrounding quotes
        else:
            wire = str(value)
        msg = f'{name}={wire}\n'
        print(f'[pypilot] SEND: {msg.strip()}')
        self._send_raw(msg)

    # ── Snapshot (sent to browser via WebSocket) ──────────────────────────
    def snapshot(self):
        with self._state_lock:
            s    = dict(self._state)
            gains = {n: dict(v) for n, v in s['gains'].items()}

        err = s['course'] - s['heading']
        if err >  180: err -= 360
        if err < -180: err += 360

        msg = s['message']
        if 'connect' not in msg.lower() and 'reconnect' not in msg.lower():
            if not s['engaged']:
                msg = 'STANDBY'
            elif abs(err) < 2:
                msg = 'On course'
            elif abs(err) < 10:
                msg = f'Correcting {"port" if err < 0 else "stbd"} {abs(err):.1f}°'
            else:
                msg = f'Large deviation {abs(err):.1f}° {"port" if err < 0 else "stbd"}'

        def rnd(v, n=1): return round(v, n) if v is not None else None

        return {
            'heading':            round(s['heading'], 1),
            'course':             round(s['course'],  1),
            'mode':               s['mode'],
            'modes':              s['modes'],
            'engaged':            s['engaged'],
            'pilot':              s['pilot'],
            'version':            s['version'],
            'tack_state':         s['tack_state'],
            'tack_direction':     s['tack_direction'],
            'profile':            s['profile'],
            'profiles':           s['profiles'],
            'wind_angle':         rnd(s['wind_angle']),
            'wind_speed':         rnd(s['wind_speed']),
            'rudder_angle':       rnd(s['rudder_angle']),
            'servo_state':        s['servo_state'],
            'servo_controller':   s['servo_controller'],
            'servo_voltage':      rnd(s['servo_voltage']),
            'servo_watts':        rnd(s['servo_watts']),
            'servo_flags':        s['servo_flags'],
            'servo_amp_hours':    rnd(s['servo_amp_hours'], 3),
            'imu_pitch':          rnd(s['imu_pitch']),
            'imu_roll':           rnd(s['imu_roll']),
            'imu_heading_offset': s['imu_heading_offset'],
            'imu_error':          s['imu_error'],
            'imu_uptime':         s['imu_uptime'],
            'nav_dtw':            rnd(s['nav_dtw'], 2),
            'nav_btw':            rnd(s['nav_btw'], 1),
            'nav_xte':            rnd(s['nav_xte'], 3),
            'gains':              gains,
            'message':            msg,
            'pypilot_host':       PYPILOT_HOST,
            'pypilot_port':       PYPILOT_PORT,
        }


# ── Mock client (PYPILOT_MOCK=1) ──────────────────────────────────────────────

class MockPypilotClient:
    """Simulates a live pypilot feed. Use PYPILOT_MOCK=1 for GUI development."""
    def __init__(self):
        self._t0 = time.time()
        self._state_lock = threading.Lock()
        self._state = {
            'heading':             0.0,
            'course':              45.0,
            'mode':                'compass',
            'modes':               ['compass', 'gps', 'nav', 'wind', 'level'],
            'engaged':             False,
            'pilot':               'simple',
            'version':             'pypilot 0.20 (mock)',
            'tack_state':          'none',
            'tack_direction':      '',
            'profile':             'default',
            'profiles':            ['default', 'light_air', 'heavy_weather'],
            'wind_angle':          0.0,
            'wind_speed':          12.0,
            'rudder_angle':        0.0,
            'servo_state':         'ready',
            'servo_controller':    'mock_controller',
            'servo_voltage':       12.6,
            'servo_watts':         0.0,
            'servo_flags':         '',
            'servo_amp_hours':     0.0,
            'imu_pitch':           0.0,
            'imu_roll':            0.0,
            'imu_heading_offset':  0.0,
            'imu_error':           '',
            'imu_uptime':          '0:00:00',
            'nav_dtw':             3.7,
            'nav_btw':             245.0,
            'nav_xte':             0.0,
            'gains': {
                n: {'value': MOCK_GAIN_VALUES[n],
                    'min':   GAIN_DEFAULTS[n]['min'],
                    'max':   GAIN_DEFAULTS[n]['max']}
                for n in GAIN_NAMES
            },
        }
        self._tack_started = 0.0

    def set(self, name: str, value):
        print(f'[mock] SET {name}={value!r}')
        with self._state_lock:
            s = self._state
            if name == 'ap.enabled':
                s['engaged'] = (value is True) or str(value).lower() in ('true', '1')
            elif name == 'ap.heading_command':
                s['course'] = float(value) % 360
            elif name == 'ap.mode':
                m = str(value).strip('"\'')
                s['mode'] = MODE_FROM_PYPILOT.get(m, m)
            elif name == 'ap.tack.direction':
                s['tack_direction'] = str(value).strip('"\'')
            elif name == 'ap.tack.state':
                v = str(value).strip('"\'')
                s['tack_state'] = v
                if v == 'begin':
                    self._tack_started = time.time()
            elif name in ('profile', 'ap.profile'):
                v = str(value).strip('"\'')
                if v in s['profiles']:
                    s['profile'] = v
            elif name == 'imu.heading_offset':
                s['imu_heading_offset'] = float(value)
            elif name == 'servo.amp_hours':
                s['servo_amp_hours'] = float(value)
            elif name.startswith('ap.gains.'):
                gname = name.split('.')[2].upper()
                if gname in s['gains']:
                    s['gains'][gname]['value'] = float(value)

    def snapshot(self):
        with self._state_lock:
            t       = time.time() - self._t0
            s       = self._state
            course  = s['course']
            engaged = s['engaged']

            heading = (course + 8 * math.sin(t * 0.3)) % 360
            s['heading']       = heading
            s['wind_angle']    = (t * 20) % 360
            s['wind_speed']    = 10 + 5 * math.sin(t * 0.1)
            s['rudder_angle']  = 15 * math.sin(t * 0.7)
            s['imu_pitch']     = 3  * math.sin(t * 0.2)
            s['imu_roll']      = 5  * math.sin(t * 0.15)
            s['imu_uptime']    = time.strftime('%H:%M:%S', time.gmtime(t))
            s['servo_watts']   = (20 + 10 * abs(math.sin(t * 0.7))) if engaged else 0.0
            s['nav_dtw']       = max(0.0, 3.7 - t * 0.002)
            s['nav_btw']       = (245 + 8 * math.sin(t * 0.04)) % 360
            s['nav_xte']       = 0.05 * math.sin(t * 0.18)
            if engaged:
                s['servo_amp_hours'] += s['servo_watts'] / 12.0 / 3600

            # Simulate tack completion after 8 s
            if s['tack_state'] == 'begin':
                s['tack_state'] = 'tacking'
            elif s['tack_state'] == 'tacking':
                if time.time() - self._tack_started > 8:
                    s['tack_state'] = 'none'
                    s['tack_direction'] = ''

            err = course - heading
            if err >  180: err -= 360
            if err < -180: err += 360

            msg = ('STANDBY' if not engaged
                   else 'On course' if abs(err) < 2
                   else f'Correcting {"port" if err < 0 else "stbd"} {abs(err):.1f}°'
                   if abs(err) < 10
                   else f'Large deviation {abs(err):.1f}° {"port" if err < 0 else "stbd"}')

            def rnd(v, n=1): return round(v, n) if v is not None else None

            return {
                'heading':            round(heading, 1),
                'course':             round(course, 1),
                'mode':               s['mode'],
                'modes':              list(s['modes']),
                'engaged':            engaged,
                'pilot':              s['pilot'],
                'version':            s['version'],
                'tack_state':         s['tack_state'],
                'tack_direction':     s['tack_direction'],
                'profile':            s['profile'],
                'profiles':           list(s['profiles']),
                'wind_angle':         rnd(s['wind_angle']),
                'wind_speed':         rnd(s['wind_speed']),
                'rudder_angle':       rnd(s['rudder_angle']),
                'servo_state':        s['servo_state'],
                'servo_controller':   s['servo_controller'],
                'servo_voltage':      rnd(s['servo_voltage']),
                'servo_watts':        rnd(s['servo_watts']),
                'servo_flags':        s['servo_flags'],
                'servo_amp_hours':    rnd(s['servo_amp_hours'], 3),
                'imu_pitch':          rnd(s['imu_pitch']),
                'imu_roll':           rnd(s['imu_roll']),
                'imu_heading_offset': s['imu_heading_offset'],
                'imu_error':          s['imu_error'],
                'imu_uptime':         s['imu_uptime'],
                'nav_dtw':            rnd(s['nav_dtw'], 2),
                'nav_btw':            rnd(s['nav_btw'], 1),
                'nav_xte':            rnd(s['nav_xte'], 3),
                'gains':              {n: dict(v) for n, v in s['gains'].items()},
                'message':            msg,
                'pypilot_host':       PYPILOT_HOST,
                'pypilot_port':       PYPILOT_PORT,
            }


# ── Command dispatcher ────────────────────────────────────────────────────────

pilot = MockPypilotClient() if DEV_MOCK else PypilotClient()


def _st(key):
    with pilot._state_lock:
        return pilot._state[key]


def handle_cmd(msg: dict):
    cmd = msg.get('cmd')
    val = msg.get('value')

    if cmd == 'engage':
        pilot.set('ap.heading_command', round(_st('heading'), 1))
        pilot.set('ap.enabled', True)

    elif cmd == 'standby':
        pilot.set('ap.enabled', False)

    elif cmd == 'mode':
        wire = MODE_TO_PYPILOT.get(val)
        if wire:
            pilot.set('ap.mode', wire)

    elif cmd == 'adjust':
        new = (_st('course') + float(val)) % 360
        pilot.set('ap.heading_command', round(new, 1))

    elif cmd == 'tack_port':
        # pypilot protocol: set direction, then trigger state="begin"
        pilot.set('ap.tack.direction', 'port')
        pilot.set('ap.tack.state', 'begin')

    elif cmd == 'tack_stbd':
        pilot.set('ap.tack.direction', 'starboard')
        pilot.set('ap.tack.state', 'begin')

    elif cmd == 'cancel_tack':
        pilot.set('ap.tack.state', 'none')

    elif cmd == 'gain':
        name  = msg.get('name')
        value = msg.get('value')
        if name in GAIN_NAMES and value is not None:
            pilot.set(f'ap.gains.{name}', round(float(value), 3))

    elif cmd == 'profile':
        if val:
            pilot.set('profile', val)

    elif cmd == 'heading_offset':
        # Adjust by delta
        if val is not None:
            current = _st('imu_heading_offset')
            pilot.set('imu.heading_offset', round(current + float(val), 1))

    elif cmd == 'heading_offset_set':
        if val is not None:
            pilot.set('imu.heading_offset', round(float(val), 1))

    elif cmd == 'reset_amp_hours':
        pilot.set('servo.amp_hours', 0.0)


# ── HTTP routes ───────────────────────────────────────────────────────────────

@app.get('/')
@app.get('/app/')
async def app_page():
    return FileResponse('app/index.html', headers={
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma':        'no-cache',
    })

app.mount('/app',     StaticFiles(directory='app'),     name='app-static')
app.mount('/onehelm', StaticFiles(directory='onehelm'), name='onehelm')


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket('/ws')
async def ws_handler(websocket: WebSocket):
    await websocket.accept()

    async def pusher():
        while True:
            try:
                await websocket.send_text(json.dumps(pilot.snapshot()))
            except Exception:
                return
            await asyncio.sleep(0.1)

    task = asyncio.create_task(pusher())
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                handle_cmd(json.loads(raw))
            except (ValueError, KeyError):
                pass
    except WebSocketDisconnect:
        pass
    finally:
        task.cancel()


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000,
                ws_ping_interval=5, ws_ping_timeout=20)
