from flask import Flask, send_from_directory
from flask_sock import Sock
import threading, time, json, socket as tcp, urllib.request

app = Flask(__name__, static_folder=None)
sock = Sock(app)

PYPILOT_HOST = '192.168.1.5'
PYPILOT_PORT = 23322

# pypilot mode names → UI mode names (and reverse)
MODE_FROM_PYPILOT = {'compass': 'auto', 'wind': 'wind', 'gps': 'nav', 'level': 'level', 'true wind': 'wind'}
MODE_TO_PYPILOT   = {'auto': 'compass', 'wind': 'wind', 'nav': 'gps', 'level': 'level'}

GAIN_NAMES = ['P', 'I', 'D', 'DD', 'FF', 'PR']
GAIN_DEFAULTS = {
    'P':  {'min': 0.0, 'max': 3.0},
    'I':  {'min': 0.0, 'max': 1.0},
    'D':  {'min': 0.0, 'max': 5.0},
    'DD': {'min': 0.0, 'max': 5.0},
    'FF': {'min': 0.0, 'max': 3.0},
    'PR': {'min': 0.0, 'max': 5.0},
}

# key → update period in seconds
WATCH_PERIODS = {
    'ap.heading':         0.25,
    'ap.heading_command': 0.25,
    'ap.enabled':         0.25,
    'ap.mode':            0.5,
    'wind.direction':     0.5,
    'rudder.angle':       0.25,
    **{f'ap.gains.{n}': 1.0 for n in GAIN_NAMES},
}


class PypilotClient:
    def __init__(self):
        self._gain_key_map = {}   # gain_name → actual pypilot key, e.g. 'P' → 'ap.gains.P'
        self._state = {
            'heading':      0.0,
            'course':       0.0,
            'mode':         'auto',
            'engaged':      False,
            'wind_angle':   None,
            'rudder_angle': None,
            'message':      'Connecting to pypilot…',
            'gains': {
                n: {'value': None, 'min': GAIN_DEFAULTS[n]['min'], 'max': GAIN_DEFAULTS[n]['max']}
                for n in GAIN_NAMES
            },
        }
        self._state_lock = threading.Lock()
        self._sock       = None
        self._sock_lock  = threading.Lock()
        threading.Thread(target=self._run, daemon=True).start()

    # ── Connection loop ──────────────────────────────────────────────────────
    def _run(self):
        while True:
            try:
                self._connect()
            except Exception as e:
                print(f'pypilot: {e}')
            with self._state_lock:
                self._state['message'] = 'Reconnecting to pypilot…'
            time.sleep(3)

    def _discover_gains(self):
        """Try HTTP first, fall back to nothing (TCP list handled in recv loop)."""
        try:
            url = f'http://{PYPILOT_HOST}:8080/values'
            resp = urllib.request.urlopen(url, timeout=4)
            all_vals = json.loads(resp.read().decode())
            gain_keys = {k: v for k, v in all_vals.items() if 'gain' in k.lower()}
            if not gain_keys:
                ap_keys = sorted(k for k in all_vals if k.startswith('ap.'))
                print(f'[pypilot] No gain keys via HTTP. ap.* keys: {ap_keys}')
                return
            print(f'[pypilot] Gain keys via HTTP: {list(gain_keys.keys())}')
            new_map = {}
            with self._state_lock:
                for key, info in gain_keys.items():
                    gain_name = key.split('.')[-1].upper()
                    if gain_name not in GAIN_NAMES:
                        continue
                    new_map[gain_name] = key
                    if isinstance(info, dict):
                        if 'min' in info:
                            self._state['gains'][gain_name]['min'] = float(info['min'])
                        if 'max' in info:
                            self._state['gains'][gain_name]['max'] = float(info['max'])
            self._gain_key_map = new_map
            print(f'[pypilot] Gain key map: {new_map}')
        except Exception as e:
            print(f'[pypilot] HTTP discovery failed: {e}')

    def _connect(self):
        self._discover_gains()

        s = tcp.socket(tcp.AF_INET, tcp.SOCK_STREAM)
        s.settimeout(10)
        s.connect((PYPILOT_HOST, PYPILOT_PORT))
        s.settimeout(30)

        with self._sock_lock:
            self._sock = s

        with self._state_lock:
            self._state['message'] = 'Connected to pypilot'

        # Request pypilot's full value list so we can discover gain key names
        s.sendall(b'list=1\n')

        # Build watch dict — base values + every plausible gain key pattern
        watch = dict(WATCH_PERIODS)
        watch['ap.pilot'] = 1.0   # active pilot algorithm name
        for name in GAIN_NAMES:
            for pattern in [
                f'ap.gains.{name}',
                f'ap.gains.{name.lower()}',
                f'ap.pilot.gains.{name}',
                f'ap.pilots.basic.gains.{name}',
                f'ap.pilots.simple.gains.{name}',
            ]:
                watch[pattern] = 1.0

        watch_msg = 'watch=' + json.dumps(watch) + '\n'
        print(f'[pypilot] SEND: {watch_msg.strip()}')
        s.sendall(watch_msg.encode('utf-8'))

        buf = ''
        while True:
            chunk = s.recv(4096).decode('utf-8', errors='ignore')
            if not chunk:
                break
            buf += chunk
            while '\n' in buf:
                line, buf = buf.split('\n', 1)
                line = line.strip()
                if line:
                    print(f'[pypilot] RECV: {line}')
                    self._handle(line)

        with self._sock_lock:
            self._sock = None

    def _handle(self, line: str):
        # Primary pypilot wire format: key=value
        if '=' in line and not line.startswith('{'):
            key, _, val_str = line.partition('=')
            self._apply(key.strip(), val_str.strip())
            return
        # JSON fallback
        if line.startswith('{'):
            try:
                for key, val in json.loads(line).items():
                    self._apply(key, str(val))
            except Exception:
                pass

    def _apply(self, key: str, val_str: str):
        with self._state_lock:
            try:
                if key == 'ap.heading':
                    self._state['heading'] = float(val_str)
                    print(f'[pypilot] heading={self._state["heading"]}')
                elif key == 'ap.heading_command':
                    self._state['course'] = float(val_str)
                    print(f'[pypilot] course={self._state["course"]}')
                elif key == 'ap.enabled':
                    self._state['engaged'] = val_str.lower() in ('true', '1')
                    print(f'[pypilot] engaged={self._state["engaged"]}')
                elif key == 'ap.mode':
                    self._state['mode'] = MODE_FROM_PYPILOT.get(val_str, val_str)
                    print(f'[pypilot] mode={self._state["mode"]} (raw={val_str})')
                elif key == 'wind.direction':
                    self._state['wind_angle'] = float(val_str)
                elif key == 'rudder.angle':
                    self._state['rudder_angle'] = float(val_str)
                    print(f'[pypilot] rudder={self._state["rudder_angle"]}')
                elif key == 'ap.pilot':
                    print(f'[pypilot] active pilot algorithm: {val_str}')
                elif 'gain' in key.lower():
                    print(f'[pypilot] GAIN KEY: {key}={val_str}')
                    gain_name = key.split('.')[-1].upper()
                    if gain_name in self._state['gains']:
                        self._state['gains'][gain_name]['value'] = float(val_str)
                elif key in ('values', 'list'):
                    self._parse_values_descriptor(val_str)
                else:
                    pass  # unrecognised key - visible in RECV log above
            except (ValueError, TypeError) as e:
                print(f'[pypilot] PARSE ERROR key={key!r} val={val_str!r}: {e}')

    def _parse_values_descriptor(self, val_str: str):
        try:
            desc = json.loads(val_str)
        except ValueError:
            print(f'[pypilot] values= parse error')
            return

        print(f'[pypilot] values descriptor: {len(desc)} keys total')

        # Print every key that might be a gain
        gain_keys = [k for k in desc if 'gain' in k.lower()]
        if gain_keys:
            print(f'[pypilot] GAIN KEYS FOUND: {gain_keys}')
        else:
            # Fall back: print everything under ap.* so we can spot the right names
            ap_keys = sorted(k for k in desc if k.startswith('ap.'))
            print(f'[pypilot] No gain keys found. ap.* keys: {ap_keys}')

        with self._state_lock:
            for key, info in desc.items():
                if 'gain' not in key.lower():
                    continue
                gain_name = key.split('.')[-1]
                if gain_name not in self._state['gains']:
                    continue
                if isinstance(info, dict):
                    if 'min' in info:
                        self._state['gains'][gain_name]['min'] = float(info['min'])
                    if 'max' in info:
                        self._state['gains'][gain_name]['max'] = float(info['max'])
                    print(f'[pypilot] gain descriptor {gain_name}: {info}')

    # ── Outgoing commands ────────────────────────────────────────────────────
    def _send_raw(self, data: str):
        with self._sock_lock:
            if self._sock:
                try:
                    self._sock.sendall(data.encode('utf-8'))
                except Exception:
                    pass

    def set(self, name, value):
        if isinstance(value, bool):
            val_str = 'true' if value else 'false'
        else:
            val_str = str(value)
        msg = f'{name}={val_str}\n'
        print(f'[pypilot] SEND: {msg.strip()}')
        self._send_raw(msg)

    # ── State snapshot for WebSocket push ────────────────────────────────────
    def snapshot(self):
        with self._state_lock:
            s = dict(self._state)

        err = s['course'] - s['heading']
        if err >  180: err -= 360
        if err < -180: err += 360

        if 'pypilot' in s['message'].lower() or 'connect' in s['message'].lower():
            pass  # keep connection message
        elif not s['engaged']:
            s['message'] = 'STANDBY — pilot disengaged'
        elif abs(err) < 2:
            s['message'] = 'On course'
        elif abs(err) < 10:
            s['message'] = f'Correcting {"port" if err < 0 else "stbd"} {abs(err):.1f}°'
        else:
            s['message'] = f'Large deviation {abs(err):.1f}° {"port" if err < 0 else "stbd"}'

        s['heading']    = round(s['heading'], 1)
        s['course']     = round(s['course'],  1)
        s['wind_angle']   = round(s['wind_angle'],   1) if s['wind_angle']   is not None else None
        s['rudder_angle'] = round(s['rudder_angle'], 1) if s['rudder_angle'] is not None else None
        # deep-copy gains so lock is not held across serialisation
        s['gains'] = {
            n: dict(v) for n, v in s['gains'].items()
        }
        return s


pilot = PypilotClient()


def handle_cmd(msg):
    cmd = msg.get('cmd')
    val = msg.get('value')

    if cmd == 'engage':
        with pilot._state_lock:
            current_heading = pilot._state['heading']
        pilot.set('ap.heading_command', round(current_heading, 1))
        pilot.set('ap.enabled', True)
    elif cmd == 'standby':
        pilot.set('ap.enabled', False)
    elif cmd == 'mode':
        pypilot_mode = MODE_TO_PYPILOT.get(val)
        if pypilot_mode:
            pilot.set('ap.mode', pypilot_mode)
    elif cmd == 'adjust':
        with pilot._state_lock:
            new_course = (pilot._state['course'] + float(val)) % 360
        pilot.set('ap.heading_command', new_course)
    elif cmd == 'tack_port':
        with pilot._state_lock:
            new_course = (pilot._state['course'] - 100) % 360
        pilot.set('ap.heading_command', new_course)
    elif cmd == 'tack_stbd':
        with pilot._state_lock:
            new_course = (pilot._state['course'] + 100) % 360
        pilot.set('ap.heading_command', new_course)
    elif cmd == 'gain':
        name  = msg.get('name')
        value = msg.get('value')
        if name in GAIN_NAMES and value is not None:
            real_key = pilot._gain_key_map.get(name, f'ap.gains.{name}')
            pilot.set(real_key, round(float(value), 3))


# ── Routes ───────────────────────────────────────────────────────────────────
@app.route('/onehelm/config.json')
def config():
    return send_from_directory('onehelm', 'config.json', mimetype='application/json')

@app.route('/onehelm/icon.png')
def icon():
    return send_from_directory('onehelm', 'icon.png', mimetype='image/png')

@app.route('/')
@app.route('/app/')
def app_page():
    return send_from_directory('app', 'index.html')


# ── WebSocket ─────────────────────────────────────────────────────────────────
@sock.route('/ws')
def ws_handler(ws):
    stop = threading.Event()

    def pusher():
        while not stop.is_set():
            try:
                ws.send(json.dumps(pilot.snapshot()))
            except Exception:
                stop.set()
                return
            time.sleep(0.1)

    threading.Thread(target=pusher, daemon=True).start()

    try:
        while True:
            raw = ws.receive()
            if raw is None:
                break
            try:
                handle_cmd(json.loads(raw))
            except (ValueError, KeyError):
                pass
    finally:
        stop.set()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=False)
