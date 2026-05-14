from flask import Flask, send_from_directory, make_response
from flask_sock import Sock
import threading, time, json, socket as tcp, urllib.request

app = Flask(__name__, static_folder=None)
app.config['SOCK_SERVER_OPTIONS'] = {'ping_interval': 5}   # keeps GPSMAP WS alive
sock = Sock(app)

PYPILOT_HOST = '192.168.1.5'
PYPILOT_PORT = 23322

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

BASE_WATCH = {
    'ap.heading':         0.25,
    'ap.heading_command': 0.25,
    'ap.enabled':         0.25,
    'ap.mode':            0.5,
    'ap.pilot':           1.0,
    'wind.direction':     0.5,
    'rudder.angle':       0.25,
    'ap.gains':           1.0,          # try as a single dict value
    **{f'ap.gains.{n}': 1.0 for n in GAIN_NAMES},
}


class PypilotClient:
    def __init__(self):
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

    def _connect(self):
        s = tcp.socket(tcp.AF_INET, tcp.SOCK_STREAM)
        s.settimeout(10)
        s.connect((PYPILOT_HOST, PYPILOT_PORT))
        s.settimeout(30)

        with self._sock_lock:
            self._sock = s
        with self._state_lock:
            self._state['message'] = 'Connected to pypilot'

        watch_msg = 'watch=' + json.dumps(BASE_WATCH) + '\n'
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

    # ── Protocol handling ────────────────────────────────────────────────────
    def _handle(self, line: str):
        if '=' in line and not line.startswith('{'):
            key, _, val_str = line.partition('=')
            self._apply(key.strip(), val_str.strip())
        elif line.startswith('{'):
            try:
                for key, val in json.loads(line).items():
                    self._apply(key, str(val))
            except Exception:
                pass

    def _apply(self, key: str, val_str: str):
        # Handle gain dict value: ap.gains={"P": 0.003, "I": 0, ...}
        if key == 'ap.gains':
            try:
                gains_dict = json.loads(val_str)
                print(f'[pypilot] ap.gains dict: {gains_dict}')
                with self._state_lock:
                    for name, value in gains_dict.items():
                        uname = name.upper()
                        if uname in self._state['gains']:
                            self._state['gains'][uname]['value'] = float(value)
            except Exception as e:
                print(f'[pypilot] ap.gains parse error: {e} val={val_str!r}')
            return

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
                    raw = val_str.strip('"').strip("'")
                    self._state['mode'] = MODE_FROM_PYPILOT.get(raw, raw)
                    print(f'[pypilot] mode={self._state["mode"]}')
                elif key == 'ap.pilot':
                    print(f'[pypilot] pilot={val_str}')
                elif key == 'wind.direction':
                    if val_str.lower() in ('false', 'none', 'null', ''):
                        self._state['wind_angle'] = None
                    else:
                        self._state['wind_angle'] = float(val_str)
                elif key == 'rudder.angle':
                    self._state['rudder_angle'] = float(val_str)
                elif 'gain' in key.lower():
                    print(f'[pypilot] GAIN KEY HIT: {key}={val_str}')
                    gain_name = key.split('.')[-1].upper()
                    if gain_name in self._state['gains']:
                        self._state['gains'][gain_name]['value'] = float(val_str)
                elif key in ('values', 'list'):
                    pass  # ignore descriptor responses
            except (ValueError, TypeError) as e:
                print(f'[pypilot] PARSE ERROR key={key!r} val={val_str!r}: {e}')

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

    # ── State snapshot ───────────────────────────────────────────────────────
    def snapshot(self):
        with self._state_lock:
            s = dict(self._state)

        err = s['course'] - s['heading']
        if err >  180: err -= 360
        if err < -180: err += 360

        if 'connect' in s['message'].lower() or 'reconnect' in s['message'].lower():
            pass
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
        s['gains'] = {n: dict(v) for n, v in s['gains'].items()}
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
            pilot.set(f'ap.gains.{name}', round(float(value), 3))


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
    resp = make_response(send_from_directory('app', 'index.html'))
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    resp.headers['Pragma']        = 'no-cache'
    return resp


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
