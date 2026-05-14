from flask import Flask, send_from_directory
from flask_sock import Sock
import threading, time, json, math

app = Flask(__name__, static_folder=None)
sock = Sock(app)

# ── Simulated pypilot state ────────────────────────────────────────────────
pilot = {
    'heading':    247.0,
    'course':     247.0,
    'mode':       'auto',
    'engaged':    False,
    'wind_angle': 42.0,
    'lock':       threading.Lock(),
}

def simulate():
    t = 0
    while True:
        time.sleep(0.1)
        t += 0.1
        with pilot['lock']:
            wander = 0.08 * math.sin(t * 0.3) + 0.03 * math.sin(t * 1.1)
            pilot['heading'] = (pilot['heading'] + wander) % 360

            if pilot['engaged']:
                err = pilot['course'] - pilot['heading']
                if err >  180: err -= 360
                if err < -180: err += 360
                correction = max(-2.0, min(2.0, err * 0.25))
                pilot['heading'] = (pilot['heading'] + correction) % 360

            pilot['wind_angle'] = (pilot['wind_angle'] + 0.02 * math.sin(t * 0.07)) % 360

threading.Thread(target=simulate, daemon=True).start()


def state_snapshot():
    with pilot['lock']:
        err = pilot['course'] - pilot['heading']
        if err >  180: err -= 360
        if err < -180: err += 360
        if not pilot['engaged']:
            msg = 'STANDBY — pilot disengaged'
        elif abs(err) < 2:
            msg = 'On course'
        elif abs(err) < 10:
            msg = f'Correcting {"port" if err < 0 else "stbd"} {abs(err):.1f}°'
        else:
            msg = f'Large deviation {abs(err):.1f}° {"port" if err < 0 else "stbd"}'
        return {
            'heading':    round(pilot['heading'], 1),
            'course':     round(pilot['course'],  1),
            'mode':       pilot['mode'],
            'engaged':    pilot['engaged'],
            'wind_angle': round(pilot['wind_angle'], 1),
            'message':    msg,
        }


def handle_cmd(msg):
    cmd = msg.get('cmd')
    val = msg.get('value')
    with pilot['lock']:
        if cmd == 'engage':
            pilot['engaged'] = True
            pilot['course']  = round(pilot['heading'], 1)
        elif cmd == 'standby':
            pilot['engaged'] = False
        elif cmd == 'mode':
            if val in ('auto', 'wind', 'nav', 'level'):
                pilot['mode'] = val
        elif cmd == 'adjust':
            pilot['course'] = (pilot['course'] + float(val)) % 360
        elif cmd == 'tack_port':
            pilot['course'] = (pilot['course'] - 100) % 360
        elif cmd == 'tack_stbd':
            pilot['course'] = (pilot['course'] + 100) % 360


# ── Routes ──────────────────────────────────────────────────────────────────
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


# ── WebSocket ────────────────────────────────────────────────────────────────
@sock.route('/ws')
def ws_handler(ws):
    stop = threading.Event()

    # Push thread — sends state at 10 Hz
    def pusher():
        while not stop.is_set():
            try:
                ws.send(json.dumps(state_snapshot()))
            except Exception:
                stop.set()
                return
            time.sleep(0.1)

    t = threading.Thread(target=pusher, daemon=True)
    t.start()

    # Receive loop — handles commands from client
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
