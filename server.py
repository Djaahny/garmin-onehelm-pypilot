from flask import Flask, send_from_directory
from flask_sock import Sock
import time, json, math

app = Flask(__name__, static_folder=None)
sock = Sock(app)

@app.route("/onehelm/config.json")
def config():
    return send_from_directory("onehelm", "config.json", mimetype="application/json")

@app.route("/onehelm/icon.png")
def icon():
    return send_from_directory("onehelm", "icon.png", mimetype="image/png")

@app.route("/")
@app.route("/app/")
def app_page():
    return send_from_directory("app", "index.html")

@sock.route("/ws")
def ws(ws):
    t = 0
    while True:
        value = 50 + 40 * math.sin(t)
        ws.send(json.dumps({"value": value}))
        t += 0.1
        time.sleep(0.05)

app.run(host="0.0.0.0", port=8000)
