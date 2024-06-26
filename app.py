from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
import sqlite3

app = Flask(__name__)

# MQTT setup
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPICS = [("company/messages", 0), ("otp/messages", 0)]

mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")
    client.subscribe(MQTT_TOPICS)

def on_message(client, userdata, msg):
    print(f"Received message '{msg.payload.decode()}' on topic '{msg.topic}'")
    store_message(msg.topic, msg.payload.decode())

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

# SQLite setup
def init_db():
    conn = sqlite3.connect('messages.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS messages
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  topic TEXT,
                  payload TEXT)''')
    conn.commit()
    conn.close()

def store_message(topic, payload):
    conn = sqlite3.connect('messages.db')
    c = conn.cursor()
    c.execute("INSERT INTO messages (topic, payload) VALUES (?, ?)", (topic, payload))
    conn.commit()
    conn.close()

init_db()

# Flask routes
@app.route('/')
def index():
    return "Welcome to the Python App!"

@app.route('/company', methods=['POST'])
def company_message():
    payload = request.json.get('message')
    mqtt_client.publish('company/messages', payload)
    return jsonify({"status": "Company message sent"}), 200

@app.route('/otp', methods=['POST'])
def otp_message():
    payload = request.json.get('message')
    mqtt_client.publish('otp/messages', payload)
    return jsonify({"status": "OTP message sent"}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
