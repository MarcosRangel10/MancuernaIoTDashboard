import json
import os
import ssl
import threading
import time

import mysql.connector
import paho.mqtt.client as mqtt
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

# HiveMQ Cloud
MQTT_BROKER = "d3befa5909cf4595a75259012d836398.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
DISPOSITIVO = "Mancuerna01"

TOPIC_STATUS = f"mancuerna/{DISPOSITIVO}/status"
TOPIC_CONFIG = f"mancuerna/{DISPOSITIVO}/config"
TOPIC_COMANDO = f"mancuerna/{DISPOSITIVO}/comando"
TOPIC_DATOS = f"mancuerna/{DISPOSITIVO}/datos"
TOPIC_EVENTOS = f"mancuerna/{DISPOSITIVO}/eventos"
TOPIC_RESUMEN = f"mancuerna/{DISPOSITIVO}/resumen"

# Render debe tener estas variables apuntando a Aiven:
# MYSQLHOST, MYSQLPORT, MYSQLUSER, MYSQLPASSWORD, MYSQLDATABASE
DB_CONFIG = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", "3306")),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": os.getenv("MYSQLDATABASE"),
    "ssl_disabled": False,
    "connection_timeout": 15,
}

# IMPORTANTE: en Render usar Gunicorn con --workers 1.
state_lock = threading.Lock()
estado_global = {
    "esp32_conectado": False,
    "ultimo_heartbeat": 0.0,
    "en_rutina": False,
    "datos_actuales": {},
    "ultimo_evento": None,
    "resumen_final": None,
}
ultimo_guardado = {}


def get_db():
    missing = [k for k in ("host", "user", "password", "database") if not DB_CONFIG.get(k)]
    if missing:
        raise RuntimeError("Faltan variables MySQL en Render: " + ", ".join(missing))
    return mysql.connector.connect(**DB_CONFIG)


def guardar_dato_mancuerna(datos):
    """Guarda una lectura por segundo como máximo. El estado web se actualiza aunque falle MySQL."""
    device = str(datos.get("dispositivo", DISPOSITIVO))
    now = time.monotonic()
    if now - ultimo_guardado.get(device, 0.0) < 1.0:
        return

    conn = cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO datos_mancuerna
            (dispositivo, ejercicio, angulo, movimiento, repeticiones, objetivo,
             correctas, rapidas, lentas, estado, subida, bajada, aceleracion,
             velocidad_angular)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            device,
            datos.get("ejercicio", "ninguno"),
            datos.get("angulo", 0),
            int(bool(datos.get("movimiento", False))),
            datos.get("repeticiones", 0),
            datos.get("objetivo", datos.get("repeticiones_objetivo", 0)),
            datos.get("correctas", 0),
            datos.get("rapidas", 0),
            datos.get("lentas", 0),
            datos.get("estado", "DESCONOCIDO"),
            datos.get("subida", 0),
            datos.get("bajada", 0),
            datos.get("aceleracion", 0),
            datos.get("velocidad_angular", 0),
        ))
        conn.commit()
        ultimo_guardado[device] = now
        print(f"Lectura guardada en MySQL | {device} | angulo={datos.get('angulo')}", flush=True)
    except Exception as exc:
        print(f"ERROR guardando lectura en MySQL: {exc}", flush=True)
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def guardar_resumen(resumen):
    conn = cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        values = (
            resumen.get("ejercicio", "ninguno"),
            resumen.get("solicitadas", 0),
            resumen.get("realizadas", 0),
            resumen.get("correctas", 0),
            resumen.get("rapidas", 0),
            resumen.get("lentas", 0),
            resumen.get("eficiencia", 0),
        )
        cursor.execute("""
            INSERT INTO sesiones_entrenamiento
            (ejercicio, reps_solicitadas, reps_totales, reps_optimas,
             reps_rapidas, reps_lentas, eficiencia)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, values)
        conn.commit()
        print(f"Resumen guardado: {values[0]} | {values[3]}/{values[1]}", flush=True)
    except Exception as exc:
        print(f"ERROR guardando resumen en MySQL: {exc}", flush=True)
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def al_conectar(client, userdata, flags, rc):
    if rc != 0:
        print(f"ERROR conexión MQTT; código={rc}", flush=True)
        return
    print(f"FLASK MQTT CONECTADO | PID={os.getpid()}", flush=True)
    for topic in (TOPIC_STATUS, TOPIC_DATOS, TOPIC_EVENTOS, TOPIC_RESUMEN):
        result, mid = client.subscribe(topic, qos=0)
        print(f"MQTT SUSCRIPCIÓN | {topic} | result={result} | mid={mid}", flush=True)


def al_recibir(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode("utf-8", errors="replace")
    print(f"MQTT RECIBIDO | PID={os.getpid()} | Tema: {topic}", flush=True)
    try:
        if topic == TOPIC_STATUS:
            status = payload.strip().upper()
            with state_lock:
                if status == "ONLINE":
                    estado_global["esp32_conectado"] = True
                    estado_global["ultimo_heartbeat"] = time.time()
                elif status == "OFFLINE":
                    estado_global["esp32_conectado"] = False
            print(f"MQTT STATUS | {status}", flush=True)
            return

        if topic == TOPIC_DATOS:
            datos = json.loads(payload)
            with state_lock:
                estado_global["datos_actuales"] = datos
                estado_global["ultimo_heartbeat"] = time.time()
                estado_global["esp32_conectado"] = True
                if datos.get("ejercicio") not in (None, "", "ninguno"):
                    estado_global["en_rutina"] = True
            print(f"MQTT DATOS ACTUALIZADOS | PID={os.getpid()} | claves={list(datos.keys())}", flush=True)
            guardar_dato_mancuerna(datos)
            return

        if topic == TOPIC_EVENTOS:
            with state_lock:
                estado_global["ultimo_evento"] = json.loads(payload)
                estado_global["ultimo_heartbeat"] = time.time()
                estado_global["esp32_conectado"] = True
            return

        if topic == TOPIC_RESUMEN:
            resumen = json.loads(payload)
            with state_lock:
                estado_global["resumen_final"] = resumen
                estado_global["en_rutina"] = False
                estado_global["ultimo_heartbeat"] = time.time()
                estado_global["esp32_conectado"] = True
            guardar_resumen(resumen)
            return
        print(f"MQTT tema no procesado: {topic}", flush=True)
    except Exception as exc:
        print(f"ERROR procesando MQTT ({topic}): {exc}", flush=True)


def al_desconectar(client, userdata, rc):
    print(f"MQTT DESCONECTADO | código={rc}", flush=True)


mqtt_client = mqtt.Client()
if MQTT_USER and MQTT_PASSWORD:
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
else:
    print("ERROR: faltan MQTT_USER o MQTT_PASSWORD en Render", flush=True)
mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
mqtt_client.on_connect = al_conectar
mqtt_client.on_message = al_recibir
mqtt_client.on_disconnect = al_desconectar
try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()
    print(f"MQTT cliente iniciado | PID={os.getpid()}", flush=True)
except Exception as exc:
    print(f"ERROR inicializando MQTT: {exc}", flush=True)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    return jsonify({"ok": True, "pid": os.getpid()})


@app.route("/api/estado")
def api_estado():
    with state_lock:
        last = estado_global["ultimo_heartbeat"]
        connected = (time.time() - last) < 30.0
        estado_global["esp32_conectado"] = connected
        result = {
            "conectado": connected,
            "en_rutina": estado_global["en_rutina"],
            "datos": dict(estado_global["datos_actuales"]),
            "evento": estado_global["ultimo_evento"],
            "resumen": estado_global["resumen_final"],
            "ultimo_heartbeat": last or None,
        }
    print(f"API ESTADO | PID={os.getpid()} | conectado={result['conectado']} | claves={list(result['datos'].keys())}", flush=True)
    return jsonify(result)


@app.route("/api/configurar", methods=["POST"])
def configurar():
    try:
        data = request.get_json(silent=True) or {}
        payload = {
            "ejercicio": data.get("ejercicio", "ninguno"),
            "repeticiones": int(data.get("repeticiones", 0)),
            "tiempo_subida": float(data.get("tiempo_subida", 1.0)),
            "tiempo_bajada": float(data.get("tiempo_bajada", 2.0)),
        }
        info = mqtt_client.publish(TOPIC_CONFIG, json.dumps(payload), qos=0)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            return jsonify({"status": "ERROR", "mensaje": f"Publicación MQTT fallida: {info.rc}"}), 503
        return jsonify({"status": "OK", "mensaje": "Configuración enviada", "configuracion": payload})
    except Exception as exc:
        print(f"ERROR /api/configurar: {exc}", flush=True)
        return jsonify({"status": "ERROR", "mensaje": str(exc)}), 500


@app.route("/api/comando", methods=["POST"])
def comando():
    try:
        data = request.get_json(silent=True) or {}
        command = data.get("comando")
        valid = {"CALIBRAR_INICIO", "CALIBRAR_FINAL", "INICIAR", "DETENER", "REINICIAR"}
        if command not in valid:
            return jsonify({"status": "ERROR", "mensaje": "Comando no válido"}), 400
        info = mqtt_client.publish(TOPIC_COMANDO, json.dumps({"comando": command}), qos=0)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            return jsonify({"status": "ERROR", "mensaje": f"Publicación MQTT fallida: {info.rc}"}), 503
        with state_lock:
            if command == "INICIAR":
                estado_global["en_rutina"] = True
                estado_global["resumen_final"] = None
            elif command == "DETENER":
                estado_global["en_rutina"] = False
        return jsonify({"status": "OK", "comando": command})
    except Exception as exc:
        print(f"ERROR /api/comando: {exc}", flush=True)
        return jsonify({"status": "ERROR", "mensaje": str(exc)}), 500


@app.route("/api/historial")
def api_historial():
    conn = cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM sesiones_entrenamiento ORDER BY id DESC LIMIT 20")
        return jsonify(cursor.fetchall())
    except Exception as exc:
        print(f"ERROR /api/historial: {exc}", flush=True)
        return jsonify({"error": "No se pudo consultar el historial"}), 500
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
