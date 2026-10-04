import os
import json
import ssl
import time

from flask import Flask, render_template, jsonify, request

import mysql.connector
import paho.mqtt.client as mqtt


app = Flask(__name__)


# =====================================================
# CONFIGURACIÓN
# =====================================================

MQTT_BROKER = "d3befa5909cf4595a75259012d836398.s1.eu.hivemq.cloud"
MQTT_PORT = 8883

MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

DISPOSITIVO = "Mancuerna01"


# =====================================================
# TOPICS DEL ESP32
# =====================================================

TOPIC_STATUS = f"mancuerna/{DISPOSITIVO}/status"
TOPIC_CONFIG = f"mancuerna/{DISPOSITIVO}/config"
TOPIC_COMANDO = f"mancuerna/{DISPOSITIVO}/comando"
TOPIC_DATOS = f"mancuerna/{DISPOSITIVO}/datos"
TOPIC_EVENTOS = f"mancuerna/{DISPOSITIVO}/eventos"
TOPIC_RESUMEN = f"mancuerna/{DISPOSITIVO}/resumen"


# =====================================================
# MYSQL
# =====================================================

DB_CONFIG = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", "3306")),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": os.getenv("MYSQLDATABASE")
}


def get_db():

    return mysql.connector.connect(
        **DB_CONFIG
    )


# =====================================================
# ESTADO GLOBAL
# =====================================================

estado_global = {

    "esp32_conectado": False,

    "ultimo_heartbeat": 0,

    "en_rutina": False,

    "datos_actuales": {},

    "ultimo_evento": None,

    "resumen_final": None
}


# =====================================================
# MQTT - CONEXIÓN
# =====================================================

def al_conectar(client, userdata, flags, rc):

    if rc == 0:

        print("========================================")
        print("      FLASK MQTT CONECTADO")
        print("========================================")

        client.subscribe(TOPIC_STATUS)

        client.subscribe(TOPIC_DATOS)

        client.subscribe(TOPIC_EVENTOS)

        client.subscribe(TOPIC_RESUMEN)

        print("Suscripciones MQTT realizadas")

    else:

        print(
            f"Error conectando MQTT. Código: {rc}"
        )


# =====================================================
# MQTT - MENSAJES
# =====================================================

def al_recibir(client, userdata, msg):

    global estado_global

    try:

        topic = msg.topic

        payload = msg.payload.decode()

        # ---------------------------------------------
        # STATUS
        # ---------------------------------------------

        if topic == TOPIC_STATUS:

            estado = payload.strip().upper()

            if estado == "ONLINE":

                estado_global["esp32_conectado"] = True
                estado_global["ultimo_heartbeat"] = time.time()

            elif estado == "OFFLINE":

                estado_global["esp32_conectado"] = False

            return


        # ---------------------------------------------
        # DATOS
        # ---------------------------------------------

        if topic == TOPIC_DATOS:

            datos = json.loads(payload)

            estado_global["datos_actuales"] = datos

            estado_global["ultimo_heartbeat"] = time.time()

            estado_global["esp32_conectado"] = True

            # Si hay un ejercicio activo
            if datos.get("ejercicio", "ninguno") != "ninguno":

                estado_global["en_rutina"] = True

            return


        # ---------------------------------------------
        # EVENTOS
        # ---------------------------------------------

        if topic == TOPIC_EVENTOS:

            evento = json.loads(payload)

            estado_global["ultimo_evento"] = evento

            estado_global["ultimo_heartbeat"] = time.time()

            return


        # ---------------------------------------------
        # RESUMEN
        # ---------------------------------------------

        if topic == TOPIC_RESUMEN:

            resumen = json.loads(payload)

            estado_global["resumen_final"] = resumen

            estado_global["en_rutina"] = False

            guardar_resumen(resumen)

            return


    except Exception as error:

        print(
            "Error procesando MQTT:",
            error
        )


# =====================================================
# GUARDAR RESUMEN
# =====================================================

def guardar_resumen(resumen):

    try:

        conn = get_db()

        cursor = conn.cursor()

        sql = """
            INSERT INTO sesiones_entrenamiento
            (
                ejercicio,
                reps_solicitadas,
                reps_totales,
                reps_optimas,
                reps_rapidas,
                reps_lentas,
                eficiencia
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
        """

        ejercicio = resumen.get(
            "ejercicio",
            "ninguno"
        )

        solicitadas = resumen.get(
            "solicitadas",
            0
        )

        realizadas = resumen.get(
            "realizadas",
            0
        )

        correctas = resumen.get(
            "correctas",
            0
        )

        rapidas = resumen.get(
            "rapidas",
            0
        )

        lentas = resumen.get(
            "lentas",
            0
        )

        eficiencia = resumen.get(
            "eficiencia",
            0
        )

        valores = (
            ejercicio,
            solicitadas,
            realizadas,
            correctas,
            rapidas,
            lentas,
            eficiencia
        )

        cursor.execute(
            sql,
            valores
        )

        conn.commit()

        cursor.close()

        conn.close()

        print(
            f"Resumen guardado: "
            f"{ejercicio} | "
            f"{correctas}/{solicitadas}"
        )


    except Exception as error:

        print(
            "Error guardando resumen:",
            error
        )


# =====================================================
# CLIENTE MQTT
# =====================================================

mqtt_client = mqtt.Client()

mqtt_client.username_pw_set(
    MQTT_USER,
    MQTT_PASSWORD
)

mqtt_client.tls_set(
    cert_reqs=ssl.CERT_REQUIRED
)

mqtt_client.on_connect = al_conectar

mqtt_client.on_message = al_recibir


# =====================================================
# CONECTAR MQTT
# =====================================================

try:

    mqtt_client.connect(
        MQTT_BROKER,
        MQTT_PORT,
        60
    )

    mqtt_client.loop_start()

except Exception as error:

    print(
        "Error inicializando MQTT:",
        error
    )


# =====================================================
# WEB
# =====================================================

@app.route("/")
def index():

    return render_template(
        "index.html"
    )


# =====================================================
# API - ESTADO
# =====================================================

@app.route("/api/estado")
def api_estado():

    conectado = (
        time.time()
        - estado_global["ultimo_heartbeat"]
    ) < 10.0

    estado_global["esp32_conectado"] = conectado

    return jsonify({

        "conectado": conectado,

        "en_rutina":
            estado_global["en_rutina"],

        "datos":
            estado_global["datos_actuales"],

        "evento":
            estado_global["ultimo_evento"],

        "resumen":
            estado_global["resumen_final"]

    })


# =====================================================
# API - CONFIGURAR EJERCICIO
# =====================================================

@app.route(
    "/api/configurar",
    methods=["POST"]
)
def configurar():

    try:

        datos = request.json

        ejercicio = datos.get(
            "ejercicio",
            "ninguno"
        )

        repeticiones = int(
            datos.get(
                "repeticiones",
                0
            )
        )

        tiempo_subida = float(
            datos.get(
                "tiempo_subida",
                1.0
            )
        )

        tiempo_bajada = float(
            datos.get(
                "tiempo_bajada",
                2.0
            )
        )


        payload = {

            "ejercicio": ejercicio,

            "repeticiones": repeticiones,

            "tiempo_subida":
                tiempo_subida,

            "tiempo_bajada":
                tiempo_bajada
        }


        resultado = mqtt_client.publish(
            TOPIC_CONFIG,
            json.dumps(payload)
        )


        return jsonify({

            "status": "OK",

            "mensaje":
                "Configuración enviada",

            "configuracion":
                payload

        })


    except Exception as error:

        return jsonify({

            "status": "ERROR",

            "mensaje": str(error)

        }), 500


# =====================================================
# API - COMANDOS
# =====================================================

@app.route(
    "/api/comando",
    methods=["POST"]
)
def comando():

    try:

        datos = request.json

        comando = datos.get(
            "comando"
        )


        comandos_validos = [

            "CALIBRAR_INICIO",

            "CALIBRAR_FINAL",

            "INICIAR",

            "DETENER",

            "REINICIAR"

        ]


        if comando not in comandos_validos:

            return jsonify({

                "status": "ERROR",

                "mensaje":
                    "Comando no válido"

            }), 400


        payload = {

            "comando": comando
        }


        mqtt_client.publish(

            TOPIC_COMANDO,

            json.dumps(payload)

        )


        if comando == "INICIAR":

            estado_global[
                "en_rutina"
            ] = True

            estado_global[
                "resumen_final"
            ] = None


        elif comando == "DETENER":

            estado_global[
                "en_rutina"
            ] = False


        return jsonify({

            "status": "OK",

            "comando": comando

        })


    except Exception as error:

        return jsonify({

            "status": "ERROR",

            "mensaje": str(error)

        }), 500


# =====================================================
# API - HISTORIAL
# =====================================================

@app.route(
    "/api/historial"
)
def api_historial():

    try:

        conn = get_db()

        cursor = conn.cursor(
            dictionary=True
        )


        cursor.execute(
            """
            SELECT *
            FROM sesiones_entrenamiento
            ORDER BY id DESC
            LIMIT 20
            """
        )


        registros = cursor.fetchall()


        cursor.close()

        conn.close()


        return jsonify(
            registros
        )


    except Exception as error:

        print(
            "Error historial:",
            error
        )

        return jsonify([])


# =====================================================
# EJECUCIÓN
# =====================================================

if __name__ == "__main__":

    app.run(

        host="0.0.0.0",

        port=int(
            os.getenv(
                "PORT",
                5000
            )
        )

    )