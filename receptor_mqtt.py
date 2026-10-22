import os
import json
import ssl
import time

import paho.mqtt.client as mqtt
import mysql.connector


# =====================================================
# CONFIGURACIÓN MQTT - HIVE MQ CLOUD
# =====================================================

MQTT_BROKER = "d3befa5909cf4595a75259012d836398.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_TOPIC = "mancuerna/+/datos"

MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")


# =====================================================
# CONFIGURACIÓN MYSQL - RAILWAY
# =====================================================

DB_HOST = os.getenv("MYSQLHOST")
DB_PORT = int(os.getenv("MYSQLPORT", "3306"))
DB_USER = os.getenv("MYSQLUSER")
DB_PASSWORD = os.getenv("MYSQLPASSWORD")
DB_NAME = os.getenv("MYSQLDATABASE")


# =====================================================
# CONTROL DE GUARDADO
# =====================================================

ultimo_guardado = {}

INTERVALO_GUARDADO = 1.0


# =====================================================
# CONEXIÓN MYSQL
# =====================================================

def conectar_mysql():

    try:

        conexion = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            ssl_disabled=False,
            connection_timeout=15
        )

        return conexion

    except mysql.connector.Error as error:

        print("ERROR MySQL:", error)

        return None


# =====================================================
# MQTT - CONEXIÓN
# =====================================================

def al_conectar(client, userdata, flags, rc):

    if rc == 0:

        print("========================================")
        print("   MANCUERNA IoT CLOUD CONECTADO")
        print("========================================")

        client.subscribe(MQTT_TOPIC)

        print(f"Suscrito a: {MQTT_TOPIC}")
        print("Esperando datos del ESP32...")

    else:

        print(f"ERROR MQTT. Código: {rc}")


# =====================================================
# MQTT - MENSAJE RECIBIDO
# =====================================================

def al_recibir(client, userdata, msg):

    try:

        datos = json.loads(msg.payload.decode())

        dispositivo = datos.get(
            "dispositivo",
            "Desconocido"
        )

        ejercicio = datos.get(
            "ejercicio",
            "ninguno"
        )

        angulo = datos.get(
            "angulo",
            0.0
        )

        movimiento = datos.get(
            "movimiento",
            False
        )

        repeticiones = datos.get(
            "repeticiones",
            0
        )

        objetivo = datos.get(
            "objetivo",
            0
        )

        correctas = datos.get(
            "correctas",
            0
        )

        rapidas = datos.get(
            "rapidas",
            0
        )

        lentas = datos.get(
            "lentas",
            0
        )

        estado = datos.get(
            "estado",
            "ESPERA"
        )

        subida = datos.get(
            "subida",
            0.0
        )

        bajada = datos.get(
            "bajada",
            0.0
        )

        aceleracion = datos.get(
            "aceleracion",
            0.0
        )

        velocidad_angular = datos.get(
            "velocidad_angular",
            0.0
        )


        # =================================================
        # CONTROL DE FRECUENCIA DE GUARDADO
        # =================================================

        ahora = time.time()

        ultima = ultimo_guardado.get(
            dispositivo,
            0
        )

        debe_guardar = (
            ahora - ultima >= INTERVALO_GUARDADO
        )

        if not debe_guardar:
            return


        # =================================================
        # CONECTAR MYSQL
        # =================================================

        conexion = conectar_mysql()

        if conexion is None:

            print(
                f"[{dispositivo}] "
                "No se pudo conectar a MySQL"
            )

            return


        cursor = None

        try:

            cursor = conexion.cursor()


            # =============================================
            # INSERTAR DATOS
            # =============================================

            sql = """
                INSERT INTO datos_mancuerna
                (
                    dispositivo,
                    ejercicio,
                    angulo,
                    movimiento,
                    repeticiones,
                    objetivo,
                    correctas,
                    rapidas,
                    lentas,
                    estado,
                    subida,
                    bajada,
                    aceleracion,
                    velocidad_angular
                )
                VALUES
                (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
            """


            valores = (
                dispositivo,
                ejercicio,
                angulo,
                movimiento,
                repeticiones,
                objetivo,
                correctas,
                rapidas,
                lentas,
                estado,
                subida,
                bajada,
                aceleracion,
                velocidad_angular
            )


            cursor.execute(
                sql,
                valores
            )


            conexion.commit()


            ultimo_guardado[dispositivo] = ahora


            print(
                f"[{dispositivo}] "
                f"Ejercicio: {ejercicio} | "
                f"Ángulo: {angulo:.2f}° | "
                f"Reps: {repeticiones} | "
                f"Estado: {estado} | "
                f"✓ BD"
            )


        except mysql.connector.Error as error:

            print(
                "ERROR guardando en MySQL:",
                error
            )


        finally:

            if cursor is not None:
                cursor.close()

            conexion.close()


    except json.JSONDecodeError:

        print(
            "ERROR: mensaje MQTT no contiene JSON válido"
        )


    except Exception as error:

        print(
            "ERROR procesando mensaje:",
            error
        )


# =====================================================
# CLIENTE MQTT
# =====================================================

client = mqtt.Client()

client.username_pw_set(
    MQTT_USER,
    MQTT_PASSWORD
)

client.tls_set(
    cert_reqs=ssl.CERT_REQUIRED
)

client.on_connect = al_conectar
client.on_message = al_recibir


# =====================================================
# CONEXIÓN A HIVE MQ
# =====================================================

print("========================================")
print("       MANCUERNA IoT CLOUD")
print("========================================")

print("Conectando a HiveMQ Cloud...")


while True:

    try:

        client.connect(
            MQTT_BROKER,
            MQTT_PORT,
            60
        )

        break

    except Exception as error:

        print(
            "Error conectando a HiveMQ."
        )

        print(
            "Reintentando en 5 segundos...",
            error
        )

        time.sleep(5)


# =====================================================
# ESPERAR MENSAJES
# =====================================================

print("Esperando datos del ESP32...")

client.loop_forever()
