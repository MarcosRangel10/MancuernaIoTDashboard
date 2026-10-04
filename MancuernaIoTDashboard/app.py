import os
from flask import Flask, render_template, jsonify
import mysql.connector

app = Flask(__name__)

def obtener_conexion():
    return mysql.connector.connect(
        host=os.getenv("MYSQLHOST"),
        port=int(os.getenv("MYSQLPORT", "3306")),
        user=os.getenv("MYSQLUSER"),
        password=os.getenv("MYSQLPASSWORD"),
        database=os.getenv("MYSQLDATABASE")
    )

def obtener_ultimo_dato():
    try:
        conexion = obtener_conexion()
        cursor = conexion.cursor(dictionary=True)
        cursor.execute("SELECT * FROM datos_mancuerna ORDER BY id DESC LIMIT 1")
        dato = cursor.fetchone()
        cursor.close()
        conexion.close()
        return dato
    except Exception as e:
        print("Error al consultar BD:", e)
        return None

def obtener_historial_onda():
    try:
        conexion = obtener_conexion()
        cursor = conexion.cursor(dictionary=True)
        # Obtenemos los últimos 20 registros para la gráfica
        cursor.execute("SELECT ax, ay, az, fecha_hora FROM datos_mancuerna ORDER BY id DESC LIMIT 20")
        registros = cursor.fetchall()
        cursor.close()
        conexion.close()
        return registros[::-1] # Invertir para orden cronológico
    except Exception as e:
        print("Error al consultar historial BD:", e)
        return []

@app.route("/api/datos")
def api_datos():
    dato = obtener_ultimo_dato()
    historial = obtener_historial_onda()
    
    if dato:
        dato["fecha_hora"] = str(dato["fecha_hora"])
    
    return jsonify({
        "actual": dato,
        "historial": historial
    })

@app.route("/")
def inicio():
    dato = obtener_ultimo_dato()
    return render_template("index.html", dato=dato)

if __name__ == "__main__":
    puerto = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=puerto)