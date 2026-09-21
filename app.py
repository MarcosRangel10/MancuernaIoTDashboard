import os
from flask import Flask, render_template
import mysql.connector

app = Flask(__name__)


def obtener_ultimo_dato():

    conexion = mysql.connector.connect(
        host=os.getenv("MYSQLHOST"),
        port=int(os.getenv("MYSQLPORT", "3306")),
        user=os.getenv("MYSQLUSER"),
        password=os.getenv("MYSQLPASSWORD"),
        database=os.getenv("MYSQLDATABASE")
    )

    cursor = conexion.cursor(dictionary=True)

    cursor.execute("""
        SELECT *
        FROM datos_mancuerna
        ORDER BY id DESC
        LIMIT 1
    """)

    dato = cursor.fetchone()

    cursor.close()
    conexion.close()

    return dato


@app.route("/api/datos")
def api_datos():

    dato = obtener_ultimo_dato()

    if dato:
        dato["fecha_hora"] = str(dato["fecha_hora"])

    return dato


@app.route("/")
def inicio():

    dato = obtener_ultimo_dato()

    return render_template(
        "index.html",
        dato=dato
    )


if __name__ == "__main__":

    puerto = int(os.getenv("PORT", 5000))

    app.run(
        host="0.0.0.0",
        port=puerto
    )