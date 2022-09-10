import time
import py_eureka_client.eureka_client as eureka_client
from flask import Flask, request, jsonify
from flask_pymongo import PyMongo

rest_port = 8060
eureka_client.init(eureka_server="http://eureka:8761/eureka",
                   app_name="preprocesamiento",
                   instance_port=rest_port)

app = Flask(__name__)
app.config["MONGO_URI"] = 'mongodb://root:123456@mongo:27018/preprocesamiento?authSource=admin'
# app.config["MONGO_URI"] = 'mongodb://root:123456@localhost:27017/pruebasPython?authSource=admin'
mongo = PyMongo(app)


# Metodo de probar conexion con servidor
@app.route('/prueba/', methods=["GET"])
def prueba():
    return "Conectado Python"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=rest_port)
    # app.run(debug=True, port=rest_port)
