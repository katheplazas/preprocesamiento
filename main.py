import time
import pickle
import py_eureka_client.eureka_client as eureka_client
from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from sklearn import preprocessing

rest_port = 8060

eureka_client.init(eureka_server="http://eureka:8761/eureka",
                   app_name="preprocesamiento",
                   instance_port=rest_port)

app = Flask(__name__)
app.config["MONGO_URI"] = 'mongodb://root:123456@mongo:27018/preprocesamiento?authSource=admin'
# app.config["MONGO_URI"] = 'mongodb://root:123456@localhost:27017/pruebasPython?authSource=admin'
mongo = PyMongo(app)


# Metodo de probar conexion con servidor /
@app.route('/prueba', methods=["GET"])
def prueba():
    return "Conectado Python"


# Metodo para almacenar parametros de estandarizacion
@app.route('/save/param/standardization', methods=["POST"])
def save_param_standardization():
    if request.method == 'POST':
        if 'param' in request.files:
            param = request.files['param']
            algorithm_files = mongo.db.fs.files
            file = algorithm_files.find_one({'filename': 'param-standardization'})
            if file is not None:
                s = mongo.db.fs.chunks.find({'files_id': file['_id']})
                for i in range(len(list(s))):
                    mongo.db.fs.chunks.delete_one({'files_id': file['_id']})
                algorithm_files.delete_one({'filename': 'param-standardization'})
            mongo.save_file('param-standardization', param)
            response = jsonify({
                'param_name': 'param-standardization',
                'created_time': time.time(),
                'message': 'Save successfully'
            })
            return response, 200
        return not_param
    return not_post


# Metodo para leer parametros de estandarizacion
@app.route('/read/param/standardization', methods=["GET"])
def test_dt():
    if request.method == 'GET':
        file = mongo.db.fs.files.find_one({'filename': 'param-standardization'})
        binary = b""
        file_chunks = mongo.db.fs.chunks.find({'files_id': file['_id']})
        for i in file_chunks:
            binary += i['data']
        param_standardization = pickle.loads(binary)
        return param_standardization
    return "no method GET"


@app.errorhandler(404)
def not_param(error=None):
    response = jsonify({'message': 'Not param-standardization', 'status': 404})
    response.status_code = 404
    return response


@app.errorhandler(409)
def not_post(error=None):
    response = jsonify({'message': 'Is not a POST', 'status': 409})
    response.status_code = 409
    return response


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=rest_port)
    # app.run(debug=True, port=rest_port)
