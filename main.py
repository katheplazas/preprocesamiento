import json
import pickle
import time
from threading import Thread

import numpy as np
import pandas as pd
import py_eureka_client.eureka_client as eureka_client
from flask import Flask, request, jsonify
from flask_pymongo import PyMongo

import preprocesamiento_service

pd.options.display.max_columns = None
rest_port = 8050

eureka_client.init(eureka_server="http://eureka:8761/eureka",
                   app_name="preprocesamiento-seguridad",
                   instance_port=rest_port)

app = Flask(__name__)
app.config["MONGO_URI"] = 'mongodb://root:123456@mongo:27018/preprocesamiento?authSource=admin'  ## Remoto
# app.config["MONGO_URI"] = 'mongodb://root:123456@mongo:27017/preprocesamiento?authSource=admin'  ## Local
mongo = PyMongo(app)

received_data = None


# Metodo de probar conexion con servidor /
@app.route('/prueba', methods=["GET"])
def prueba():
    return "conectado"


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


# Metodo para estandarizar los datos del trafico de red
@app.route('/data/standardization', methods=["GET"])
async def process():
    if request.method == 'GET':
        data = request.get_json()
        # print(f'Tipo de dato que llega: {type(data)}')
        # print(f'dato que llega: {data}')
        # data = json.load(data)
        # traffic = request.files['traffic']
        # data_stream = traffic.stream.read()
        # stream = io.StringIO(data_stream.decode("UTF8"), newline=None)
        # reader = csv.reader(stream)

        # ids = ["stime", "proto", "saddr", "sport", "daddr", "dport", "pkts", "bytes",
        #       "state", "ltime", "dur", "spkts", "dpkts", "sbytes", "dbytes"]
        data = pd.DataFrame(data)
        print(f'DATOS QUE LLEGAN: \n{data.head()}')

        # print(f'data is null: {data.isnull().sum().sum}')

        dict_proto = {'arp': 0, 'tcp': 1, 'udp': 2, 'icmp': 4}
        data.insert(2, 'proto_number', data.proto.map(dict_proto, na_action='ignore'))
        data.proto_number = (data.proto_number.fillna(value=3)).astype('int64')

        dict_state = {'CON': 0, 'INT': 1, 'FIN': 2, 'NRS': 3, 'RST': 4, 'URP': 5, 'REQ': 6,
                      'ACC': 7, 'TST': 8, 'ECO': 9, 'MAS': 10, 'CLO': 11, 'TXD': 12, 'ECR': 13, 'URN': 14}
        data.insert(10, 'state_number', data.state.map(dict_state, na_action='ignore'))
        data.state_number = (data.state_number.fillna(value=15)).astype('int64')

        data[['sport']] = data[['sport']].astype('int64')
        data[['dport']] = data[['dport']].astype('int64')

        data.loc[data.proto == 'icmp', 'sport'] = -1
        data.loc[data.proto == 'icmp', 'dport'] = -1

        data.loc[data.proto == 'arp', 'sport'] = -1
        data.loc[data.proto == 'arp', 'dport'] = -1

        data.loc[data.sport == 0, 'sport'] = -1
        data.loc[data.dport == 0, 'dport'] = -1

        data[['dpkts']] = data[['dpkts']].astype('int64')
        data[['pkts']] = data[['pkts']].astype('int64')
        data[['dur']] = data[['dur']].astype('float64')
        data[['stime']] = data[['stime']].astype('float64')

        data[['bytes']] = data[['bytes']].astype('int64')
        data[['ltime']] = data[['ltime']].astype('float64')
        data[['spkts']] = data[['spkts']].astype('int64')
        data[['sbytes']] = data[['sbytes']].astype('int64')

        data = preprocesamiento_service.calcule_feature(data)

        print(f'DATOS A COPIAR: \n{data.head()}')
        data_save = data.copy()
        data_save['tag'] = 0
        # print(data_save)
        data_save['tag'] = np.where(((data_save['saddr'] == '9.9.9.9') & (data_save['dport'] == 80)), 1,
                                    data_save['tag'])
        data_save['tag'] = np.where(((data_save['saddr'] == '192.168.100.12') & (data_save['dport'] == 80)), 1,
                                    data_save['tag'])
        data_save['tag'] = np.where(((data_save['saddr'] == '192.168.100.13') & (data_save['dport'] == 80)), 1,
                                    data_save['tag'])

        data_save['attack_tool'] = ''
        data_save['attack_tool'] = np.where(((data_save['saddr'] == '9.9.9.9') & (data_save['dport'] == 80)),
                                            'hping3', data_save['attack_tool'])
        data_save['attack_tool'] = np.where(((data_save['saddr'] == '192.168.100.12') & (data_save['dport'] == 80)),
                                            'hulk', data_save['attack_tool'])
        data_save['attack_tool'] = np.where(((data_save['saddr'] == '192.168.100.13') & (data_save['dport'] == 80)),
                                            'golden-eye', data_save['attack_tool'])

        data_save['attack_param'] = ''
        data_save['attack_param'] = np.where(((data_save['saddr'] == '9.9.9.9') & (data_save['dport'] == 80)),
                                             'faster', data_save['attack_param'])
        data_save['attack_param'] = np.where(
            ((data_save['saddr'] == '192.168.100.13') & (data_save['dport'] == 80)),
            's10mg', data_save['attack_param'])  # s10 -> sockets = 10 mr -> metodo = random

        data.drop(['saddr', 'sport', 'daddr', 'dport', 'proto', 'state'], axis=1, inplace=True)
        file = mongo.db.fs.files.find_one({'filename': 'param-standardization'})
        binary = b""
        file_chunks = mongo.db.fs.chunks.find({'files_id': file['_id']})
        for i in file_chunks:
            binary += i['data']
        scaler = pickle.loads(binary)

        # Estandarizando
        data[data.columns] = scaler.transform(data[data.columns])
        ## DATO PRUEBA
        # print("SE ENVIAN LOS DATOS")
        # print(data)

        prediction = await preprocesamiento_service.data_publish(data)
        list_prediction = []
        list_time_prediction = []
        for i in prediction:
            list_prediction.append(i[:-1])
            list_time_prediction.append(i[-1])
        data_save['prediction_dt'] = list_prediction[0]
        data_save['time_prediction_dt'] = list_time_prediction[0]
        data_save['prediction_lr'] = list_prediction[1]
        data_save['time_prediction_lr'] = list_time_prediction[1]
        data_save['prediction_rf'] = list_prediction[2]
        data_save['time_prediction_rf'] = list_time_prediction[2]
        data_save['prediction_svm_linear'] = list_prediction[3]
        data_save['time_prediction_svm_linear'] = list_time_prediction[3]
        print(f'DATOS A GUARDAR: \n{data_save.head()}')
        data_save = data_save.to_dict('records')
        ### ALMACENAR
        data_files = mongo.db.data
        data_files.insert_many(data_save)
        return prediction


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
    t = Thread(target=preprocesamiento_service.data_suscription)
    t.start()
    app.run(host='0.0.0.0', port=rest_port)
    # app.run(debug=True, port=rest_port)
