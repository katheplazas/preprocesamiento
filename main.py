import time
import pickle
import io
import csv
import requests
import ast
import numpy as np
import pandas as pd
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

#API_URL = 'http://172.18.5.29:8061/prediction/model/dt'





# API_URL = 'http://prediccion/prediction/model/rf'
# API_URL = 'http://prediccion/prediction/model/lr'
# API_URL = 'http://prediccion/prediction/model/svm-linear'


'''def test_model(df):
    files = {
        'data': df.to_json().encode(),
        'type_ml': 'dt',
    }
    response = requests.get(API_URL, files=files)
    return response.text'''


# Metodo de probar conexion con servidor /
@app.route('/prueba', methods=["GET"])
def prueba():
    res = eureka_client.do_service("prediccion", "/prueba")
    return res


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
def process():
    if request.method == 'GET':
        if 'traffic' in request.files:
            traffic = request.files['traffic']
            data_stream = traffic.stream.read()
            stream = io.StringIO(data_stream.decode("UTF8"), newline=None)
            reader = csv.reader(stream)

            ids = ["stime", "proto", "saddr", "sport", "daddr", "dport", "pkts", "bytes",
                   "state", "ltime", "dur", "spkts", "dpkts", "sbytes", "dbytes"]
            data = pd.DataFrame(data=reader, columns=ids)
            data = data.drop([0], axis=0)
            data.reset_index(inplace=True, drop=False)
            data.drop(['index'], axis=1, inplace=True)

            # print(f'data is null: {data.isnull().sum().sum}')

            dict_proto = {'arp': 0, 'tcp': 1, 'udp': 2, 'icmp': 4}
            data.insert(2, 'proto_number', data.proto.map(dict_proto, na_action='ignore'))
            data.proto_number = (data.proto_number.fillna(value=3)).astype('int64')

            dict_state = {'CON': 0, 'INT': 1, 'FIN': 2, 'NRS': 3, 'RST': 4, 'URP': 5, 'REQ': 6,
                          'ACC': 7, 'TST': 8, 'ECO': 9, 'MAS': 10, 'CLO': 11, 'TXD': 12, 'ECR': 13, 'URN': 14}
            data.insert(10, 'state_number', data.state.map(dict_state, na_action='ignore'))
            data.state_number = (data.state_number.fillna(value=15)).astype('int64')

            data['sport'] = np.where((data['sport'] == 'http'), '80', data['sport'])
            data['sport'] = np.where((data['sport'] == 'http-alt'), '591', data['sport'])
            data['sport'] = np.where((data['sport'] == 'netbios-dgm'), '138', data['sport'])
            data['sport'] = np.where((data['sport'] == 'https'), '443', data['sport'])
            data['sport'] = np.where((data['sport'] == 'hostmon'), '5355', data['sport'])
            data['sport'] = np.where((data['sport'] == 'netbios-ns'), '137', data['sport'])
            data['sport'] = np.where((data['sport'] == 'mdns'), '5353', data['sport'])
            data['sport'] = np.where((data['sport'] == 'snap'), '4752', data['sport'])
            data['sport'] = np.where((data['sport'] == 'dhcpv6-client'), '546', data['sport'])
            data['sport'] = np.where((data['sport'] == 'dhcpv6-server'), '547', data['sport'])
            data['sport'] = np.where((data['sport'] == 'domain'), '53', data['sport'])
            data['sport'] = np.where((data['sport'] == '0x008f'), '143', data['sport'])
            data['sport'] = np.where((data['sport'] == '0x0085'), '133', data['sport'])
            data['sport'] = np.where((data['sport'] == '0x0000'), '0', data['sport'])
            data['sport'] = np.where((data['sport'] == ''), '-1', data['sport'])  # Vacio

            data['sport'] = np.where((data['proto'] == 'arp'), '-1', data['sport'])

            data['dport'] = np.where((data['dport'] == 'http'), '80', data['dport'])
            data['dport'] = np.where((data['dport'] == 'http-alt'), '591', data['dport'])
            data['dport'] = np.where((data['dport'] == 'netbios-dgm'), '138', data['dport'])
            data['dport'] = np.where((data['dport'] == 'https'), '443', data['dport'])
            data['dport'] = np.where((data['dport'] == 'hostmon'), '5355', data['dport'])
            data['dport'] = np.where((data['dport'] == 'netbios-ns'), '137', data['dport'])
            data['dport'] = np.where((data['dport'] == 'mdns'), '5353', data['dport'])
            data['dport'] = np.where((data['dport'] == 'snap'), '4752', data['dport'])
            data['dport'] = np.where((data['dport'] == 'dhcpv6-client'), '546', data['dport'])
            data['dport'] = np.where((data['dport'] == 'dhcpv6-server'), '547', data['dport'])
            data['dport'] = np.where((data['dport'] == '0x008f'), '143', data['dport'])
            data['dport'] = np.where((data['dport'] == '0x0085'), '133', data['dport'])
            data['dport'] = np.where((data['dport'] == '0x0000'), '0', data['dport'])
            data['dport'] = np.where((data['dport'] == 'domain'), '53', data['dport'])
            data['dport'] = np.where((data['dport'] == ''), '-1', data['dport'])  # Vacio

            data['dport'] = np.where((data['proto'] == 'arp'), '-1', data['dport'])

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
            data[['stime']] = data[['stime']].astype('float64').astype('int64')

            data[['bytes']] = data[['bytes']].astype('int64')
            data[['ltime']] = data[['ltime']].astype('float64').astype('int64')
            data[['spkts']] = data[['spkts']].astype('int64')
            data[['sbytes']] = data[['sbytes']].astype('int64')

            # print(f'data info: \n{data.info()}')

            # data_complete = pd.DataFrame()
            calcule_feature(data)

            # print(f'data complete info: \n{data_complete.info()}')

            # print(f'data_complete: {data_complete}')

            # print("Almacenado")
            data2 = data.copy()
            data.drop(['saddr', 'sport', 'daddr', 'dport', 'proto', 'state'], axis=1, inplace=True)
            file = mongo.db.fs.files.find_one({'filename': 'param-standardization'})
            binary = b""
            file_chunks = mongo.db.fs.chunks.find({'files_id': file['_id']})
            for i in file_chunks:
                binary += i['data']
            scaler = pickle.loads(binary)

            # Estandarizando
            data[data.columns] = scaler.transform(data[data.columns])

            res = eureka_client.do_service("prediccion", "/model/dt", method="GET", data=data.to_json())
            print(res)
            print(type(res))
            ret = ast.literal_eval(res)

            algorithm_files = mongo.db.save_model
            for i in range(len(data2)):
                # print(data.iloc[i].to_dict())
                algorithm_files.insert_one(data2.iloc[i].to_dict())

            return ret




def calcule_feature(df_features):
    dict = df_features[['saddr', 'bytes']].groupby("saddr").sum().T.to_dict('records')
    df_features['TnBPSrcIP'] = df_features['saddr'].map(dict[0], na_action='ignore')

    dict = df_features[['daddr', 'bytes']].groupby("daddr").sum().T.to_dict('records')
    df_features['TnBPDstIP'] = df_features['daddr'].map(dict[0], na_action='ignore')

    dict = df_features[['saddr', 'pkts']].groupby("saddr").sum().T.to_dict('records')
    df_features['TnP_PSrcIP'] = df_features['saddr'].map(dict[0], na_action='ignore')

    dict = df_features[['daddr', 'pkts']].groupby("daddr").sum().T.to_dict('records')
    df_features['TnP_PDstIP'] = df_features['daddr'].map(dict[0], na_action='ignore')

    dict = df_features[['proto', 'pkts']].groupby("proto").sum().T.to_dict('records')
    df_features['TnP_PerProto'] = df_features['proto'].map(dict[0], na_action='ignore')

    dict = df_features[['dport', 'pkts']].groupby("dport").sum().T.to_dict('records')
    df_features['TnP_Per_Dport'] = df_features['dport'].map(dict[0], na_action='ignore')

    dict = df_features[['saddr', 'proto', 'pkts', 'dur']].groupby(['saddr', 'proto']).sum().reset_index()
    dict['AR_P_Proto_P_SrcIP'] = dict.pkts / dict.dur
    dict['key'] = dict.apply(lambda row: row.saddr + row.proto, axis=1)
    dict = dict[['key', 'AR_P_Proto_P_SrcIP']].set_index('key').T.to_dict('records')
    df_features['AR_P_Proto_P_SrcIP'] = df_features.apply(lambda row: row.saddr + row.proto, axis=1).map(dict[0],
                                                                                                         na_action='ignore')

    dict = df_features[['daddr', 'proto', 'pkts', 'dur']].groupby(['daddr', 'proto']).sum().reset_index()
    dict['AR_P_Proto_P_DstIP'] = dict.pkts / dict.dur
    dict['key'] = dict.apply(lambda row: row.daddr + row.proto, axis=1)
    dict = dict[['key', 'AR_P_Proto_P_DstIP']].set_index('key').T.to_dict('records')
    df_features['AR_P_Proto_P_DstIP'] = df_features.apply(lambda row: row.daddr + row.proto, axis=1).map(dict[0],
                                                                                                         na_action='ignore')

    dict = df_features.daddr.value_counts().to_dict()
    df_features['N_IN_Conn_P_DstIP'] = df_features['daddr'].map(dict, na_action='ignore')

    dict = df_features.saddr.value_counts().to_dict()
    df_features['N_IN_Conn_P_SrcIP'] = df_features['saddr'].map(dict, na_action='ignore')

    dict = df_features[['sport', 'proto', 'pkts', 'dur']].groupby(['sport', 'proto']).sum().reset_index()
    dict['AR_P_Proto_P_Sport'] = dict.pkts / dict.dur
    dict['key'] = dict.apply(lambda row: str(row.sport) + row.proto, axis=1)
    dict = dict[['key', 'AR_P_Proto_P_Sport']].set_index('key').T.to_dict('records')
    df_features['AR_P_Proto_P_Sport'] = df_features.apply(lambda row: str(row.sport) + row.proto, axis=1).map(dict[0],
                                                                                                              na_action='ignore')

    dict = df_features[['dport', 'proto', 'pkts', 'dur']].groupby(['dport', 'proto']).sum().reset_index()
    dict['AR_P_Proto_P_Dport'] = dict.pkts / dict.dur
    dict['key'] = dict.apply(lambda row: str(row.dport) + row.proto, axis=1)
    dict = dict[['key', 'AR_P_Proto_P_Dport']].set_index('key').T.to_dict('records')
    df_features['AR_P_Proto_P_Dport'] = df_features.apply(lambda row: str(row.dport) + row.proto, axis=1).map(dict[0],
                                                                                                              na_action='ignore')


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
