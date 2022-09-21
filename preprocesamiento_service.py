import json

from configparser import ConfigParser
from confluent_kafka import Producer, Consumer
import main

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
producer_config = dict(config_parser['kafka_client'])
consumer_config = dict(config_parser['kafka_client'])
consumer_config.update(config_parser['consumer'])
preprocessing_producer = Producer(producer_config)


def data_suscription():
    preprocessing_consumer = Consumer(consumer_config)
    preprocessing_consumer.subscribe(['data-prediction'])
    while True:
        event = preprocessing_consumer.poll(1.0)
        if event is None:
            pass
        elif event.error():
            print(f'Bummer - {event.error()}')
        else:
            prediction = json.loads(event.value())
            print(f'producto: {prediction}')
            # main.order_warmer = product


async def data_publish(data):
    preprocessing_producer.produce('data-preprocessing', value=data.to_json())
    preprocessing_producer.flush()
    while main.received_data is None:
        var = None
    received_data = main.received_data.copy()
    main.received_data = None
    return received_data

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
    return df_features

