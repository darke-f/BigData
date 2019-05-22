from time import sleep
from json import dumps
from kafka import KafkaProducer
import os
import pandas as pd

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
    
dataset_folder_path = os.path.join(os.path.dirname(os.getcwd()), 'Dataset')
dataset_file_path = os.path.join(dataset_folder_path, 'data.csv')

with open(dataset_file_path,"r", encoding="utf-8") as f:
    for row in f:
        producer.send('netflixdata', value=row)
        print(row)
        sleep(0.005)