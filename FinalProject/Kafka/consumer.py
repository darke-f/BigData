from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import os

consumer = KafkaConsumer(
    'netflixdata',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

folder_path = os.path.join(os.path.dirname(os.getcwd()), 'Kafka_Data')
batch_limit = 500000
batch_counter = 0
batch_number = 0

try:
    while True:
        for message in consumer:
            if batch_counter >= batch_limit:
                batch_counter = 0
                batch_number += 1
                writefile.close()
            if batch_counter == 0:
                file_path = os.path.join(folder_path, ('data_part_' + str(batch_number) + '.txt'))
                writefile = open(file_path, "w", encoding="utf-8")
            message = message.value
            writefile.write(message)
            batch_counter += 1
            print('current batch : ' + str(batch_number) + ' current data for this batch : ' + str(batch_counter))
except KeyboardInterrupt:
    writefile.close()
print('Keyboard Interrupt called by user, exiting.....')