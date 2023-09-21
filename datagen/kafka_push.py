from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
from data_generator import RandomDataGenerator
import json

def make_producer(host):
    producer = KafkaProducer(bootstrap_servers=[host], value_serializer=lambda x: json.dumps(x).encode("utf-8"))
    return producer

def insert_mongo(document):
    client = MongoClient("mongodb://localhost:27018")
    database = client["mongo_datalake"]
    database["Users"].insert_one(document)

def send_random(producer):
    for msg in RandomDataGenerator():
        producer.send("probando", msg)
        print("Sent")

def main():
    producer = make_producer(host="kafka:9092")
    send_random(producer)
    data = {
        "FirstName": "Alejandro",
        "LastName": "Gonzalez",
        "Age": 40,
        "City": "Santiago",
    }
    insert_mongo(data)
    print("Documento insertado")

if __name__ == "__main__":
    main()