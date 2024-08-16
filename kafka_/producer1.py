from time import sleep
from kafka import KafkaProducer
from json import dumps

def get_producer():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: dumps(x).encode('utf-8'),
                acks = 'all'
                )
    return producer

def make_data(producer, n = 1000):
    for e in range(n):
        data = {'number' : e}
        producer.send('numtest', value=data)
        sleep(5)

def main():
    producer = get_producer()
    make_data(producer = producer)

if __name__ == '__main__':
    main()

