from confluent_kafka import Producer, Consumer, KafkaError
import json

#изменения для гита

while True:
    # Конфигурация продюсера
    conf = {
        'bootstrap.servers': '192.168.0.112:9092',
        'client.id': 'python-producer-01',
        'broker.address.family': 'v4'
    }

    producer = Producer(conf)

    # Функция обратного вызова для обработки результата доставки
    def delivery_report(err, msg):
        if err is not None:
            print(f'Ошибка доставки: {err}')
        else:
            print(f'Сообщение доставлено в {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

    # Подготовка сообщения (значение должно быть байтами)
    message = {"order_id": "ORD-999", "amount": 99.99}
    value_bytes = json.dumps(message).encode('utf-8')

    # Асинхронная отправка сообщения
    producer.produce(
        partition=0,
        topic='orders',
        key='ORD-999',  # Ключ помогает определить партицию
        value=value_bytes,
        callback=delivery_report  # Вызовется при подтверждении или ошибке
    )

    # flush() блокирует выполнение до тех пор, пока все сообщения не будут доставлены
    print("Ожидание доставки...")
    producer.flush()
    print("Готово.")

    conf = {
        'bootstrap.servers': '192.168.0.112:9092',
        'client.id': 'python-producer-02',
        'broker.address.family': 'v4'
    }

    producer = Producer(conf)


    # Функция обратного вызова для обработки результата доставки
    def delivery_report(err, msg):
        if err is not None:
            print(f'Ошибка доставки: {err}')
        else:
            print(f'Сообщение доставлено в {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')


    # Подготовка сообщения (значение должно быть байтами)
    message = {"order_id": "ORD-999", "amount": 99.99}
    value_bytes = json.dumps(message).encode('utf-8')

    # Асинхронная отправка сообщения
    producer.produce(
        partition=1,
        topic='orders',
        key='ORD',  # Ключ помогает определить партицию
        value=value_bytes,
        callback=delivery_report  # Вызовется при подтверждении или ошибке
    )

    # flush() блокирует выполнение до тех пор, пока все сообщения не будут доставлены
    print("Ожидание доставки...")
    producer.flush()
    print("Готово.")