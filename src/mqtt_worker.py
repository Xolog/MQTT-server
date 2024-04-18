import json

import paho.mqtt.client as mqtt

from paho.mqtt.client import Client
from .zmq_pipeline import ZmqPipelineNode


class MqttWorker:
    def __init__(self,
                 mqtt_broker: str,
                 mqtt_port: int,
                 mqtt_topic: str,
                 host_ip: str,
                 neighbors: list,
                 zmq_pipeline: ZmqPipelineNode):

        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.mqtt_topic = mqtt_topic
        self.host_ip = host_ip
        self.neighbors = neighbors

        self.zmq_pipeline = zmq_pipeline
        self.client = self.__connect_mqtt()

    def publish_neighbours(self, neighbours: list) -> None:
        """Отправляет список соседей neighbours узла брокеру"""

        self.send_message_on_mqtt({'command': 'neighbors', 'neighbors': neighbours})
        print('Отправил соседей в mqtt')

    def publish_nodes(self, nodes_dict: dict):
        self.send_message_on_mqtt({'all_nodes': nodes_dict})

    def start_listening_topic(self, listen_topic: str) -> None:
        """Запускает прослушивание топика listen_topic, проверив что он указан,
        и если приходит нужная команда - рассылает её соседям
        """

        def on_message(client, userdata, message):
            """Обработчик сообщений из MQTT"""

            payload = json.loads(message.payload.decode('utf-8'))
            if 'command' in payload and 'message' in payload:
                for neighbour in self.neighbors:
                    self.zmq_pipeline.start_sending_packet(neighbour, payload)

        self.client.on_message = on_message

        if listen_topic:
            self.client.subscribe(listen_topic)
            self.client.loop_start()
        else:
            print("Ошибка: Топик для прослушивания не указан.")

    def __connect_mqtt(self) -> Client:
        """Подключается к брокеру mqtt по его адрессу mqtt_broker и порту mqtt_port"""

        client = mqtt.Client()
        client.connect(self.mqtt_broker, self.mqtt_port)

        return client

    def send_message_on_mqtt(self, message: dict) -> None:
        """Отправляет сообщение message в виде json по топику mqtt_topic"""

        self.client.publish(self.mqtt_topic, json.dumps(message))
