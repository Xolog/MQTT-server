import sys
import time
import threading

from loguru import logger

from .mqtt_worker import MqttWorker
from .zmq_pipeline import ZmqPipelineNode
from .node_monitor import NodeMonitor
from .utils import get_node_id_from_addr, get_my_ip, sleeping

logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="DEBUG")


class Node:
    def __init__(self):
        logger.info('Старт инициализации')

        self._my_ip: str = get_my_ip()
        self._ping_port: int = 80
        self._zmq_port: int = 5566
        self._mqtt_broker_port: int = 1883
        self._mqtt_broker_host: str = 'localhost'

        self._neighbors_monitor = NodeMonitor(get_node_id_from_addr(addr=self._my_ip), self._ping_port, self._my_ip)
        self._neighbors: list = self.__get_neighbors()
        self.zmq_pipeline = ZmqPipelineNode(self._zmq_port, self._my_ip, self._neighbors)
        self.mqtt_worker = MqttWorker(mqtt_broker=self._mqtt_broker_host, mqtt_port=self._mqtt_broker_port,
                                      mqtt_topic='/leader/core', host_ip=self._my_ip,
                                      neighbors=self._neighbors, zmq_pipeline=self.zmq_pipeline
                                      )

    def start_work(self):
        my_node = get_node_id_from_addr(addr=self._my_ip)
        logger.info(f'Node: {my_node}')

        self._neighbors_monitor.mqtt = self.mqtt_worker
        threading.Thread(target=self.zmq_pipeline.handle_received_packet, args=(self.mqtt_worker,)).start()

        self.mqtt_worker.start_listening_topic(listen_topic='/leader/network')
        self.mqtt_worker.publish_neighbours(self._neighbors)

        for neighbour in self._neighbors:
            self.zmq_pipeline.start_sending_packet(addressee=neighbour,
                                                   packet={'nodes_in_network': [my_node]})

        all_nodes = self.zmq_pipeline.get_all_node()
        while my_node not in all_nodes:
            sleeping(True)
            all_nodes = self.zmq_pipeline.get_all_node()

        self.mqtt_worker.publish_nodes(all_nodes)

        logger.info('Инициализация завершена. Начало работы...')

        self.monitor_network()

    def monitor_network(self) -> None:
        while True:
            if not self._neighbors:
                print('Нет соседей. Поиск...')
                self._neighbors = self._neighbors_monitor.init_neighbors()
                self.mqtt_worker.publish_neighbours(self._neighbors)
            else:
                gone_neighbors = self.get_gone_neighbors()
                new_neighbors = self._neighbors_monitor.get_neighbors()

                if new_neighbors != self._neighbors and not gone_neighbors:
                    # если соседи изменились, но при прошлой проверке никто не упал проверить ещё раз
                    gone_neighbors = self.get_gone_neighbors()

                # проверка вернувшихся соседей
                back_neighbors = ()
                if new_neighbors != 0 and new_neighbors != self._neighbors:
                    back_neighbors = self.get_back_neighbors(new_neighbors)

                if back_neighbors:
                    self.__send_event_message(event='neighbour_back', event_neighbors=back_neighbors)

                self.__set_neighbors(new_neighbors, gone_neighbors, back_neighbors)

                if gone_neighbors:
                    self.__send_event_message(event='neighbour_gone', event_neighbors=gone_neighbors)

            time.sleep(5)

    def get_gone_neighbors(self) -> tuple:
        gone_neighbors = self._neighbors_monitor.check_gone_neighbors()

        return gone_neighbors

    def get_back_neighbors(self, new_neighbors: list) -> tuple:
        back_neighbors = self._neighbors_monitor.check_back_neighbors(new_neighbors, self.zmq_pipeline.get_all_node())

        return back_neighbors

    def __get_neighbors(self) -> list:
        return self._neighbors_monitor.init_neighbors()

    def __set_neighbors(self, neighbors: list, gone_neighbors: tuple, back_neighbors: tuple) -> None:
        self._neighbors = neighbors
        self._neighbors_monitor.set_neighbors(self._neighbors)

        for neighbour in gone_neighbors:
            self.zmq_pipeline.nodes_dict.pop(get_node_id_from_addr(neighbour), None)

        if back_neighbors:
            nodes = [get_node_id_from_addr(neighbour) for neighbour in back_neighbors]
            self.zmq_pipeline.set_nodes_dict(nodes)

        self.mqtt_worker.neighbors = self._neighbors

    def __send_event_message(self, event: str, event_neighbors: tuple) -> None:
        for event_neighbor in event_neighbors:
            message = {'event': event,
                       'neighbour_ip': event_neighbor,
                       'neighbour_node': get_node_id_from_addr(event_neighbor)}

            for neighbour in self._neighbors:
                self.zmq_pipeline.start_sending_packet(neighbour, message)

            self.mqtt_worker.send_message_on_mqtt(message)
