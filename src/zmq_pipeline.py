import sys

import zmq
from zmq import Socket
from loguru import logger
from typing import NamedTuple
# from dataclasses import dataclass

from .utils import NeighbourChecker as checker
from .utils import get_node_id_from_addr, sleeping, get_unix_time

waiting = False

logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="INFO")


class ZmqPipelineNode:
    def __init__(self,
                 zmq_port: int,
                 host_ip: str,
                 neighbors: list
                 ):

        self.zmq_port = zmq_port
        self.host_ip = host_ip
        self.context = zmq.Context()
        self.consumer_receiver_socket = None
        self.nodes_dict: dict = {}
        self.neighbors = neighbors

    # @dataclass(frozen=True)
    # class ERAPMessage:
    #     """Extended Ring Applied Protocol Message - класс для задания структуры сообщений"""
    #     source: str
    #     sender_neighbour: str
    #     addressee: str
    #     command: str
    #     message: NamedTuple

    class SenderNode(NamedTuple):
        address: str
        live_time: int

    def handle_received_packet(self, mqtt) -> None:
        self.consumer_receiver_socket = self.__create_receiver_socket()
        while True:
            try:
                packet = self.__receive_response(self.consumer_receiver_socket)
            except zmq.error.ZMQError:
                pass
            else:
                my_node = get_node_id_from_addr(self.host_ip)
                if packet.get('sender_node') != my_node:
                    # если передаём список своих адресов друг другу
                    nodes = packet.get('nodes_in_network')
                    if nodes:
                        if my_node not in nodes:
                            nodes.append(my_node)
                        packet.update({'nodes_in_network': nodes})
                        self.set_nodes_dict(nodes)

                    addressee = self.__identify_addressee(packet, my_node)
                    consumer_sender_socket = self.__create_sender_sockets(addressee)

                    sleeping(waiting)
                    if packet.get('command'):
                        mqtt.send_message_on_mqtt(packet)
                    self.__send_packet(consumer_sender_socket, addressee, packet)
                else:
                    nodes = packet.get('nodes_in_network')
                    if nodes:
                        self.set_nodes_dict(nodes)
                    sleeping(waiting)
                    logger.info(f'Получил:\n{packet}\n')

    def start_sending_packet(self, addressee: str, packet: dict):
        consumer_sender_socket = self.__create_sender_sockets(addressee)
        packet.update({'sender_node': get_node_id_from_addr(self.host_ip)})

        sleeping(waiting)
        self.__send_packet(consumer_sender_socket, addressee, packet)

        logger.info(f'Отправлено:\n{packet}\n')
        consumer_sender_socket.close()

    def close_sockets(self):
        self.consumer_receiver_socket.close()

    def __create_receiver_socket(self) -> Socket:
        # для получения
        consumer_receiver_socket = self.context.socket(zmq.PULL)
        consumer_receiver_socket.bind(f'tcp://{self.host_ip}:{self.zmq_port}')

        return consumer_receiver_socket

    def __create_sender_sockets(self, addressee: str) -> Socket:
        # для отправления
        consumer_sender_socket = self.context.socket(zmq.PUSH)
        consumer_sender_socket.connect(f'tcp://{addressee}:{self.zmq_port}')

        return consumer_sender_socket

    def __send_packet(self, socket: Socket, neighbour: str, packet) -> None:
        self.__change_packet(neighbour, packet)
        if packet.get('sender_node') != get_node_id_from_addr(self.host_ip):
            logger.debug(f'Получил и отправил дальше:\n{packet}\n')
        socket.send_json(packet)

    @staticmethod
    def __receive_response(socket: Socket) -> dict:
        return socket.recv_json()

    def __identify_addressee(self, packet: dict, my_node: int) -> str:
        from_node = get_node_id_from_addr(packet.get('from'))
        addressed_node = None
        self.nodes_dict = self.__sorted_dict(self.nodes_dict)
        nodes = tuple(self.nodes_dict.keys())

        if from_node in nodes:
            if len(nodes) > 2:
                if not checker.is_node_outermost(node_id=my_node, nodes=nodes):
                    if from_node > my_node:
                        addressed_node = nodes[nodes.index(my_node) - 1]
                    elif from_node < my_node:
                        addressed_node = nodes[nodes.index(my_node) + 1]
                else:
                    if checker.is_node_first(node_id=my_node, nodes=nodes):

                        if from_node == nodes[-1]:
                            addressed_node = nodes[1]
                        else:
                            addressed_node = nodes[-1]
                    elif checker.is_node_last(node_id=my_node, nodes=nodes):
                        if from_node == nodes[0]:
                            addressed_node = nodes[-2]
                        else:
                            addressed_node = nodes[0]
            elif len(nodes) == 2:
                if my_node in nodes:
                    return packet.get('from')
                else:
                    for node in nodes:
                        if node != from_node:
                            addressed_node = node
            else:
                addressed_node = nodes[0]

            return f'10.20.{addressed_node}.1'

        elif packet.get('from') in self.neighbors:
            for neighbour in self.neighbors:
                if neighbour != packet.get('from'):
                    return packet.get('from')

    def __change_packet(self, addressee: str, packet: dict):
        return packet.update({'from': self.host_ip, 'to': addressee})

    # --------------------получение всех узлов--------------------
    def get_all_node(self) -> dict:
        self.nodes_dict = self.__sorted_dict(self.nodes_dict)

        return self.nodes_dict

    @staticmethod
    def __sorted_dict(unsorted_dict: dict) -> dict:
        return dict(sorted(unsorted_dict.items(), key=lambda x: x[0]))

    def set_nodes_dict(self, nodes: list):
        """Проходим по пакету и если там есть ключ sender_address, то добавляем номер узла и значение sender_address в очередь"""

        for node in nodes:
            self.nodes_dict.update({
                node: self.SenderNode(
                    address=f'10.20.{node}.1',
                    live_time=get_unix_time()
                )
            })
