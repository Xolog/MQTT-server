from src.node import Node
import sys
sys.path.append(f'{sys.path[0]}/src')

if __name__ == '__main__':
    worker = Node()
    worker.start_work()
