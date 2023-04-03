from kafka import KafkaConsumer, errors
import nbformat
from datetime import datetime
import json
from typing import List, Dict
import threading
import os
from pathlib import Path
from src.color import COLOR, cprint, cstr

BACKUP_PATH = os.path.join(str(Path(__file__).parent.resolve().parent.resolve()),"backups","backup.json")

class Consumer():
    """ Netbooks consumer class. Params
        * bootstrap_servers: Kafka Server `IP:PORT` to connect
        * topics: List of topics to subscribe
        * target_nb: Jupyter notebook to updated with input messages
    """

    def __init__(self, bootstrap_servers: List[str], topics: List[str], target_nb: str) -> None:
        try:
            # Creates Kafka Consumer
            self._consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
            cprint(f"New consumer created:",COLOR.BOLD)
            [cprint(f" * Bootstrap server {i}: {server}",COLOR.OKBLUE) for i, server in enumerate(bootstrap_servers.split(","))]
            # Subscribes to topics
            self.topics = topics
            self.subscribe(topics)
            # Parses target notebook path to OS (Linux|Windows))
            self.target_nb = Path(target_nb).resolve()
            self.messages = self.load_backup()
        except errors.NoBrokersAvailable:
            cprint(f"[ERROR] Can't connect to bootstrap servers {bootstrap_servers}",COLOR.FAIL)
            cprint(f"[ERROR] Please check if Kafka is running and IP address is correct.",COLOR.FAIL)

    def subscribe(self, topics: List[str]) -> None:
        """ Subscribes consumer to indicated topics.
            * topics: list of topics to subscribe
        """
        self._consumer.subscribe(topics)
        [cprint(f" * Subscribed to topic: {t}",COLOR.OKCYAN) for t in topics]

    def _request_input(self) -> None:
        """ Ask user to perform a Notebook synchronization."""
        try:
            while True:
                # Advise message
                cprint(f"\n[#] Select Action: ",COLOR.BOLD)
                cprint(f" - Sync Notebook ([A])",COLOR.BOLD)
                cprint(f" - Subscribe to new topic (B)",COLOR.BOLD)
                answer = input().lower()
                # Proceeds to update the notebook with changes
                if answer == "a" or answer == '':
                    # Show topics with received messages
                    if len(self.messages.keys()):
                        print(f"[A1.] Messages received from topics: ")
                        [print(f" - {cstr(t,COLOR.OKCYAN)}") for t in self.messages.keys()]
                    else:
                        cprint(f"[A1.] No messages received to any topic.",COLOR.WARNING)
                    # Requests topic
                    topic = input(f"[A1.] Enter topic: \n")
                    if topic.lower() != "exit": 
                        try:
                            # Gets that topic producers
                            producers = self.messages[topic]
                            # Show that topic producers
                            if len(producers):
                                print(f"[A2.] Messages received to {cstr(topic,COLOR.OKCYAN)} from producers: ")
                                [print(f" - {cstr(p,COLOR.OKBLUE)}") for p in producers]
                            else:
                                cprint(f"[A1.] No messages received from any producer to this topic.",COLOR.WARNING)
                            # Requests producer
                            producer = input(f"[A2.] Enter producer: \n")
                            if producer.lower() != "exit":
                                if producer in producers:
                                    # Gets message from producer and topic
                                    message = self.messages[topic][producer]
                                    answer = input(f"[A3.] Is the target notebook path {cstr(self.target_nb,COLOR.WARNING)} ([Y]/n)?: ").lower()
                                    if answer.lower() != "exit": 
                                        if answer == '' or answer == "y":
                                            # Synchronizes the target notebook
                                            self.sync_nb(message,self.target_nb)
                                        elif answer == 'n':
                                            # New  target notebook path
                                            target_nb = input(f"[A3.1] Enter the target notebook path: ")
                                            self.sync_nb(message,target_nb)
                                else:
                                    cprint(f"[Error] No message from {producer} to {topic}",COLOR.FAIL)
                        except KeyError:
                            cprint(f"[Error] No messages from topic {topic}",COLOR.FAIL)
                elif answer == "b":
                    topic = input(f"[B.1] Enter new topic: \n")
                    if topic.lower() != "exit":
                        if topic not in self.topics:
                            # Subscribes to the new topic
                            self._consumer.subscribe(topic)
                            self.topics.append(topic)
                            print(f"[B] {cstr('Successfully',COLOR.BOLD)} subscribed to topic {cstr(topic,COLOR.OKCYAN)}")
                        else:
                            print(f"[B] {cstr('Already',COLOR.BOLD)} subscribed to topic {cstr(topic,COLOR.OKCYAN)}")

        except EOFError:
            self.__del__()      

    def run(self) -> None:
        """ Awaits for input messages produced to the consumer's interest topics. """
        try:
            assert self._consumer
            self._threads = [threading.Thread(target=self._run)]
            self._threads.append(threading.Thread(target=self._request_input))
            [thread.start() for thread in self._threads]
        except AttributeError:
            pass
        
    def _run(self) -> None:
        self._consumer.poll(timeout_ms=2000)
        # Awaits for messages
        for msg in self._consumer:
            # Message datetime
            msg_time = datetime.fromtimestamp(int(msg.timestamp/100)).strftime('%H:%M:%S')
            # Message info
            topic = msg.topic # Message topic
            producer = msg.value['producer'] # Message producer
            message = msg.value["message"] # Message value
            # print(cstr(f"[{msg_time}] New message from {cstr(msg.value['producer'],COLOR.WARNING)}"{to ",COLOR.OKBLUE)}{cstr(msg.topic,COLOR.OKCYAN)}",COLOR.OKBLUE)
            print(f"[{msg_time}] New message from {cstr(producer,COLOR.OKBLUE)} to topic {cstr(topic,COLOR.OKCYAN)}")
            # Saves the message (overwrites oldest message from producer at that topic)
            try:
                self.messages[topic][producer] = message
            except KeyError:
                self.messages[topic] = {producer: message}
            # Updates backup
            self.update_backup()

    def __del__(self):
        try:
            [thread.interrupt_main() for thread in self._threads]
        except AttributeError:
            pass

    def sync_nb(self, message: str, target_nb: str) -> None:
        # Code cells
        cells = message
        # Converts dict to list in case is a one cell message
        if type(cells) is dict:
            cells = [cells]
        try:
            # Reads target nb
            with open(target_nb, 'r') as f:
                nb = nbformat.read(f, as_version=4)
            # Split markdown cell
            nb['cells'].append(nbformat.v4.new_markdown_cell("---"))
            # Appends the new cells to the notebook
            for c in cells:
                if c['cell_type'] == 'code':
                    new_cell = nbformat.v4.new_code_cell(c["source"])
                elif c['cell_type'] == 'markdown':
                    new_cell = nbformat.v4.new_markdown_cell(c["source"])
                # Inserta la nueva celda de código en la posición deseada en la lista de celdas
                nb['cells'].append(new_cell)
            # Saves updated notebook
            with open(target_nb, 'w') as f:
                nbformat.write(nb, f)
            # Output succeed message
            cprint("[$] Notebook successfully updated.\n",COLOR.OKGREEN)
        except FileNotFoundError:
            cprint(f"[Error] Could not found file {target_nb}",COLOR.FAIL)
    
    def load_backup(self) -> Dict[str, Dict[str, List[str]]]:
        """ Reads and returns the backup of messages."""
        #Retrieves backup data messages
        with open(BACKUP_PATH,'r+') as f:
            backup_data = json.load(f)
        return backup_data

    def update_backup(self) -> None:
        """ Updates the backup with the new messages received. 
            * messages: New messages to include in the backup
        """
        # Loads backup
        backup_data = self.load_backup()
        backup_data.update(self.messages)
        # Saves backup
        with open(BACKUP_PATH, 'w') as f:
            json.dump(backup_data, f)