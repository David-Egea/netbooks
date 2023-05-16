from kafka import KafkaProducer, errors
from typing import List
import nbformat
from src.color import COLOR, cprint, cstr
import json
from pathlib import Path

class Producer:
    """ Netbooks producer class. Params
        * bootstrap_servers: Kafka Server `IP:PORT` to connect
        * PRODUCER_ID: Producer name to be identified
    """

    def __init__(self, bootstrap_servers: List[str], PRODUCER_ID: str) -> None:
        try:
            # Creates Kafka Producer
            self._producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            cprint(f"New producer created:",COLOR.BOLD)
            self.PRODUCER_ID = PRODUCER_ID
            cprint(f" * PRODUCER ID: {PRODUCER_ID}",COLOR.HEADER)
            [cprint(f" * Bootstrap server {i}: {server}",COLOR.OKBLUE) for i, server in enumerate(bootstrap_servers.split(","))]
        except errors.NoBrokersAvailable:
            cprint(f"[ERROR] Can't connect to bootstrap servers {bootstrap_servers}",COLOR.FAIL)
            cprint(f"[ERROR] Please check if Kafka is running and IP address is correct.",COLOR.FAIL)

    def publish_notebook(self, topics: List[str], source_nb: str, cell_types: List[str] = ['code','markdown'], first_cell: int = None, last_cell: int = None) -> None:
        """
            Sends source notebook cells to the `topic` as a message. 
                * topics: List of topics to publish the message
                * source_nb: Path of Jupyter notebook to publish
                * cells: indexes of cells to publish
                * cell_types: Types of the output cells 
                * first_cell: First cell to publish
                * last_cell: Last cell to publish
        """
        try:
            if type(topics) == str:
                topics = [topics]
            # Parses source notebook path to OS (Linux|Windows)
            source_nb = Path(source_nb).resolve()
            # Reads source nb
            with open(source_nb, 'r') as f:
                nb = nbformat.read(f, as_version=4)
            # All notebook cells
            cells = nb['cells']
            # Gets indicated cells
            if first_cell is not None and last_cell is not None:
                # First and last cell defined
                cells = cells[first_cell:last_cell+1]
            elif first_cell is not None:
                # Last cell not defined (from first cell to end)
                cells = cells[first_cell-1:]
            elif last_cell is not None:
                # First cell not defined (from start to last cell)
                cells = cells[:last_cell+1]
            # Filters cells by type
            cells = [c for c in cells if c["cell_type"] in cell_types]
            # Creates
            message = {'producer': self.PRODUCER_ID, "message": cells}
            self._producer.send("main", json.dumps(message).encode('utf-8')) # sends by default to main topic
            [self._producer.send(topic, json.dumps(message).encode('utf-8')) for topic in topics]
            print(f"[{cstr(self.PRODUCER_ID,COLOR.OKBLUE)}] Message successfully sent to topics: ")
            [print(f"- {cstr(topic,COLOR.OKCYAN)}") for topic in topics]
        except FileNotFoundError:
            cprint(f"[ERROR] Source notebook found at {source_nb}", COLOR.FAIL)