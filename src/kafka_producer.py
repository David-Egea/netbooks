from kafka import KafkaProducer
from typing import List
import nbformat
from src.color import COLOR, cprint, cstr
import json

class Producer:
    """ Netbooks producer class. Params
        * bootstrap_servers: Kafka Server IP+PORT to connect
        * PRODUCER_ID: Producer name to be identified
    """

    def __init__(self, bootstrap_servers: List[str], PRODUCER_ID: str) -> None:
        # Creates Kafka Producer
        self._producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        cprint(f"New producer created:",COLOR.BOLD)
        self.PRODUCER_ID = PRODUCER_ID
        cprint(f" * PRODUCER ID:",COLOR.HEADER)
        [cprint(f" * Bootstrap server {i}: {server}",COLOR.OKBLUE) for i, server in enumerate(bootstrap_servers.split(","))]

    def publish_notebook(self, topic: str, source_nb: str, cells: List[int], cell_types: List[str] = ['code','markdown']) -> None:
        """
            Sends source notebook cells to the `topic` as a message.
                * topic: Topic to publish the message
                * source_nb: Path of Jupyter notebook to publish
                * cells: indexes of cells to publish
                * cell_types: Types of the output cells 
        """
        try:
            # Reads source nb
            with open(source_nb, 'r') as f:
                nb = nbformat.read(f, as_version=4)
            # All notebook cells
            cells = nb['cells']
            # Filters cells by type
            cells = [c for c in cells if c["cell_type"] in cell_types]
            # Creates
            message = {'producer': self.PRODUCER_ID, "message": cells}
            self._producer.send(topic, json.dumps(message).encode('utf-64'))
            print(f"[{cstr(self.PRODUCER_ID,COLOR.OKBLUE)}] Message successfully sent to {cstr(topic,COLOR.OKCYAN)}")
        except FileNotFoundError:
            cprint(f"[ERROR] Source notebook found at {source_nb}", COLOR.FAIL)