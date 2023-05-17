from src.kafka_consumer import Consumer

myconsumer = Consumer(
    bootstrap_servers = '34.175.78.32:9092',
    topics = ["ejercicio-1", "ejercicio-2"], 
    target_nb = "notebookB.ipynb"
)

myconsumer.run()