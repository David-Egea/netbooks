from src.kafka_consumer import Consumer

myconsumer = Consumer(
    bootstrap_servers = 'localhost:9092',
    topics = ["ejercicio-1", "ejercicio-2"], 
    target_nb = "C:\\Users\\egeah\\netbooks\\otra_practica.ipynb"
)

myconsumer.run()