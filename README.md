# Netbooks

> A ready-to-use Kafka system for online 

---
## How to install
Clone the repository code from Github.
```bash
git clone https://github.com/David-Egea/netbooks.git
```

Build, start and run the containers declared in `docker-compose.yml`. 
```bash
cd netbooks
docker-compose up
```
If the containers are stopped or exited, execute the previous command  to run them again.

In order to run this project requires other additional python libraries. These dependencies are included at `requirements.txt`. To install the requirements:
```bash
pip install -r requirements.txt
```

---
## How to use

Open a new terminal and execute the command...