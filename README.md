
# Project: 🤖 

Authors: **Lohin Yurii, Bronetskyi Volodymyr**


## 🖥 Usage

### How to run the application

1. Clone the repository
1. `docker compose up -d` is running:
    - Kafka with Zookeeper
        + creates topic
    - Spark cluster (master and worker)
    - Cassandra node
        + runs DDL script with tables
    - wiki producer
    - rest api

1. To run Spark Streaming programs:
    - Run a python app with script `./scripts/run.sh`. Don't forget to give a permition and change path in script. 
        - here is command which reads data from wiki endpiont
        - and another one which parse row data with Spark
1. To stop a container of Spark with `docker compose down`


### Results

<mark>Design document and diagrams are described here:

https://docs.google.com/document/d/1RiTMuGNhQAimIPycb1w6-eJBI7aehcoXsmjsjyfDgOg/edit?usp=sharing
