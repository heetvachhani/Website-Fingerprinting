first run zookeeper serer and kafka server.
Then run kafka consumer from KafkaNaiveBayes directory using following command
sbt "run <zookeeper-server-address> <group-name> <topic-name> <num_of_threads=2> mymodelall"

Then run kafka producer from KafkaProducer directory using following command
sbt "run <broker-address> <path-to-training-data-directory> <topic-name>"

Kafka Consumer will print predictions and accuracy, error values on terminal.
