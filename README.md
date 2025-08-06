### Kafka Setup
After running docker-compose up -d, run these commands:

docker exec -it kafka kafka-topics.sh \
  --create --topic orders --bootstrap-server localhost:9092 --partitions 8 --replication-factor 1

docker exec -it kafka kafka-topics.sh \
  --create --topic match-events --bootstrap-server localhost:9092 --partitions 8 --replication-factor 1

docker exec -it kafka kafka-topics.sh \
  --create --topic control-plane --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

This will create our topics. For now, we are using 8 partitions for order matching. Orders is the ingestion channel, which is where incoming orders get sent by the producer. Then, after a matching engine executes a trade we write a match event to the match-events topic. This produces a message which is a log of what order was matched. We also want another partition as a side-channel to broadcast cross-partition commands.