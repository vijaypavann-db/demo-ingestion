# with an unencrypted plaintext connection
kafka_bootstrap_servers_plaintext = "b-1.oetrta-kafka.XXXX.XX.kafka.us-west-2.amazonaws.com:9092,b-2.oetrta-kafka.XXXX.XX.kafka.us-west-2.amazonaws.com:9092"
topic = "user_oetrta_kafka_test"
startingOffsets = "earliest"
checkpoint_location = "/home/<USER>>@databricks.com/oetrta/temp/load_kafka"


# You can connect to Kafka over either SSL/TLS encrypted connection
kafka_bootstrap_servers_plaintext = "b-1.oetrta-kafka.XXXX.XX.kafka.us-west-2.amazonaws.com:9094,b-2.oetrta-kafka.XXXX.XX.kafka.us-west-2.amazonaws.com:9094"
topic = "user_oetrta_kafka_test"
startingOffsets = "earliest"
checkpoint_location = "/home/<USER>@databricks.com/oetrta/temp/load_kafka"