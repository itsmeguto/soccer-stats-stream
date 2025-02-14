# soccer-stats-stream

-- safe code:
Invoke-Expression (&starship init powershell)
#pip install -r requirements.txt

docker exec -it cranky_newton /opt/kafka/bin/kafka-topics.sh --create --topic epl-matches --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

opt/kafka/bin/kafka-topics.sh --create --topic epl_matches --bootstrap-server localhost:9092
docker exec -it cranky_newton /opt/kafka/bin/kafka-topics.sh --version

# to find kafka path

on Mac OS _ ❯ docker exec -it cranky_newton /bin/sh                                                                                                             
/ # find / -name "*kafka-topics*" 2>/dev/null
/opt/kafka_2.12-2.3.0/bin/kafka-topics.sh
/opt/kafka_2.12-2.3.0/bin/windows/kafka-topics.bat

# create topic
docker exec -it cranky_newton /opt/kafka_2.12-2.3.0/bin/kafka-topics.sh --create --topic epl-matches --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1