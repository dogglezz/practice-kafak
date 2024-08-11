docker compose exec kafka1 kafka-console-consumer.sh --bootstrap-server kafka1:19092 --topic $1 --from-beginning
