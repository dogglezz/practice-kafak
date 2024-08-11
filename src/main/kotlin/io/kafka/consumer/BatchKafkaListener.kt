package io.kafka.consumer

import io.kafka.topics.Topics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class BatchKafkaListener {

    private val log = LoggerFactory.getLogger(javaClass)


    @KafkaListener(
        topics = [Topics.PRACTICE_KAFKA_TOPIC],
        groupId = "batch-kafka-listener"
    )
    fun batchTopicListener(messages: List<ConsumerRecord<String, String>>) {
        messages.forEach {
            log.info("Received message: ${it.value()}")
            log.info("partition: ${it.partition()} offset: ${it.offset()}")
        }
    }

}