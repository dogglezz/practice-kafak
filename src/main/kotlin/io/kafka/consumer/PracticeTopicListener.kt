package io.kafka.consumer

import io.kafka.topics.Topics
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class PracticeTopicListener {

    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        topics = [Topics.PRACTICE_KAFKA_TOPIC],
        groupId = "practice-kafka-listener"
    )
    fun practiceTopicListener(message: String) {
        log.info("Received message: $message")
    }
}