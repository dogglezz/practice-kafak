package io.kafka.producer

import com.fasterxml.jackson.databind.ObjectMapper
import io.kafka.message.TopicMessage
import io.kafka.topics.Topics
import org.apache.kafka.common.protocol.types.Field.Str
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class PracticeTopicProducer(
    private val objectMapper: ObjectMapper,
    private val kafkaTemplate: KafkaTemplate<String, String>,
) {
    private val log = org.slf4j.LoggerFactory.getLogger(javaClass)

    fun sendMessage(message: TopicMessage) {
        log.info("send message: $message")
        kafkaTemplate.send(Topics.PRACTICE_KAFKA_TOPIC, message.id.toString(), convertMessage(message))
    }

    private fun convertMessage(message: TopicMessage): String {
        return try {
            objectMapper.writeValueAsString(message)
        } catch (e: Exception) {
            throw RuntimeException("Error converting message to JSON", e)
        }
    }
}