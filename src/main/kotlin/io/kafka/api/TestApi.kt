package io.kafka.api

import io.kafka.message.TopicMessage
import io.kafka.producer.PracticeTopicProducer
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api")
class TestApi(
    private val topicProducer: PracticeTopicProducer,
) {
    @PostMapping("/message")
    fun sendMessage(@RequestBody request: MessageRequest) {
        topicProducer.sendMessage(TopicMessage(request.id, request.message))
    }
}