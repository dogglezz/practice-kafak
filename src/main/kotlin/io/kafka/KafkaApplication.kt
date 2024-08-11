package io.kafka

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@EnableConfigurationProperties
@SpringBootApplication
class KafkaApplication

fun main(args: Array<String>) {
    runApplication<KafkaApplication>(*args)
}
