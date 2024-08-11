package io.kafka.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ContainerProperties


@EnableKafka
@Configuration
class KafkaConfig {

    @Bean
    @Primary
    fun consumerFactory(kafkaProperties: KafkaProperties): ConsumerFactory<String, Any> {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProperties.bootstrapServers
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = kafkaProperties.consumer.keyDeserializer
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = kafkaProperties.consumer.valueDeserializer
        props[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = "false"
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    @Primary
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String?, Any?>,
    ): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = consumerFactory
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        factory.setConcurrency(1)

        return factory
    }

    @Bean
    @Qualifier("batchConsumerFactory")
    fun batchConsumerFactory(kafkaProperties: KafkaProperties): ConsumerFactory<String, Any> {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProperties.bootstrapServers
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = kafkaProperties.consumer.keyDeserializer
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = kafkaProperties.consumer.valueDeserializer
        props[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = "false"
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = ConsumerConfig.DEFAULT_MAX_POLL_RECORDS
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    @Qualifier("batchKafkaListenerContainerFactory")
    fun batchKafkaListenerContainerFactory(
        @Qualifier("batchConsumerFactory") batchConsumerFactory: ConsumerFactory<String?, Any?>,
    ): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = batchConsumerFactory
        factory.isBatchListener = true
        factory.setConcurrency(1)
        factory.containerProperties.ackMode = ContainerProperties.AckMode.BATCH
        return factory
    }

    @Bean
    @Primary
    fun producerFactory(kafkaProperties: KafkaProperties): ProducerFactory<String, Any> {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProperties.bootstrapServers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = kafkaProperties.producer.keySerializer
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = kafkaProperties.producer.valueSerializer
        props[ProducerConfig.ACKS_CONFIG] = kafkaProperties.producer.acks
        props[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = "true"
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    @Primary
    fun kafkaTemplate(kafkaProperties: KafkaProperties): KafkaTemplate<String, *> {
        return KafkaTemplate(producerFactory(kafkaProperties))
    }
}