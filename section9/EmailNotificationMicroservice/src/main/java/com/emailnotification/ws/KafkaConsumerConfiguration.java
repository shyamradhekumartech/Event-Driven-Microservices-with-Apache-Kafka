package com.emailnotification.ws;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfiguration {

    @Autowired
    private Environment environment;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.consumer.bootstrap-server"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"));

        // Setup String Deserializer for Key
        StringDeserializer keyDeserializer = new StringDeserializer();

        // Setup Json Deserializer for Value with specific fallback target type
        JsonDeserializer<Object> valueDeserializer = new JsonDeserializer<>();
        // Tell the deserializer explicitly to trust these packages
        valueDeserializer.addTrustedPackages("com.example.core", "com.emailnotification.ws");

        // OPTIONAL CRITICAL FIX: If the producer's package structure for ProductCreatedEvent
        // does not match the consumer's package structure, explicitly map the type header like this:
        // valueDeserializer. some type mappings if packages differ...

        return new DefaultKafkaConsumerFactory<>(config, keyDeserializer, valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> containerFactory(ConsumerFactory<String, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

}
