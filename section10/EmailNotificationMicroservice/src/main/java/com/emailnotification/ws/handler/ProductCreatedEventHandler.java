package com.emailnotification.ws.handler;

import com.example.core.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
// Explicitly link the containerFactory bean here
@KafkaListener(topics = {"product-created-events-topic"}, containerFactory = "kafkaListenerContainerFactory")
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent) {
        LOGGER.info("Received a new event: " + productCreatedEvent.getTitle());
    }

    // Fallback handler if a message still arrives parsed incorrectly as String
    @KafkaHandler(isDefault = true)
    public void handleDefault(Object unmappedMessage) {
        LOGGER.warn("Received unmapped message payload structure: " + unmappedMessage.toString());
    }
}
