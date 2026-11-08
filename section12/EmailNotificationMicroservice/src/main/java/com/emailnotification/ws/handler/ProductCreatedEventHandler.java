package com.emailnotification.ws.handler;

import com.emailnotification.ws.error.NotRetryableException;
import com.example.core.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = {"product-created-events-topic"})
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent) {
        // due to some business logic, got an error like below
        if (true)
            throw new NotRetryableException("An error took place, no need to consume this message again!");

        LOGGER.info("Received a new event: " + productCreatedEvent.getTitle());
    }

}
