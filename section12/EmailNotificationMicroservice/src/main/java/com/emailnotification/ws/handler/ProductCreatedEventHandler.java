package com.emailnotification.ws.handler;

import com.emailnotification.ws.error.NotRetryableException;
import com.emailnotification.ws.error.RetryableException;
import com.example.core.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = {"product-created-events-topic"})
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private final RestTemplate restTemplate;

    public ProductCreatedEventHandler(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent) {
        // due to some business logic, got an error like below
        if (false)
            throw new NotRetryableException("An error took place, no need to consume this message again!");

        LOGGER.info("Received a new event: " + productCreatedEvent.getTitle());

        // handle retryable and not retryable exception together, if making external call failed
        if (true) {
            String requestUrl = "http://localhost:8082";
            try {
                ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);

                if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                    LOGGER.info("Received response from a remote service: " + response.getBody());
                }
            } catch (ResourceAccessException e) {
                LOGGER.error(e.getMessage());
                throw new RetryableException(e);
            } catch (HttpServerErrorException e) {
                LOGGER.error(e.getMessage());
                throw new NotRetryableException(e);
            }  catch (Exception e) {
                LOGGER.error(e.getMessage());
                throw new NotRetryableException(e);
            }
        }
    }

}
