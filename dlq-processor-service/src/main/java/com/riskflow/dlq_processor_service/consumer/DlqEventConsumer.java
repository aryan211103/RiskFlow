package com.riskflow.dlq_processor_service.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.riskflow.dlq_processor_service.service.DlqProcessorService;

// Kafka consumer that listens on the DLQ topic.
// Every message here represents a transaction the scoring service
// failed to process. This consumer extracts the relevant fields
// and hands off to DlqProcessorService for classification and routing.
@Component
public class DlqEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqEventConsumer.class);

    private final DlqProcessorService dlqProcessorService;

    public DlqEventConsumer(DlqProcessorService dlqProcessorService) {
        this.dlqProcessorService = dlqProcessorService;
    }

    // @KafkaListener tells Spring to call this method automatically
    // whenever a message arrives on transaction.scoring.failed.
    // The group-id matches what we set in application.properties.
    @KafkaListener(
        topics = "transaction.scoring.failed",
        groupId = "dlq-processor-service"
    )
    public void consume(
        // @Payload is the message value — the original pipe-delimited payload
        @Payload String payload,
        // The Kafka message key — this is the transactionId set by the scoring service
        @Header(KafkaHeaders.RECEIVED_KEY) String transactionId,
        // Custom header set by the scoring service containing the error message
        // We use getOrDefault pattern — if header is missing we use a fallback
        @Header(value = "error-message", required = false) String errorMessage
    ) {
        log.info("DlqEventConsumer: received failed event txnId={}", transactionId);

        // Use a fallback if the error header was not set
        String resolvedError = (errorMessage != null) ? errorMessage : "Unknown error";

        dlqProcessorService.process(transactionId, payload, resolvedError);
    }
}