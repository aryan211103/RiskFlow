package com.riskflow.dlq_processor_service.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.riskflow.dlq_processor_service.model.FailedEvent;
import com.riskflow.dlq_processor_service.model.FailedEventStatus;

// Standard Spring Data repository for FailedEvent.
// Gives us save(), findById(), delete() etc for free.
@Repository
public interface FailedEventRepository extends JpaRepository<FailedEvent, Long> {

    // Used by the DLQ processor to find all events that still need
    // to be retried or classified.
    // Translates to: SELECT * FROM failed_events WHERE status = ?
    List<FailedEvent> findByStatus(FailedEventStatus status);

    // Used to check if we have already recorded this transaction as failed.
    // Prevents duplicate entries if Kafka delivers the DLQ message more than once.
    boolean existsByTransactionId(String transactionId);
}