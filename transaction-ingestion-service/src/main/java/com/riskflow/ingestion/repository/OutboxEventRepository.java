package com.riskflow.ingestion.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.riskflow.ingestion.model.OutboxEvent;
import com.riskflow.ingestion.model.OutboxStatus;

// Standard Spring Data repository for OutboxEvent.
// JpaRepository gives us save(), findById(), delete() etc for free.
@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    // The poller calls this to find all records that have not yet
    // been published to Kafka.
    // Spring Data translates this method name into:
    // SELECT * FROM outbox_events WHERE status = 'PENDING'
    List<OutboxEvent> findByStatus(OutboxStatus status);
}