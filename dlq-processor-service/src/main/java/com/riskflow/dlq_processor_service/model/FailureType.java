package com.riskflow.dlq_processor_service.model;

// Classifies why a message failed.
// TRANSIENT: temporary problem — database blip, timeout, network hiccup.
//            Worth retrying. Will likely succeed on a later attempt.
// POISON_PILL: permanent problem — malformed payload, null pointer on
//              bad data, business rule violation. Will never succeed.
//              Must be quarantined and reviewed by a human.
public enum FailureType {
    TRANSIENT,
    POISON_PILL
}