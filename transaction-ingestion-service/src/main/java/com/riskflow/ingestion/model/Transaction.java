package com.riskflow.ingestion.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

// JPA entity representing a transaction event persisted in PostgreSQL.
// The rich data model here is what enables cross-entity behavioral analytics
// in the Risk Scoring Service — card fingerprint, device fingerprint, IP address,
// and country fields feed directly into the sliding-window velocity trackers in Redis.
@Entity
@Table(name = "transactions")
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Unique identifier for this transaction — used as the idempotency key
    // in the Risk Scoring Service to prevent duplicate scoring on Kafka retries
    @Column(nullable = false, unique = true)
    private String transactionId;

    // The user who initiated this transaction
    @Column(nullable = false)
    private String userId;

    // Hashed fingerprint of the card used — used for per-card velocity tracking
    @Column(nullable = false)
    private String cardFingerprint;

    // Transaction amount in the smallest currency unit (e.g. cents for USD)
    @Column(nullable = false)
    private Long amount;

    // ISO 4217 currency code (e.g. USD, EUR, GBP)
    @Column(nullable = false)
    private String currency;

    // Identifier of the merchant receiving the transaction
    @Column(nullable = false)
    private String merchantId;

    // Merchant Category Code — 4-digit ISO 18245 code
    // Used in rule engine expressions (e.g. MCC 6051 = cryptocurrency exchanges)
    private String merchantCategoryCode;

    // Platform-assigned risk tier for this merchant (low / medium / high)
    private String merchantRiskTier;

    // Hashed fingerprint of the device submitting the transaction
    // Used for per-device cross-card velocity tracking
    private String deviceFingerprint;

    // IP address of the request — used for per-IP cross-card velocity tracking
    private String ipAddress;

    // Country inferred from the IP address
    // Compared against billingCountry for geographic mismatch detection
    private String ipCountry;

    // Country on the billing address registered to this card
    private String billingCountry;

    // Raw user agent string — used for bot detection heuristics
    private String userAgent;

    // Lifecycle status — set to PENDING on creation, updated by Risk Scoring Service
    @Enumerated(EnumType.STRING)
    private TransactionStatus status;

    // Timestamp of when this transaction was received by the ingestion service
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        // Automatically set createdAt and default status on first persist
        this.createdAt = LocalDateTime.now();
        this.status = TransactionStatus.PENDING;
    }

    // --- Getters and Setters ---

    public Long getId() { return id; }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getCardFingerprint() { return cardFingerprint; }
    public void setCardFingerprint(String cardFingerprint) { this.cardFingerprint = cardFingerprint; }

    public Long getAmount() { return amount; }
    public void setAmount(Long amount) { this.amount = amount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }

    public String getMerchantCategoryCode() { return merchantCategoryCode; }
    public void setMerchantCategoryCode(String merchantCategoryCode) { this.merchantCategoryCode = merchantCategoryCode; }

    public String getMerchantRiskTier() { return merchantRiskTier; }
    public void setMerchantRiskTier(String merchantRiskTier) { this.merchantRiskTier = merchantRiskTier; }

    public String getDeviceFingerprint() { return deviceFingerprint; }
    public void setDeviceFingerprint(String deviceFingerprint) { this.deviceFingerprint = deviceFingerprint; }

    public String getIpAddress() { return ipAddress; }
    public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }

    public String getIpCountry() { return ipCountry; }
    public void setIpCountry(String ipCountry) { this.ipCountry = ipCountry; }

    public String getBillingCountry() { return billingCountry; }
    public void setBillingCountry(String billingCountry) { this.billingCountry = billingCountry; }

    public String getUserAgent() { return userAgent; }
    public void setUserAgent(String userAgent) { this.userAgent = userAgent; }

    public TransactionStatus getStatus() { return status; }
    public void setStatus(TransactionStatus status) { this.status = status; }

    public LocalDateTime getCreatedAt() { return createdAt; }
}