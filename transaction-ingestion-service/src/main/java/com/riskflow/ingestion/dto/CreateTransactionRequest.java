package com.riskflow.ingestion.dto;

// Data Transfer Object for incoming transaction creation requests.
// This is what the caller sends in the POST /transactions request body.
// We keep this separate from the Transaction entity so the API contract
// is decoupled from the database schema — a standard best practice.
public class CreateTransactionRequest {

    private String userId;
    private String cardFingerprint;
    private Long amount;
    private String currency;
    private String merchantId;
    private String merchantCategoryCode;
    private String merchantRiskTier;
    private String deviceFingerprint;
    private String ipAddress;
    private String ipCountry;
    private String billingCountry;
    private String userAgent;

    // --- Getters and Setters ---

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
}