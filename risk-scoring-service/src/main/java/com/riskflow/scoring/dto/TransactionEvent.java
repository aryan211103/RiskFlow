package com.riskflow.scoring.dto;

/**
 * In-memory representation of a transaction event received from Kafka.
 *
 * This is NOT a JPA entity — it has no @Entity annotation and maps to
 * no database table. It exists only to give us a strongly-typed object
 * to work with after parsing the raw pipe-delimited Kafka message.
 *
 * The field order matches the pipe-delimited payload published by the
 * Transaction Ingestion Service exactly. If the ingestion service ever
 * changes its payload format, this class and the parser must be updated
 * together.
 *
 * Payload format (pipe-delimited, positional):
 *   0  transactionId
 *   1  userId
 *   2  cardFingerprint
 *   3  amount         (parsed as double)
 *   4  currency
 *   5  merchantId
 *   6  merchantCategoryCode
 *   7  merchantRiskTier
 *   8  deviceFingerprint
 *   9  ipAddress
 *   10 ipCountry
 *   11 billingCountry
 *   12 userAgent
 *   13 createdAt
 */
public class TransactionEvent {

    private final String transactionId;
    private final String userId;
    private final String cardFingerprint;
    private final double amount;
    private final String currency;
    private final String merchantId;
    private final String merchantCategoryCode;
    private final String merchantRiskTier;
    private final String deviceFingerprint;
    private final String ipAddress;
    private final String ipCountry;
    private final String billingCountry;
    private final String userAgent;
    private final String createdAt;

    /**
     * Static factory method that parses a raw pipe-delimited Kafka payload
     * into a TransactionEvent.
     *
     * Why static factory instead of a constructor that takes a String?
     * Clarity. TransactionEvent.from(raw) reads as "create a TransactionEvent
     * from this raw string," which is more expressive than new TransactionEvent(raw).
     *
     * Why split("\\|", -1)?
     * The -1 limit tells String.split() to include trailing empty strings.
     * Without it, if the last field is empty, split() silently drops it and
     * the array is shorter than expected, causing ArrayIndexOutOfBoundsException.
     *
     * @param raw the pipe-delimited string from Kafka
     * @return a populated TransactionEvent
     * @throws IllegalArgumentException if the payload doesn't have exactly 14 fields
     */
    public static TransactionEvent from(String raw) {
        // The ingestion service publishes key:value pairs separated by pipes.
        // Format: transactionId:txn_xxx|userId:user_xxx|cardFingerprint:fpr_xxx|...
        // We parse into a map first, then extract by key — this is resilient to
        // field reordering and tolerant of missing optional fields like userAgent.
        java.util.Map<String, String> fields = new java.util.HashMap<>();
        for (String part : raw.split("\\|", -1)) {
            int colonIdx = part.indexOf(':');
            if (colonIdx > 0) {
                String key = part.substring(0, colonIdx).trim();
                String value = part.substring(colonIdx + 1).trim();
                fields.put(key, value);
            }
        }
    
        // These fields are required — fail fast if any are missing
        String[] required = {
            "transactionId", "userId", "cardFingerprint", "amount",
            "currency", "merchantId", "merchantCategoryCode", "merchantRiskTier",
            "deviceFingerprint", "ipAddress", "ipCountry", "billingCountry"
        };
        for (String key : required) {
            if (!fields.containsKey(key)) {
                throw new IllegalArgumentException(
                    "Missing required field '" + key + "' in payload: " + raw);
            }
        }
    
        return new TransactionEvent(
            fields.get("transactionId"),
            fields.get("userId"),
            fields.get("cardFingerprint"),
            Double.parseDouble(fields.get("amount")),
            fields.get("currency"),
            fields.get("merchantId"),
            fields.get("merchantCategoryCode"),
            fields.get("merchantRiskTier"),
            fields.get("deviceFingerprint"),
            fields.get("ipAddress"),
            fields.get("ipCountry"),
            fields.get("billingCountry"),
            fields.getOrDefault("userAgent", ""),   // optional — not in current payload
            fields.getOrDefault("createdAt", "")    // optional — not in current payload
        );
    }

    // Private constructor — force use of the static factory method
    private TransactionEvent(String transactionId, String userId,
                              String cardFingerprint, double amount,
                              String currency, String merchantId,
                              String merchantCategoryCode, String merchantRiskTier,
                              String deviceFingerprint, String ipAddress,
                              String ipCountry, String billingCountry,
                              String userAgent, String createdAt) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.cardFingerprint = cardFingerprint;
        this.amount = amount;
        this.currency = currency;
        this.merchantId = merchantId;
        this.merchantCategoryCode = merchantCategoryCode;
        this.merchantRiskTier = merchantRiskTier;
        this.deviceFingerprint = deviceFingerprint;
        this.ipAddress = ipAddress;
        this.ipCountry = ipCountry;
        this.billingCountry = billingCountry;
        this.userAgent = userAgent;
        this.createdAt = createdAt;
    }

    // Getters
    public String getTransactionId() { return transactionId; }
    public String getUserId() { return userId; }
    public String getCardFingerprint() { return cardFingerprint; }
    public double getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public String getMerchantId() { return merchantId; }
    public String getMerchantCategoryCode() { return merchantCategoryCode; }
    public String getMerchantRiskTier() { return merchantRiskTier; }
    public String getDeviceFingerprint() { return deviceFingerprint; }
    public String getIpAddress() { return ipAddress; }
    public String getIpCountry() { return ipCountry; }
    public String getBillingCountry() { return billingCountry; }
    public String getUserAgent() { return userAgent; }
    public String getCreatedAt() { return createdAt; }
}