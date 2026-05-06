#!/bin/bash
echo "Sending high-risk transaction..."
curl -s -X POST http://localhost:8082/transactions \
  -H "Content-Type: application/json" \
  -d '{"userId":"user_test","cardFingerprint":"fpr_rule_test","amount":600,"currency":"USD","merchantId":"mch_001","merchantCategoryCode":"5942","merchantRiskTier":"high","deviceFingerprint":"dev_rule_001","ipAddress":"203.0.113.45","ipCountry":"US","billingCountry":"US"}'
echo ""