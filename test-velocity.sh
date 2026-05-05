#!/bin/bash
for i in 1 2 3 4 5; do
  echo "Sending transaction $i..."
  curl -s -X POST http://localhost:8082/transactions \
    -H "Content-Type: application/json" \
    -d '{"userId":"user_test","cardFingerprint":"fpr_velocity_test","amount":50,"currency":"USD","merchantId":"mch_001","merchantCategoryCode":"5942","merchantRiskTier":"low","deviceFingerprint":"dev_test_001","ipAddress":"203.0.113.45","ipCountry":"US","billingCountry":"US"}'
  echo ""
done