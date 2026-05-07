#!/Users/aryansmac/miniforge3/bin/python3
"""
RiskFlow — Synthetic Transaction Generator
==========================================
Fires realistic transaction traffic against the Transaction Ingestion Service
to demonstrate velocity scoring, rule matching, DLQ routing, and audit trails
without manually writing curl commands.

Usage:
    python3 generate_transactions.py                  # one-shot mode (default)
    python3 generate_transactions.py --continuous     # runs until Ctrl+C

Requirements:
    pip install requests
"""

import argparse
import random
import string
import time
import sys

# The `requests` library is the standard Python HTTP client.
# It is not in the standard library, so install it with: pip install requests
try:
    import requests
except ImportError:
    print("ERROR: 'requests' library not found. Run: pip install requests")
    sys.exit(1)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Base URL of your Transaction Ingestion Service.
# Change this if you run the service on a different port.
INGESTION_URL = "http://localhost:8082/transactions"

# How many times each scenario fires in one-shot mode.
ONE_SHOT_REPETITIONS = 3

# Seconds to wait between transactions in continuous mode.
# A random value between these two bounds is chosen each time,
# so the traffic looks organic rather than perfectly metronomic.
CONTINUOUS_DELAY_MIN = 0.3
CONTINUOUS_DELAY_MAX = 1.2

# Seconds to wait between bursts in the velocity spike scenario.
# The spike fires 10 transactions rapidly, then this pause separates bursts.
VELOCITY_BURST_DELAY = 0.05  # 50ms between each transaction in the burst


# =============================================================================
# TERMINAL COLOR CODES
# =============================================================================
# ANSI escape codes let us print colored text in the terminal.
# \033[  opens the escape sequence
# The number selects the color
# m    closes the color selection
# \033[0m resets back to default color

GREEN  = "\033[92m"   # APPROVED decisions
YELLOW = "\033[93m"   # NEEDS_REVIEW decisions / in-progress
RED    = "\033[91m"   # AUTO_REJECTED / errors
CYAN   = "\033[96m"   # Section headers
RESET  = "\033[0m"    # Back to normal terminal color


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def random_id(prefix: str, length: int = 8) -> str:
    """
    Generate a random alphanumeric ID with a given prefix.
    Example: random_id("txn") might return "txn_a3f9bc12"

    We use this to ensure every transaction has a unique transactionId,
    which is required for the idempotency check in Redis:
        SET risk:processed:{txnId} NX EX 86400
    Sending the same transactionId twice would be silently de-duplicated.
    """
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return f"{prefix}_{suffix}"


def send_transaction(payload: dict, label: str) -> None:
    """
    POST a single transaction payload to the ingestion service and print the result.

    The ingestion service returns HTTP 201 Created on success.
    It does NOT return the risk decision — that happens asynchronously
    through the Kafka pipeline. To see decisions, watch the risk-scoring-service logs.

    Parameters:
        payload : dict  — the transaction fields as a Python dictionary
        label   : str   — human-readable scenario name for console output
    """
    try:
        # requests.post() sends an HTTP POST request.
        # json=payload automatically sets Content-Type: application/json
        # and serializes the dict to a JSON string.
        # timeout=5 prevents hanging forever if the service is down.
        response = requests.post(INGESTION_URL, json=payload, timeout=5)

        # Format the amount for display with two decimal places
        amount_display = f"${float(payload.get('amount', 0)):.2f}"

        if response.status_code in (200, 201):
            # HTTP 201 means the ingestion service accepted the transaction.
            # The risk decision is NOT here — it arrives asynchronously.
            print(
                f"{GREEN}[SENT ✓]{RESET} "
                f"{payload['transactionId']:<20} "
                f"{label:<30} "
                f"{amount_display:<10} "
                f"card: {payload.get('cardFingerprint', 'n/a')}"
            )
        else:
            # Any non-2xx status means the ingestion service rejected the request.
            # This is a problem with the payload or the service, not a risk decision.
            print(
                f"{RED}[HTTP {response.status_code}]{RESET} "
                f"{payload['transactionId']:<20} "
                f"{label:<30} "
                f"Response: {response.text[:80]}"
            )

    except requests.exceptions.ConnectionError:
        # The service is not running or the port is wrong.
        print(
            f"{RED}[CONNECTION ERROR]{RESET} "
            f"Cannot reach {INGESTION_URL}. "
            f"Is the transaction-ingestion-service running on port 8082?"
        )
    except requests.exceptions.Timeout:
        # The service took more than 5 seconds to respond.
        print(
            f"{RED}[TIMEOUT]{RESET} "
            f"{payload['transactionId']} — service did not respond within 5s"
        )


# =============================================================================
# TRANSACTION FACTORIES
# =============================================================================
# Each factory function returns a dict representing one transaction payload.
# The dict keys map to the fields your ingestion service expects in the
# CreateTransactionRequest DTO.
#
# Every factory calls random_id("txn") so each transaction gets a unique ID.
# Fields that are fixed across calls (like cardFingerprint) are intentionally
# constant — that is what makes velocity and blocklist tests work.

def make_normal_low_risk() -> dict:
    """
    A clean, low-risk transaction that should score 0 and be APPROVED.
    No rule conditions are met:
        - amount < 500         (no high_amount rule)
        - merchantRiskTier low (no high_risk_merchant rule)
        - MCC 5942 books       (no gambling_mcc rule)
        - ipCountry == billingCountry (no country_mismatch rule)
    """
    return {
        "transactionId":       random_id("txn"),
        "userId":              random_id("user"),
        "cardFingerprint":     f"fpr_normal_{random.randint(1, 5):03d}",
        "amount":              round(random.uniform(10.0, 150.0), 2),
        "currency":            "USD",
        "merchantId":          random_id("mch"),
        "merchantCategoryCode": "5942",    # Books & stationery — harmless MCC
        "merchantRiskTier":    "low",
        "deviceFingerprint":   f"dev_normal_{random.randint(1, 3):03d}",
        "ipAddress":           "203.0.113.10",
        "ipCountry":           "US",
        "billingCountry":      "US",
    }


def make_high_amount_hard_rule() -> dict:
    """
    Amount exceeds the hard cap of 10,000. This triggers Stage 1 hard rules
    in the risk scoring service and results in AUTO_REJECTED without
    even reaching the rule engine.

    Hard rule check in TransactionEventConsumer (Stage 1):
        if (event.getAmount().compareTo(BigDecimal.valueOf(10000)) > 0)
            → AUTO_REJECTED
    """
    return {
        "transactionId":       random_id("txn"),
        "userId":              random_id("user"),
        "cardFingerprint":     f"fpr_normal_{random.randint(1, 5):03d}",
        "amount":              round(random.uniform(10001.0, 25000.0), 2),
        "currency":            "USD",
        "merchantId":          random_id("mch"),
        "merchantCategoryCode": "5411",    # Grocery store — unremarkable MCC
        "merchantRiskTier":    "low",
        "deviceFingerprint":   f"dev_normal_{random.randint(1, 3):03d}",
        "ipAddress":           "203.0.113.20",
        "ipCountry":           "US",
        "billingCountry":      "US",
    }


def make_blocked_card() -> dict:
    """
    Uses one of the three hardcoded blocked card fingerprints from Stage 1.
    The hard rules blocklist is:
        Set.of("fpr_blocked_001", "fpr_blocked_002", "fpr_blocked_003")

    This will be AUTO_REJECTED at Stage 1 — fastest possible rejection path.
    Useful for showing that the blocklist check fires before any Redis or DB work.
    """
    blocked_cards = ["fpr_blocked_001", "fpr_blocked_002", "fpr_blocked_003"]
    return {
        "transactionId":       random_id("txn"),
        "userId":              random_id("user"),
        "cardFingerprint":     random.choice(blocked_cards),
        "amount":              round(random.uniform(50.0, 300.0), 2),
        "currency":            "USD",
        "merchantId":          random_id("mch"),
        "merchantCategoryCode": "5411",
        "merchantRiskTier":    "low",
        "deviceFingerprint":   f"dev_normal_{random.randint(1, 3):03d}",
        "ipAddress":           "203.0.113.30",
        "ipCountry":           "US",
        "billingCountry":      "US",
    }


def make_high_risk_merchant() -> dict:
    """
    merchantRiskTier = 'high' triggers the seeded rule:
        high_risk_merchant: merchantRiskTier == 'high' → score += 25

    Amount is kept under 500 so the high_amount rule does NOT also fire.
    This isolates the high_risk_merchant rule contribution.
    Expected outcome: score = 25 → likely NEEDS_REVIEW (threshold-dependent).
    """
    return {
        "transactionId":       random_id("txn"),
        "userId":              random_id("user"),
        "cardFingerprint":     f"fpr_normal_{random.randint(1, 5):03d}",
        "amount":              round(random.uniform(50.0, 499.0), 2),
        "currency":            "USD",
        "merchantId":          random_id("mch"),
        "merchantCategoryCode": "5999",    # Miscellaneous — not a flagged MCC
        "merchantRiskTier":    "high",     # ← this is the trigger
        "deviceFingerprint":   f"dev_normal_{random.randint(1, 3):03d}",
        "ipAddress":           "203.0.113.40",
        "ipCountry":           "US",
        "billingCountry":      "US",
    }


def make_gambling_mcc() -> dict:
    """
    merchantCategoryCode = '7995' triggers the seeded rule:
        gambling_mcc: merchantCategoryCode == '7995' → score += 30

    MCC 7995 is the official Visa/Mastercard code for betting and casino transactions.
    Combined with a high amount this could push into AUTO_REJECTED territory.
    """
    return {
        "transactionId":       random_id("txn"),
        "userId":              random_id("user"),
        "cardFingerprint":     f"fpr_normal_{random.randint(1, 5):03d}",
        "amount":              round(random.uniform(100.0, 800.0), 2),
        "currency":            "USD",
        "merchantId":          random_id("mch"),
        "merchantCategoryCode": "7995",    # ← gambling MCC, the trigger
        "merchantRiskTier":    "medium",
        "deviceFingerprint":   f"dev_normal_{random.randint(1, 3):03d}",
        "ipAddress":           "203.0.113.50",
        "ipCountry":           "US",
        "billingCountry":      "US",
    }


def make_country_mismatch() -> dict:
    """
    ipCountry != billingCountry triggers the seeded rule:
        country_mismatch: ipCountry != billingCountry → score += 15

    This simulates a cardholder billed in the US but connecting from a
    foreign IP address — a common signal in card-not-present fraud.

    The SpEL expression your rule engine evaluates:
        #event.ipCountry != #event.billingCountry
    """
    foreign_countries = ["RU", "CN", "NG", "UA", "BR"]
    return {
        "transactionId":       random_id("txn"),
        "userId":              random_id("user"),
        "cardFingerprint":     f"fpr_normal_{random.randint(1, 5):03d}",
        "amount":              round(random.uniform(50.0, 400.0), 2),
        "currency":            "USD",
        "merchantId":          random_id("mch"),
        "merchantCategoryCode": "5411",
        "merchantRiskTier":    "low",
        "deviceFingerprint":   f"dev_normal_{random.randint(1, 3):03d}",
        "ipAddress":           "198.51.100.77",
        "ipCountry":           random.choice(foreign_countries),  # ← foreign IP country
        "billingCountry":      "US",                              # ← US billing address
    }


def make_prepaid_high_amount() -> dict:
    """
    amount > 300 AND merchantRiskTier == 'high' triggers the compound rule:
        prepaid_high_amount: amount > 300 and merchantRiskTier == 'high' → score += 35

    This is your only multi-condition SpEL rule and is the highest single-rule score.
    This scenario specifically exists to test that your SpEL engine correctly
    evaluates compound boolean expressions (the 'and' operator in SpEL).
    """
    return {
        "transactionId":       random_id("txn"),
        "userId":              random_id("user"),
        "cardFingerprint":     f"fpr_normal_{random.randint(1, 5):03d}",
        "amount":              round(random.uniform(301.0, 900.0), 2),
        "currency":            "USD",
        "merchantId":          random_id("mch"),
        "merchantCategoryCode": "5999",
        "merchantRiskTier":    "high",     # ← condition 1
        # amount > 300 is guaranteed by the range above ← condition 2
        "deviceFingerprint":   f"dev_normal_{random.randint(1, 3):03d}",
        "ipAddress":           "203.0.113.60",
        "ipCountry":           "US",
        "billingCountry":      "US",
    }


def make_high_amount_rule_only() -> dict:
    """
    amount between 501 and 9999 triggers only the seeded rule:
        high_amount: amount > 500 → score += 20

    This is distinct from make_high_amount_hard_rule() which exceeds the
    10,000 HARD cap and gets auto-rejected at Stage 1.
    This scenario reaches the rule engine and adds 20 to the score.
    """
    return {
        "transactionId":       random_id("txn"),
        "userId":              random_id("user"),
        "cardFingerprint":     f"fpr_normal_{random.randint(1, 5):03d}",
        "amount":              round(random.uniform(501.0, 999.0), 2),
        "currency":            "USD",
        "merchantId":          random_id("mch"),
        "merchantCategoryCode": "5411",
        "merchantRiskTier":    "low",      # no high_risk_merchant rule
        "deviceFingerprint":   f"dev_normal_{random.randint(1, 3):03d}",
        "ipAddress":           "203.0.113.70",
        "ipCountry":           "US",
        "billingCountry":      "US",
    }


# =============================================================================
# VELOCITY SPIKE — SPECIAL CASE
# =============================================================================
# This is not a single factory call. It fires 10 transactions rapidly from
# the same card fingerprint to trigger the Redis sliding-window behavioral
# analyzer in Stage 2.
#
# Your BehavioralAnalyzer tracks:
#   per-card velocity (1m / 5m / 1h) using sorted sets in Redis
#   key pattern: velocity:card:{cardFingerprint}:1m
#
# Firing 10 transactions within 60 seconds from fpr_velocity_001 will cause
# the 1-minute window to overflow and add a behavioral score contribution.

VELOCITY_CARD = "fpr_velocity_001"    # fixed fingerprint so Redis accumulates counts
VELOCITY_DEVICE = "dev_velocity_001"  # fixed device so cross-device check also fires


def make_velocity_burst(burst_index: int) -> dict:
    """
    One transaction in a rapid-fire velocity burst.

    All transactions share the same cardFingerprint (VELOCITY_CARD) and
    deviceFingerprint (VELOCITY_DEVICE) so the behavioral analyzer counts
    them in the same sliding windows.

    Parameters:
        burst_index : int — used only for the console label, e.g. "(3/10)"
    """
    return {
        "transactionId":       random_id("txn"),
        "userId":              "user_velocity_tester",
        "cardFingerprint":     VELOCITY_CARD,
        "amount":              round(random.uniform(20.0, 80.0), 2),
        "currency":            "USD",
        "merchantId":          random_id("mch"),
        "merchantCategoryCode": "5411",
        "merchantRiskTier":    "low",
        "deviceFingerprint":   VELOCITY_DEVICE,
        "ipAddress":           "203.0.113.99",
        "ipCountry":           "US",
        "billingCountry":      "US",
    }


def fire_velocity_spike() -> None:
    """
    Fires 10 transactions from VELOCITY_CARD with a 50ms gap between each.
    The first few will likely be APPROVED. By transactions 5-10, the 1-minute
    Redis window should be filling up and scores should rise.

    Watch the risk-scoring-service logs to see the velocity counts increment:
        [BEHAVIORAL] card fpr_velocity_001 1m=7 5m=7 1h=7
    """
    print(f"\n{CYAN}--- VELOCITY SPIKE: firing 10 rapid transactions from {VELOCITY_CARD} ---{RESET}")
    for i in range(1, 11):
        payload = make_velocity_burst(i)
        send_transaction(payload, f"velocity_spike ({i}/10)")
        time.sleep(VELOCITY_BURST_DELAY)   # 50ms gap — fast enough to saturate the 1m window
    print(f"{CYAN}--- VELOCITY SPIKE: complete ---{RESET}\n")


# =============================================================================
# SCENARIO REGISTRY
# =============================================================================
# Each entry is a tuple: (factory_function, label, weight)
#
# Weight determines how often this scenario appears in continuous mode.
# A weight of 5 means it is 5x as likely to be chosen as a weight of 1.
#
# In real fraud traffic, roughly 90% of transactions are legitimate.
# We give normal_low_risk a high weight to reflect that reality.
# Velocity spike is handled separately because it fires 10 transactions at once.

SCENARIOS = [
    (make_normal_low_risk,          "normal_low_risk",          10),
    (make_high_amount_hard_rule,    "hard_rule_amount_cap",      2),
    (make_blocked_card,             "hard_rule_blocked_card",    2),
    (make_high_risk_merchant,       "rule_high_risk_merchant",   3),
    (make_gambling_mcc,             "rule_gambling_mcc",         2),
    (make_country_mismatch,         "rule_country_mismatch",     3),
    (make_prepaid_high_amount,      "rule_prepaid_high_amount",  2),
    (make_high_amount_rule_only,    "rule_high_amount_only",     2),
]

# Unpack for use with random.choices()
SCENARIO_FACTORIES = [s[0] for s in SCENARIOS]
SCENARIO_LABELS    = [s[1] for s in SCENARIOS]
SCENARIO_WEIGHTS   = [s[2] for s in SCENARIOS]


# =============================================================================
# RUNNER — ONE-SHOT MODE
# =============================================================================

def run_one_shot() -> None:
    """
    Fires each scenario ONE_SHOT_REPETITIONS times, then fires one velocity spike.
    Total transactions = (len(SCENARIOS) * ONE_SHOT_REPETITIONS) + 10 velocity.

    With the defaults: 8 scenarios × 3 reps = 24 transactions + 10 velocity = 34 total.
    """
    print(f"\n{CYAN}{'='*60}{RESET}")
    print(f"{CYAN}  RiskFlow Synthetic Transaction Generator — ONE-SHOT MODE{RESET}")
    print(f"{CYAN}  Target: {INGESTION_URL}{RESET}")
    print(f"{CYAN}  Scenarios: {len(SCENARIOS)} × {ONE_SHOT_REPETITIONS} reps + 1 velocity burst{RESET}")
    print(f"{CYAN}{'='*60}{RESET}\n")

    # Fire each scenario the configured number of times.
    for factory, label, _ in SCENARIOS:
        print(f"{YELLOW}--- Scenario: {label} ({ONE_SHOT_REPETITIONS}x) ---{RESET}")
        for _ in range(ONE_SHOT_REPETITIONS):
            payload = factory()
            send_transaction(payload, label)
            time.sleep(0.1)   # small pause so the ingestion service is not overwhelmed
        print()

    # Fire the velocity spike as the final scenario.
    fire_velocity_spike()

    print(f"\n{GREEN}One-shot run complete.{RESET}")
    print(f"Watch risk-scoring-service logs for decisions.")
    print(f"Query PostgreSQL to audit decisions:")
    print(f"  SELECT transaction_id, decision, risk_score, reason")
    print(f"  FROM risk_decisions ORDER BY created_at DESC LIMIT 40;\n")


# =============================================================================
# RUNNER — CONTINUOUS MODE
# =============================================================================

def run_continuous() -> None:
    """
    Continuously picks scenarios by weight and fires them with a random delay.
    Every 25 transactions, fires a velocity burst to keep the Redis windows active.

    Press Ctrl+C to stop.
    """
    print(f"\n{CYAN}{'='*60}{RESET}")
    print(f"{CYAN}  RiskFlow Synthetic Transaction Generator — CONTINUOUS MODE{RESET}")
    print(f"{CYAN}  Target: {INGESTION_URL}{RESET}")
    print(f"{CYAN}  Press Ctrl+C to stop{RESET}")
    print(f"{CYAN}{'='*60}{RESET}\n")

    transaction_count = 0

    try:
        while True:
            transaction_count += 1

            # Every 25 transactions, fire a velocity burst.
            # This keeps the Redis sliding windows populated so the behavioral
            # analyzer always has data to score against.
            if transaction_count % 25 == 0:
                fire_velocity_spike()
                transaction_count += 10   # account for the 10 burst transactions
                continue

            # Pick a scenario using weighted random selection.
            # random.choices() returns a list; [0] extracts the single element.
            factory, label = random.choices(
                list(zip(SCENARIO_FACTORIES, SCENARIO_LABELS)),
                weights=SCENARIO_WEIGHTS,
                k=1
            )[0]

            payload = factory()
            send_transaction(payload, label)

            # Sleep a random duration between transactions.
            # Randomness makes traffic look organic rather than perfectly timed.
            delay = random.uniform(CONTINUOUS_DELAY_MIN, CONTINUOUS_DELAY_MAX)
            time.sleep(delay)

    except KeyboardInterrupt:
        # Ctrl+C raises KeyboardInterrupt. We catch it to print a clean exit message
        # instead of a Python traceback.
        print(f"\n\n{YELLOW}Continuous mode stopped by user. Total transactions sent: {transaction_count}{RESET}\n")


# =============================================================================
# ENTRY POINT
# =============================================================================

def main() -> None:
    """
    Parse command-line arguments and dispatch to the appropriate runner.

    argparse is Python's standard library for command-line argument parsing.
    `--continuous` is a flag: if present, its value is True; if absent, False.
    """
    parser = argparse.ArgumentParser(
        description="RiskFlow Synthetic Transaction Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 generate_transactions.py                 # one-shot mode
  python3 generate_transactions.py --continuous    # continuous until Ctrl+C
        """
    )
    parser.add_argument(
        "--continuous",
        action="store_true",    # store True if flag is present, False if absent
        help="Run continuously until Ctrl+C (default: one-shot)"
    )
    args = parser.parse_args()

    if args.continuous:
        run_continuous()
    else:
        run_one_shot()


# Standard Python idiom: only run main() if this file is executed directly,
# not if it is imported as a module by another script.
if __name__ == "__main__":
    main()
