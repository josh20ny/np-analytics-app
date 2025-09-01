# app/cadence/constants.py

# How many samples we require to “trust” a bucket classification.
MIN_SAMPLES_FOR_BUCKET = 2

# Rolling window (days) used by cadence calculations unless overridden.
DEFAULT_ROLLING_DAYS = 180

# Threshold: how many cycles missed => count as “lapsed” (for non-irregular buckets).
LAPSE_CYCLES_THRESHOLD = 3

REGULAR_MIN_SAMPLES = 2

# Single source of truth for bucket targets.
def bucket_days(name: str) -> int:
    return {
        "weekly":   7,
        "biweekly": 14,
        "monthly":  30,   # unified to 30 (remove any 28s)
        "6weekly":  42,
    }.get(name, 9999)

# Which gap sizes we “snap” to when estimating cadence
BUCKET_TARGETS = [7, 14, 30, 42]  # weekly, biweekly, monthly, 6weekly
