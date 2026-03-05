# ============================================================
# SIMPLE STRUCTURED LOGGER
# ============================================================

def log(level, message):
    """
    Basic structured logger for consistent console output.
    """

    print(f"[{level}] {message}")