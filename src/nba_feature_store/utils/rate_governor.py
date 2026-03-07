import time
from collections import deque


class RateGovernor:
    """
    Adaptive rate limiter for NBA API requests.

    Tracks request velocity and dynamically slows ingestion when
    API pressure or failures occur.
    """

    def __init__(self):

        self.window_seconds = 300
        self.request_timestamps = deque()

        self.throttle_level = 0
        self.max_throttle_level = 4

        self.multipliers = [1.0, 1.5, 2.0, 3.0, 4.0]

        self.base_endpoint_delay = 0.6
        self.base_game_delay = 0.9
        self.base_day_delay = 3.0

        self.failure_score = 0
        self.consecutive_failures = 0

        self.cooldown_tiers = [60, 120, 240, 300]
        self.cooldown_index = 0

        self.batch_cooldown = 120
        self.max_batch_cooldown = 300

        self.max_requests_per_minute = 75

    def register_request(self):

        now = time.time()
        self.request_timestamps.append(now)

        while self.request_timestamps and \
              now - self.request_timestamps[0] > self.window_seconds:
            self.request_timestamps.popleft()

        self._evaluate_pressure()

    def _evaluate_pressure(self):

        rpm = len(self.request_timestamps) / (self.window_seconds / 60)

        if rpm > self.max_requests_per_minute:
            self._increase_throttle()

    def _increase_throttle(self):

        if self.throttle_level < self.max_throttle_level:
            self.throttle_level += 1
            print(f"[WARNING] Throttle level increased to {self.throttle_level}")

    def _decrease_throttle(self):

        if self.throttle_level > 0:
            self.throttle_level -= 1
            print(f"[INFO] Throttle level decreased to {self.throttle_level}")

    def multiplier(self):

        return self.multipliers[self.throttle_level]

    def sleep_endpoint(self):

        time.sleep(self.base_endpoint_delay * self.multiplier())

    def sleep_game(self):

        time.sleep(self.base_game_delay * self.multiplier())

    def sleep_day(self):

        time.sleep(self.base_day_delay * self.multiplier())

    def record_success(self):

        self.consecutive_failures = 0

        if self.failure_score > 0:
            self.failure_score -= 1

        if self.failure_score == 0:
            self._decrease_throttle()

    def record_failure(self):

        self.failure_score += 1
        self.consecutive_failures += 1
        self._increase_throttle()

    def apply_cooldown_if_needed(self):

        if self.consecutive_failures >= 2:

            cooldown = self.cooldown_tiers[self.cooldown_index]

            print(f"[WARNING] Applying cooldown: {cooldown}s")

            time.sleep(cooldown)

            if self.cooldown_index < len(self.cooldown_tiers) - 1:
                self.cooldown_index += 1

            self.consecutive_failures = 0

    def sleep_batch(self):

        print(f"[INFO] Batch cooldown: {self.batch_cooldown}s")

        time.sleep(self.batch_cooldown)

        self.batch_cooldown = min(
            int(self.batch_cooldown * 1.5),
            self.max_batch_cooldown
        )