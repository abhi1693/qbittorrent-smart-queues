import unittest
from unittest import mock

from qbittorrent_smart_queues.scheduling import RelativeSpeedPolicy, WorkerSpeedSample


class RelativeSpeedPolicyTests(unittest.TestCase):
    def sample(self, worker_id, speed, group="movies", trial_age=600):
        return WorkerSpeedSample(
            worker_id=worker_id,
            group=group,
            speed_bytes_per_second=speed,
            trial_age_seconds=trial_age,
        )

    def test_disabled_policy_never_yields(self):
        policy = RelativeSpeedPolicy(enabled=False)

        decisions = policy.assess(
            [
                self.sample("slow", 10_000),
                self.sample("fast-one", 4_000_000),
                self.sample("fast-two", 5_000_000),
            ],
            {"movies": 1},
        )

        self.assertEqual([], decisions)

    def test_yields_multiple_outliers_using_peer_median(self):
        policy = RelativeSpeedPolicy(enabled=True)
        samples = [
            self.sample("slow-movie", 10_000),
            self.sample("slow-tv", 20_000, group="tv"),
            self.sample("fast-one", 3_000_000),
            self.sample("fast-two", 4_000_000),
            self.sample("fast-three", 5_000_000),
        ]

        decisions = policy.assess(samples, {"movies": 1, "tv": 1})

        self.assertEqual({"slow-movie", "slow-tv"}, {item.sample.worker_id for item in decisions})
        self.assertTrue(all(item.peer_reference_bytes_per_second >= 3_000_000 for item in decisions))

    def test_requires_same_group_replacement(self):
        policy = RelativeSpeedPolicy(enabled=True)
        samples = [
            self.sample("slow-tv", 10_000, group="tv"),
            self.sample("fast-one", 3_000_000),
            self.sample("fast-two", 4_000_000),
        ]

        decisions = policy.assess(samples, {"movies": 3})

        self.assertEqual([], decisions)

    def test_preserves_worker_during_minimum_trial(self):
        policy = RelativeSpeedPolicy(enabled=True, minimum_trial_seconds=300)
        samples = [
            self.sample("new-worker", 10_000, trial_age=299),
            self.sample("fast-one", 3_000_000),
            self.sample("fast-two", 4_000_000),
        ]

        decisions = policy.assess(samples, {"movies": 1})

        self.assertEqual([], decisions)

    def test_does_not_yield_when_whole_pool_is_slow(self):
        policy = RelativeSpeedPolicy(enabled=True)
        samples = [
            self.sample("one", 100_000),
            self.sample("two", 120_000),
            self.sample("three", 140_000),
        ]

        decisions = policy.assess(samples, {"movies": 3})

        self.assertEqual([], decisions)

    def test_environment_values_are_bounded(self):
        with mock.patch.dict(
            "os.environ",
            {
                "QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_ENABLED": "true",
                "QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_FRACTION": "2",
                "QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_MIN_PEERS": "0",
                "QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_MIN_REFERENCE_BYTES_PER_SEC": "0",
                "QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_MIN_TRIAL_SECONDS": "-1",
                "QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_DEFER_SECONDS": "-1",
            },
        ):
            policy = RelativeSpeedPolicy.from_environment()

        self.assertTrue(policy.enabled)
        self.assertEqual(0.95, policy.threshold_fraction)
        self.assertEqual(1, policy.minimum_peer_workers)
        self.assertEqual(1, policy.minimum_reference_bytes_per_second)
        self.assertEqual(0, policy.minimum_trial_seconds)
        self.assertEqual(0, policy.defer_seconds)


if __name__ == "__main__":
    unittest.main()
