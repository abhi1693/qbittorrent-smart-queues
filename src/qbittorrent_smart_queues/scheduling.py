from dataclasses import dataclass
from statistics import median

from qbittorrent_smart_queues.config import env_bool, env_float, env_int


@dataclass(frozen=True)
class WorkerSpeedSample:
    worker_id: str
    group: str
    speed_bytes_per_second: int
    trial_age_seconds: int | None = None


@dataclass(frozen=True)
class RelativeSpeedAssessment:
    sample: WorkerSpeedSample
    peer_reference_bytes_per_second: int
    yield_threshold_bytes_per_second: int
    should_yield: bool
    reason: str


@dataclass(frozen=True)
class RelativeSpeedPolicy:
    """Select workers that should yield to faster queue candidates.

    The policy is intentionally scheduler-only: yielding is not a download
    failure and does not imply that the worker should be permanently rejected.
    """

    enabled: bool = False
    threshold_fraction: float = 0.25
    threshold_tolerance_fraction: float = 0.10
    minimum_acceptable_bytes_per_second: int = 1_048_576
    minimum_peer_workers: int = 1
    minimum_reference_bytes_per_second: int = 1_048_576
    minimum_trial_seconds: int = 300
    defer_seconds: int = 1_800

    @classmethod
    def from_environment(cls):
        return cls(
            enabled=env_bool("QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_ENABLED", False),
            threshold_fraction=max(
                0.01,
                min(
                    0.95,
                    env_float("QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_FRACTION", 0.25),
                ),
            ),
            threshold_tolerance_fraction=max(
                0.0,
                min(
                    0.95,
                    env_float(
                        "QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_TOLERANCE_FRACTION",
                        0.10,
                    ),
                ),
            ),
            minimum_acceptable_bytes_per_second=max(
                1,
                env_int(
                    "QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_MIN_ACCEPTABLE_BYTES_PER_SEC",
                    1_048_576,
                ),
            ),
            minimum_peer_workers=max(
                1,
                env_int("QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_MIN_PEERS", 1),
            ),
            minimum_reference_bytes_per_second=max(
                1,
                env_int(
                    "QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_MIN_REFERENCE_BYTES_PER_SEC",
                    1_048_576,
                ),
            ),
            minimum_trial_seconds=max(
                0,
                env_int("QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_MIN_TRIAL_SECONDS", 300),
            ),
            defer_seconds=max(
                0,
                env_int("QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_DEFER_SECONDS", 1_800),
            ),
        )

    def assess(self, samples, queued_alternatives_by_group=None):
        samples = [
            sample
            for sample in samples or []
            if sample.worker_id and sample.speed_bytes_per_second >= 0
        ]
        if not self.enabled or not samples:
            return []

        alternatives = {
            str(group): max(0, int(count or 0))
            for group, count in (queued_alternatives_by_group or {}).items()
        }
        assessments = []
        for sample in samples:
            peer_speeds = [
                peer.speed_bytes_per_second
                for peer in samples
                if peer.worker_id != sample.worker_id
            ]
            if len(peer_speeds) < self.minimum_peer_workers:
                continue

            reference = max(0, int(median(peer_speeds)))
            threshold = max(
                1,
                int(
                    reference
                    * self.threshold_fraction
                    * (1.0 - self.threshold_tolerance_fraction)
                ),
            )
            acceptable_floor = max(
                1,
                int(
                    self.minimum_acceptable_bytes_per_second
                    * (1.0 - self.threshold_tolerance_fraction)
                ),
            )
            trial_complete = (
                sample.trial_age_seconds is None
                or sample.trial_age_seconds >= self.minimum_trial_seconds
            )
            underperforming = (
                reference >= self.minimum_reference_bytes_per_second
                and sample.speed_bytes_per_second < threshold
                and sample.speed_bytes_per_second < acceptable_floor
            )
            reason = (
                f"observed {sample.speed_bytes_per_second} B/s versus peer median "
                f"{reference} B/s and yield threshold {threshold} B/s"
            )
            assessments.append(
                RelativeSpeedAssessment(
                    sample=sample,
                    peer_reference_bytes_per_second=reference,
                    yield_threshold_bytes_per_second=threshold,
                    should_yield=underperforming and trial_complete,
                    reason=reason,
                )
            )

        assessments.sort(
            key=lambda item: (
                item.sample.speed_bytes_per_second
                / max(1, item.peer_reference_bytes_per_second),
                item.sample.worker_id,
            )
        )
        selected = []
        for assessment in assessments:
            if not assessment.should_yield:
                continue
            group = str(assessment.sample.group)
            if alternatives.get(group, 0) <= 0:
                continue
            alternatives[group] -= 1
            selected.append(assessment)
        return selected
