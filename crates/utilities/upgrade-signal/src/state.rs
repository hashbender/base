//! Upgrade signal state values.

use std::collections::BTreeMap;

use alloy_primitives::U256;
use base_common_genesis::BaseUpgrade;
use tracing::{debug, error, info};

use crate::{
    AlloyUpgradeSignalReader, UpgradeSignalMetricLayer, UpgradeSignalMetrics, UpgradeSignalRefresher,
};

/// L1 upgrade signal values for one contract-backed upgrade.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UpgradeSignal {
    /// Contract-backed upgrade passed to the L1 contract.
    pub upgrade_id: BaseUpgrade,
    /// L2 activation timestamp announced on L1.
    pub activation_timestamp: u64,
    /// Minimum node protocol version announced on L1.
    pub protocol_version: U256,
}

impl UpgradeSignal {
    /// Returns the positive activation timestamp announced for this upgrade.
    pub fn positive_activation_timestamp(&self) -> Option<u64> {
        (self.activation_timestamp > 0).then_some(self.activation_timestamp)
    }

    /// Returns true if both signals contain the same contract-backed upgrade values.
    pub fn has_same_contract_values(&self, other: &Self) -> bool {
        self.upgrade_id == other.upgrade_id
            && self.activation_timestamp == other.activation_timestamp
            && self.protocol_version == other.protocol_version
    }
}

/// L1 upgrade signal values for a configured upgrade schedule.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UpgradeSignalSchedule {
    /// L1 block number used to read the complete schedule.
    pub l1_block_number: u64,
    /// Signals read from L1.
    pub signals: Vec<UpgradeSignal>,
}

impl UpgradeSignalSchedule {
    /// Creates a new upgrade signal schedule.
    pub const fn new(l1_block_number: u64, signals: Vec<UpgradeSignal>) -> Self {
        Self { l1_block_number, signals }
    }
}

/// Result of applying a live signal read to local metrics state.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum UpgradeSignalStateUpdate {
    /// The signal established the initial live baseline.
    Initialized,
    /// The signal is identical to the previous live signal.
    Unchanged,
    /// The signal changed while the node was live.
    Changed,
}

impl UpgradeSignalStateUpdate {
    /// Returns true when this update requires re-applying the schedule.
    ///
    /// [`Self::Initialized`] requires apply: a signal observed live for the first time may carry
    /// a schedule change that landed after the baseline should have been established (an upgrade
    /// registered on L1 after node start, or a startup window of failed reads), so it must not be
    /// silently adopted as the baseline.
    const fn requires_apply(self) -> bool {
        matches!(self, Self::Initialized | Self::Changed)
    }
}

/// Stateful live tracker for one contract-backed upgrade.
///
/// Two independent baselines are tracked: `observed` advances on every read and drives metrics and
/// change detection, while `applied` advances only when a schedule is successfully committed to the
/// runtime registry. Keeping them separate means a failed apply does not poison the baseline: the
/// signal keeps being offered for apply until a commit actually succeeds.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
struct UpgradeSignalState {
    /// Last signal read from L1 by the live observer.
    observed: Option<UpgradeSignal>,
    /// Last signal a successful apply committed to the runtime registry.
    applied: Option<UpgradeSignal>,
}

impl UpgradeSignalState {
    /// Creates an empty upgrade signal state tracker.
    const fn new() -> Self {
        Self { observed: None, applied: None }
    }

    /// Records a newly read live signal against the observed baseline.
    fn update_signal(&mut self, signal: UpgradeSignal) -> UpgradeSignalStateUpdate {
        let update = match self.observed.as_ref() {
            Some(previous) if previous.has_same_contract_values(&signal) => {
                UpgradeSignalStateUpdate::Unchanged
            }
            Some(_) => UpgradeSignalStateUpdate::Changed,
            None => UpgradeSignalStateUpdate::Initialized,
        };

        self.observed = Some(signal);
        update
    }

    /// Returns true when `signal` has not yet been successfully applied.
    fn needs_apply(&self, signal: &UpgradeSignal) -> bool {
        self.applied.as_ref().is_none_or(|applied| !applied.has_same_contract_values(signal))
    }

    /// Advances the applied baseline after a successful commit.
    fn mark_applied(&mut self, signal: &UpgradeSignal) {
        self.applied = Some(signal.clone());
    }
}

/// Records live upgrade signal metrics and, when a refresher is supplied, auto-applies observed
/// schedule changes.
#[derive(Debug, Clone)]
pub struct UpgradeSignalMonitor {
    /// Metric layer recorded by this monitor.
    pub metrics_layer: UpgradeSignalMetricLayer,
    /// Live metrics and apply state by contract-backed upgrade.
    states: BTreeMap<BaseUpgrade, UpgradeSignalState>,
    /// Contract values of the last schedule that failed to apply, used to page only on the first
    /// occurrence of a persistent failure rather than every poll.
    last_apply_failure: Option<Vec<UpgradeSignal>>,
}

impl UpgradeSignalMonitor {
    /// Creates a monitor for all contract-backed upgrades.
    pub fn new(metrics_layer: UpgradeSignalMetricLayer) -> Self {
        UpgradeSignalMetrics::init();
        let mut states = BTreeMap::new();
        for upgrade_id in BaseUpgrade::CONTRACT_VARIANTS {
            states.insert(upgrade_id, UpgradeSignalState::new());
        }
        Self { metrics_layer, states, last_apply_failure: None }
    }

    /// Tolerantly polls the reader, records live metrics, and — when `refresher` is supplied —
    /// applies any schedule not yet successfully committed.
    ///
    /// This is the single live-poll routine shared by the consensus actor and the execution
    /// metrics extension. Read failures are recorded but do not abort the poll and do not advance
    /// either baseline. The applied baseline advances only when [`UpgradeSignalRefresher::apply`]
    /// succeeds, so a failed apply leaves the schedule offered for retry on the next poll; failures
    /// increment `apply_failures_total`, raise the `apply_failed` gauge, and page once per distinct
    /// failure.
    pub async fn poll_and_apply(
        &mut self,
        reader: &AlloyUpgradeSignalReader,
        refresher: Option<&UpgradeSignalRefresher>,
    ) {
        let Some(schedule) = reader.read_schedule_tolerant(&[self.metrics_layer]).await else {
            return;
        };

        let observed_changes = self
            .update_schedule(schedule.clone())
            .iter()
            .filter(|update| update.requires_apply())
            .count();
        if observed_changes > 0 {
            info!(
                target: "upgrade_signal",
                updated_signals = observed_changes,
                "observed live L1 upgrade signal update"
            );
        }

        let Some(refresher) = refresher else {
            return;
        };
        if !self.schedule_needs_apply(&schedule) {
            return;
        }

        match refresher.apply(&schedule) {
            Ok(_) => {
                self.mark_schedule_applied(&schedule);
                self.last_apply_failure = None;
                UpgradeSignalMetrics::record_apply_success(self.metrics_layer, &schedule);
            }
            Err(apply_error) => {
                UpgradeSignalMetrics::record_apply_failure(self.metrics_layer, &schedule);
                if self.last_apply_failure.as_deref() != Some(schedule.signals.as_slice()) {
                    error!(
                        target: "upgrade_signal",
                        error = %apply_error,
                        "failed to auto-apply live upgrade signal update"
                    );
                    self.last_apply_failure = Some(schedule.signals.clone());
                } else {
                    debug!(
                        target: "upgrade_signal",
                        error = %apply_error,
                        "live upgrade signal still failing to apply"
                    );
                }
            }
        }
    }

    /// Returns true when any signal in `schedule` has not yet been successfully applied.
    fn schedule_needs_apply(&self, schedule: &UpgradeSignalSchedule) -> bool {
        schedule.signals.iter().any(|signal| {
            self.states.get(&signal.upgrade_id).is_none_or(|state| state.needs_apply(signal))
        })
    }

    /// Advances the applied baseline for every signal in a successfully committed schedule.
    fn mark_schedule_applied(&mut self, schedule: &UpgradeSignalSchedule) {
        for signal in &schedule.signals {
            self.states.entry(signal.upgrade_id).or_default().mark_applied(signal);
        }
    }

    /// Applies signals read from L1 and records corresponding live metrics.
    fn update_schedule(
        &mut self,
        schedule: UpgradeSignalSchedule,
    ) -> Vec<UpgradeSignalStateUpdate> {
        schedule
            .signals
            .into_iter()
            .map(|signal| self.update_signal(schedule.l1_block_number, signal))
            .collect()
    }

    /// Applies one signal read from L1 and records corresponding live metrics.
    fn update_signal(
        &mut self,
        l1_block_number: u64,
        signal: UpgradeSignal,
    ) -> UpgradeSignalStateUpdate {
        let upgrade_id = signal.upgrade_id;
        UpgradeSignalMetrics::record_signal(self.metrics_layer, l1_block_number, &signal);

        let update = self.states.entry(upgrade_id).or_default().update_signal(signal);
        if matches!(update, UpgradeSignalStateUpdate::Changed) {
            UpgradeSignalMetrics::record_signal_update(self.metrics_layer, upgrade_id);
        }

        update
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;

    use super::*;

    fn signal(timestamp: u64) -> UpgradeSignal {
        UpgradeSignal {
            upgrade_id: BaseUpgrade::Azul,
            activation_timestamp: timestamp,
            protocol_version: U256::from(7),
        }
    }

    #[test]
    fn signal_returns_positive_activation_timestamp() {
        assert_eq!(signal(10).positive_activation_timestamp(), Some(10));
    }

    #[test]
    fn signal_ignores_zero_activation_timestamp() {
        assert_eq!(signal(0).positive_activation_timestamp(), None);
    }

    #[test]
    fn state_initializes_then_tracks_unchanged_signal() {
        let mut state = UpgradeSignalState::new();

        assert_eq!(state.update_signal(signal(10)), UpgradeSignalStateUpdate::Initialized);
        assert_eq!(state.update_signal(signal(10)), UpgradeSignalStateUpdate::Unchanged);
    }

    #[test]
    fn state_detects_contract_value_changes() {
        let mut state = UpgradeSignalState::new();

        state.update_signal(signal(10));

        assert_eq!(state.update_signal(signal(12)), UpgradeSignalStateUpdate::Changed);
    }

    fn monitor() -> UpgradeSignalMonitor {
        UpgradeSignalMonitor::new(UpgradeSignalMetricLayer::Consensus)
    }

    fn schedule(timestamp: u64) -> UpgradeSignalSchedule {
        UpgradeSignalSchedule::new(1, vec![signal(timestamp)])
    }

    #[test]
    fn first_observation_and_change_require_apply_but_unchanged_does_not() {
        assert!(UpgradeSignalStateUpdate::Initialized.requires_apply());
        assert!(UpgradeSignalStateUpdate::Changed.requires_apply());
        assert!(!UpgradeSignalStateUpdate::Unchanged.requires_apply());
    }

    #[test]
    fn monitor_counts_first_observation_as_update() {
        let mut monitor = monitor();

        let updates = monitor.update_schedule(schedule(10));

        assert_eq!(updates, vec![UpgradeSignalStateUpdate::Initialized]);
    }

    #[test]
    fn monitor_ignores_unchanged_signal() {
        let mut monitor = monitor();

        monitor.update_schedule(schedule(10));

        assert_eq!(
            monitor.update_schedule(schedule(10)),
            vec![UpgradeSignalStateUpdate::Unchanged]
        );
    }

    #[test]
    fn monitor_ignores_l1_block_update_with_unchanged_contract_values() {
        let mut monitor = monitor();

        monitor.update_schedule(schedule(10));
        let updated_schedule = UpgradeSignalSchedule::new(2, vec![signal(10)]);

        assert_eq!(
            monitor.update_schedule(updated_schedule),
            vec![UpgradeSignalStateUpdate::Unchanged]
        );
    }

    #[test]
    fn monitor_detects_changed_signal() {
        let mut monitor = monitor();

        monitor.update_schedule(schedule(10));

        assert_eq!(monitor.update_schedule(schedule(12)), vec![UpgradeSignalStateUpdate::Changed]);
    }

    #[test]
    fn state_needs_apply_until_marked_applied() {
        let mut state = UpgradeSignalState::new();
        let signal = signal(10);

        // Never applied: needs apply even before it has been observed.
        assert!(state.needs_apply(&signal));

        // Observing the signal advances the observed baseline but not the applied baseline.
        state.update_signal(signal.clone());
        assert!(state.needs_apply(&signal));

        // Only a successful apply advances the applied baseline.
        state.mark_applied(&signal);
        assert!(!state.needs_apply(&signal));
    }

    #[test]
    fn state_changed_signal_needs_apply_after_previous_applied() {
        let mut state = UpgradeSignalState::new();

        state.mark_applied(&signal(10));

        assert!(!state.needs_apply(&signal(10)));
        assert!(state.needs_apply(&signal(12)));
    }

    #[test]
    fn failed_apply_keeps_schedule_offered_for_retry() {
        let mut monitor = monitor();
        let schedule = schedule(10);

        // Mirror `poll_and_apply`: observe first (advances the observed baseline), then a failed
        // apply must NOT advance the applied baseline, so the schedule is offered again next poll.
        monitor.update_schedule(schedule.clone());
        assert!(
            monitor.schedule_needs_apply(&schedule),
            "an unapplied schedule must remain offered for retry"
        );

        // A subsequent successful apply advances the applied baseline and stops the retries.
        monitor.mark_schedule_applied(&schedule);
        assert!(!monitor.schedule_needs_apply(&schedule));
    }

    #[test]
    fn l1_change_after_failed_apply_is_offered() {
        let mut monitor = monitor();

        // Observe v1 and leave it unapplied (apply failed).
        monitor.update_schedule(schedule(10));
        assert!(monitor.schedule_needs_apply(&schedule(10)));

        // L1 then changes to v2, which must still be offered for apply.
        monitor.update_schedule(schedule(12));
        assert!(monitor.schedule_needs_apply(&schedule(12)));
    }

    #[test]
    fn applied_schedule_is_not_reoffered() {
        let mut monitor = monitor();
        let schedule = schedule(10);

        monitor.update_schedule(schedule.clone());
        monitor.mark_schedule_applied(&schedule);

        // The same contract values, even at a new L1 block number, are not re-offered.
        let same_values_new_block = UpgradeSignalSchedule::new(2, vec![signal(10)]);
        assert!(!monitor.schedule_needs_apply(&same_values_new_block));
    }
}
