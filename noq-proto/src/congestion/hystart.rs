use crate::{Duration, Instant};

const MIN_RTT_THRESH: Duration = Duration::from_millis(4);
const MAX_RTT_THRESH: Duration = Duration::from_millis(16);
const MIN_RTT_DIVISOR: u32 = 8;
const N_RTT_SAMPLE: u32 = 8;
const CSS_GROWTH_DIVISOR: u64 = 4;
const CSS_ROUNDS: u32 = 5;

/// HyStart++ (RFC 9406) for the initial slow start. Rounds end once a packet sent after their
/// start is acknowledged, and ACKs grow the window uncapped (L = infinity) as sending is paced.
#[derive(Debug, Default, Clone)]
pub(super) struct HyStart {
    round_start: Option<Instant>,
    newest_acked_sent: Option<Instant>,
    last_round_min_rtt: Option<Duration>,
    round_min_rtt: Option<Duration>,
    round_samples: u32,
    css_baseline_and_rounds: Option<(Duration, u32)>,
}

impl HyStart {
    pub(super) fn on_ack(&mut self, ssthresh: u64, sent: Instant, bytes: u64) -> u64 {
        if ssthresh != u64::MAX {
            return bytes;
        }
        self.newest_acked_sent = self.newest_acked_sent.max(Some(sent));
        match self.css_baseline_and_rounds {
            Some(_) => bytes / CSS_GROWTH_DIVISOR,
            None => bytes,
        }
    }

    /// Takes the RTT sample of the ACK just processed and returns whether slow start is over
    pub(super) fn on_end_acks(&mut self, now: Instant) -> bool {
        let Some(sent) = self.newest_acked_sent.take() else {
            return false;
        };
        if self.round_start.is_none_or(|start| sent > start) {
            self.round_start = Some(now);
            self.last_round_min_rtt = self.round_min_rtt.take();
            self.round_samples = 0;
            if let Some((_, rounds)) = &mut self.css_baseline_and_rounds {
                *rounds += 1;
                if *rounds == CSS_ROUNDS {
                    return true;
                }
            }
        }
        if self.round_samples == N_RTT_SAMPLE {
            return false;
        }
        let rtt = now.saturating_duration_since(sent);
        let min_rtt = self.round_min_rtt.map_or(rtt, |min| min.min(rtt));
        self.round_min_rtt = Some(min_rtt);
        self.round_samples += 1;
        if self.round_samples < N_RTT_SAMPLE {
            return false;
        }
        match (self.css_baseline_and_rounds, self.last_round_min_rtt) {
            (None, Some(last)) => {
                let threshold = (last / MIN_RTT_DIVISOR).clamp(MIN_RTT_THRESH, MAX_RTT_THRESH);
                if min_rtt >= last + threshold {
                    self.css_baseline_and_rounds = Some((min_rtt, 0));
                }
            }
            (Some((baseline, _)), _) if min_rtt < baseline => self.css_baseline_and_rounds = None,
            _ => {}
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::congestion::{ControllerFactory, CubicConfig, NewRenoConfig};
    use crate::connection::RttEstimator;

    #[test]
    fn slow_start_ends_once_round_min_rtt_rises_by_threshold() {
        let ms = Duration::from_millis;
        let us = Duration::from_micros;
        let factories: [Arc<dyn ControllerFactory>; 2] = [
            Arc::new(CubicConfig::default()),
            Arc::new(NewRenoConfig::default()),
        ];
        // The threshold is an eighth of the base RTT within 4-16 ms; CSS lasts five rounds
        let css_exit_window = 12_000 + (4 * 16 + 8) * 1200 + (8 + 4 * 16 + 1) * 1200 / 4;
        for (base, rise, rise_rounds, ends) in [
            (ms(20), us(4_000), 8, true),
            (ms(20), us(3_999), 8, false),
            (ms(100), us(12_500), 8, true),
            (ms(100), us(12_499), 8, false),
            (ms(200), us(16_000), 8, true),
            (ms(200), us(15_999), 8, false),
            (ms(100), us(12_500), 1, false),
            (ms(100), Duration::ZERO, 8, false),
        ] {
            let rtts = (0..12).map(|round| {
                if (4..4 + rise_rounds).contains(&round) {
                    base + rise
                } else {
                    base
                }
            });
            for factory in &factories {
                assert_eq!(
                    ssthresh(factory.clone(), rtts.clone()),
                    ends.then_some(css_exit_window),
                    "{base:?} + {rise:?} for {rise_rounds} rounds"
                );
            }
        }
    }

    fn ssthresh(
        factory: Arc<dyn ControllerFactory>,
        round_rtts: impl Iterator<Item = Duration>,
    ) -> Option<u64> {
        let (ms, us) = (Duration::from_millis, Duration::from_micros);
        let mut now = Instant::now();
        let mut controller = factory.build(now, 1200);
        let estimator = RttEstimator::new(ms(100));
        let mut pn = 0;
        for (round, rtt) in (0..).zip(round_rtts) {
            let round_start = now;
            for i in 0..16 {
                let sent = round_start + (i + 1) * ms(1);
                now = if i < N_RTT_SAMPLE {
                    sent + rtt + (3 * i + round) % 8 * us(100)
                } else {
                    sent + rtt - us(50)
                };
                controller.on_ack(now, sent, 1200, pn, false, &estimator);
                controller.on_end_acks(now, 0, false, Some(pn));
                pn += 1;
            }
        }
        let ssthresh = controller.metrics().ssthresh.filter(|&s| s != u64::MAX);
        controller.on_congestion_event(now, now, true, false, 0, pn);
        let window = controller.window();
        controller.on_ack(now + ms(101), now + ms(1), 1200, pn, false, &estimator);
        assert_eq!(controller.window(), window + 1200, "standard slow start");
        ssthresh
    }
}
