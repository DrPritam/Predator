import sys
import json
import time
import math
import os
import random
import threading
from collections import deque
from datetime import datetime, time as dtime, timezone, timedelta

try:
    import shift
    from shift import Order as _ShiftOrder
    _HAS_SHIFT = True
except ImportError:
    shift = None
    _ShiftOrder = None
    _HAS_SHIFT = False

MY_USERNAME = os.environ.get("SHIFT_USERNAME", "")
MY_PASSWORD = os.environ.get("SHIFT_PASSWORD", "")
CFG_FILE    = os.environ.get("SHIFT_CFG",      "initiator.cfg")
SESSION_LOG = "predator_session_log.json"


class Config:
    INITIAL_CAPITAL      = 1_000_000.0
    SHARES_PER_LOT       = 100
    MARKET_OPEN          = dtime(9, 30)
    MARKET_CLOSE         = dtime(16, 0)
    SESSION_SECONDS      = 6.5 * 3600

    GAMMA                = 0.07
    K_BASE               = 38.0
    INV_SKEW_COEFF       = 0.018
    ALPHA_QI             = 0.020
    LAMBDA_FLOW          = 0.030
    FLOW_EMA_ALPHA       = 0.20
    FLOW_NORM_CAP        = 1000.0

    EWMA_LAMBDA          = 0.97
    SIGMA_FLOOR          = 0.001
    SIGMA_MAX            = 0.026
    VOL_WARMUP_TICKS     = 50

    SPREAD_MIN           = 0.02
    SPREAD_MAX           = 0.12

    Q_MAX_LOTS           = 3
    LOT_BASE             = 2
    LOT_MIN              = 1

    LIMIT_REBATE         = 0.002
    MARKET_FEE           = 0.003

    DRAWDOWN_LIMIT       = -10_000.0
    CAPITAL_RISK_FRACTION = 0.08
    SURGE_CANCEL_THRESH  = 4.0
    SURGE_HALT_THRESH    = 7.0
    SURGE_RESUME_RATIO   = 2.5

    FLASH_CRASH_WINDOW   = 30
    FLASH_CRASH_PCT      = 0.035
    FLASH_CRASH_PAUSE_S  = 8.0

    # FIX-NEW1: Separate EOD phases properly.
    # WIND_DOWN phase (300s before close): reduce lot sizes, skew toward flat
    # FLATTEN phase (120s before close): cancel all and market-flatten
    # v18 had both at 600s → 10-min total blackout; now split into 2 phases
    EOD_WIND_DOWN_START_S  = 300   # was: EOD_FLATTEN_START_S=600 (dead config)
    EOD_FLATTEN_START_S    = 120   # was: EOD_AGGRESS_START_S=600 (same as above)
    EOD_AGGRESS_START_S    = 120   # keep alias for compatibility

    QUOTE_INTERVAL_S     = 0.25
    QUOTE_MAX_AGE_LO     = 1.5
    QUOTE_MAX_AGE_HI     = 2.5

    API_REFRESH_S        = 0.30

    HAWKES_MU            = 7.0
    HAWKES_ALPHA         = 5.0
    HAWKES_BETA          = 5.0

    TARGET_FILLS         = 700
    FILL_PACE_K          = 0.20
    FILL_PACE_MIN_MULT   = 0.90
    FILL_PACE_MAX_MULT   = 1.50

    RS_WINDOW_S          = 5.0
    RS_TOXIC_THRESH      = -0.001
    RS_WIDEN_MULT        = 1.15
    RS_MAX_HISTORY       = 50
    TOXIC_DURATION_S     = 15.0
    TOXIC_SPREAD_MULT    = 1.30
    TOXIC_LOT_PENALTY    = 0.50

    # FIX-NEW4: Raised from 1000 to 5000. On AAPL, best-bid/ask size routinely
    # exceeds 1000 shares at a 1-tick spread → old threshold caused the bot to
    # ALWAYS step ahead to join best quote, completely bypassing the AS-solver
    # pricing model and exposing it to adverse selection by iceberg algos.
    MM_DETECT_SIZE       = 5000

    SCALPER_SIGMA_THRESH = 0.012
    SCALPER_QI_THRESH    = 0.25
    VOL_BURST_LO_SIGMA   = 0.015
    VOL_BURST_LO_WIDEN   = 1.10
    VOL_BURST_HI_SIGMA   = 0.020
    VOL_BURST_HI_WIDEN   = 1.20

    MOMENTUM_WINDOW      = 6
    MOMENTUM_BETA        = 0.40
    MOMENTUM_CAP         = 0.015
    SHOCK_WINDOW         = 4
    SHOCK_THRESH         = 0.02

    TREND_WINDOW         = 20
    TREND_THRESH         = 0.004
    TREND_STOP_THRESH    = 0.008
    TREND_LOT_BRAKE      = 0.50

    TELEM_INTERVAL_S     = 2.0

    TRADE_RATE_ALPHA     = 0.30
    QUEUE_FILL_THRESH_S  = 2.0
    TRADE_RATE_HI        = 1500.0
    TRADE_RATE_LO        = 400.0
    LOT_SCALE_HI         = 0.7
    LOT_SCALE_LO         = 1.3

    LAYER_MAX_INV        = 1
    LAYER_MAX_SIGMA      = 0.010
    LAYER_LOTS           = 1

    TC_TARGET_PNL          = 600.0
    TC_ATTACK_BELOW        = -200.0
    TC_DEFENSE_ABOVE       = 400.0
    TC_ATTACK_SPREAD_MULT  = 0.90
    TC_ATTACK_LOT_MULT     = 1.3
    TC_DEFENSE_SPREAD_MULT = 1.15
    TC_DEFENSE_LOT_MULT    = 0.8
    TC_SPRINT_TAU_S        = 600.0
    TC_SPRINT_PNL_THRESH   = 200.0
    TC_SPRINT_SPREAD_MULT  = 0.92
    TC_SPRINT_LOT_MULT     = 1.20

    ANTI_RL_INTERVAL_LO  = 350
    ANTI_RL_INTERVAL_HI  = 600

    AUTOCALIB_INTERVAL   = 500
    AUTOCALIB_K_MIN      = 28.0
    AUTOCALIB_K_MAX      = 60.0
    INTRA_K_INTERVAL     = 250
    INTRA_K_UP           = 1.004
    INTRA_K_DOWN         = 0.996
    INTRA_K_FILL_RATIO   = 0.80
    AUTOCALIB_G_MIN      = 0.05
    AUTOCALIB_G_MAX      = 0.16

    XROUND_K_MIN         = 28.0
    XROUND_K_MAX         = 60.0
    XROUND_G_MIN         = 0.05
    XROUND_G_MAX         = 0.20
    XROUND_AQI_MIN       = 0.015
    XROUND_AQI_MAX       = 0.050

    MIN_FILLS_SESSION    = 200
    LOG_EVERY            = 500

    SIM_TICKS_PER_SESSION = 15_000

    REGIME_VOL_THRESH    = 0.012
    REGIME_TOXIC_THRESH  = 0.020
    REGIME_TOXIC_SURGE   = 5.0
    REGIME_SMOOTH_ALPHA  = 0.15

    REGIME_CALM_GAMMA    = 0.07
    REGIME_CALM_SMULT    = 1.00
    REGIME_CALM_QMAX     = 3

    REGIME_VOL_GAMMA     = 0.10
    REGIME_VOL_SMULT     = 1.10
    REGIME_VOL_QMAX      = 2

    REGIME_TOXIC_GAMMA   = 0.15
    REGIME_TOXIC_SMULT   = 1.35
    REGIME_TOXIC_QMAX    = 1

    SPREAD_EXP_OPTIONS   = [0.95, 1.0, 1.05]
    SPREAD_EXP_EPSILON   = 0.10
    SPREAD_EXP_SWITCH_S  = 120.0

    SIG_PERF_ALPHA       = 0.02
    SIG_WEIGHT_MIN       = 0.5
    SIG_WEIGHT_MAX       = 2.0

    ALPHA_ACCEL          = 0.025
    QI_ACCEL_ALPHA       = 0.30
    ACCEL_CAP            = 0.15

    SKEW_COEFF           = 0.008
    SKEW_MAX             = 0.03

    SHARPE_SNAPSHOT_S    = 60.0
    SHARPE_MIN_SNAPS     = 10
    SHARPE_SNAP_TICKS    = 400
    SHARPE_USE_TICKS     = True

    EDGE_LOT_SCALE       = 0.80
    EDGE_QMAX_SCALE      = 0.00
    EDGE_QMAX_CAP        = 10

    DECOY_PROB           = 0.085
    DECOY_TICKS          = 6

    QCOLLAPSE_RATIO      = 0.35
    QCOLLAPSE_HOLD_S     = 0.35
    # FIX-NEW2: VACUUM_RATIO raised from 0.25 to 0.10.
    # v18 value of 0.25 meant a 25% drop in book size (common tick-to-tick noise
    # on AAPL) triggered cancel_all every cycle → near-zero fill rate.
    # 0.10 = only cancel on a genuine 90% book wipe (vacuum event), not normal noise.
    VACUUM_RATIO         = 0.10

    EARLY_WIDEN_S        = 120.0
    EARLY_WIDEN_MULT     = 1.15

    WIDEN_MAX_MULT       = 2.00
    WIDEN_MIN_MULT       = 0.75
    INV_EMERGENCY_LOTS   = 4

    # FIX-NEW3: Drawdown check supplemental params
    # Worst-case PnL can deviate from pnl_cache due to API lag.
    # Reserve buffer so halt fires before actual loss far exceeds limit.
    DRAWDOWN_BUFFER      = 500.0  # halt at DRAWDOWN_LIMIT + DRAWDOWN_BUFFER
    DRAWDOWN_HYSTERESIS  = 1000.0  # resume trading only when PnL > halt_level + hysteresis

    EOD_FLATTEN_RETRY_S  = 2.0   # retry market flatten every N seconds if still have inventory
    EMERG_FLATTEN_RETRY_S = 2.0  # retry emergency flatten every N seconds


def load_session_log(cfg):
    try:
        with open(SESSION_LOG, "r") as f:
            s = json.load(f)
        cfg.K_BASE   = float(s.get("k",        cfg.K_BASE))
        cfg.GAMMA    = float(s.get("gamma",     cfg.GAMMA))
        cfg.ALPHA_QI = float(s.get("alpha_qi",  cfg.ALPHA_QI))
        fills        = int(  s.get("fills",     cfg.TARGET_FILLS))
        rs           = float(s.get("mean_rs",   0.005))
        pnl          = float(s.get("pnl",       0.0))
        if fills >= cfg.MIN_FILLS_SESSION:
            if fills < cfg.TARGET_FILLS and rs >= 0:
                cfg.K_BASE = min(cfg.XROUND_K_MAX, cfg.K_BASE * 1.03)
            if rs < 0:
                cfg.K_BASE = max(cfg.XROUND_K_MIN, cfg.K_BASE * 0.95)
            if pnl < 0:
                cfg.K_BASE = max(cfg.XROUND_K_MIN, cfg.K_BASE * 0.97)
                cfg.GAMMA  = min(cfg.XROUND_G_MAX,  cfg.GAMMA  * 1.05)
            if 0 < rs < 0.004:
                cfg.ALPHA_QI = min(cfg.XROUND_AQI_MAX, cfg.ALPHA_QI * 1.05)
        else:
            print(f"[XROUND] prior session had only {fills} fills — skipping param adaptation",
                  flush=True)
        cfg.K_BASE   = max(cfg.XROUND_K_MIN,  min(cfg.XROUND_K_MAX,  cfg.K_BASE))
        cfg.GAMMA    = max(cfg.XROUND_G_MIN,   min(cfg.XROUND_G_MAX,  cfg.GAMMA))
        cfg.ALPHA_QI = max(cfg.XROUND_AQI_MIN, min(cfg.XROUND_AQI_MAX, cfg.ALPHA_QI))
        print(f"[XROUND] k={cfg.K_BASE:.2f} γ={cfg.GAMMA:.3f} αqi={cfg.ALPHA_QI:.3f} "
              f"fills={fills} rs={rs:.4f} pnl={pnl:.0f}", flush=True)
    except FileNotFoundError:
        print("[XROUND] no prior log — using defaults", flush=True)
    except Exception as e:
        print(f"[XROUND] load error: {e}", file=sys.stderr)


def save_session_log(cfg, fills, pnl, mean_rs, sharpe, best_exp):
    if fills < cfg.MIN_FILLS_SESSION:
        print(f"[XROUND] NOT saving — only {fills} fills (min={cfg.MIN_FILLS_SESSION})",
              flush=True)
        return
    try:
        with open(SESSION_LOG, "w") as f:
            json.dump({
                "fills":    fills,
                "pnl":      round(pnl, 2),
                "mean_rs":  round(mean_rs, 6),
                "k":        round(cfg.K_BASE, 4),
                "gamma":    round(cfg.GAMMA,  4),
                "alpha_qi": round(cfg.ALPHA_QI, 4),
                "sharpe":   round(sharpe, 4),
                "best_exp": round(best_exp, 2),
            }, f, indent=2)
        print(f"[XROUND] saved fills={fills} pnl={pnl:.0f} rs={mean_rs:.4f} "
              f"sharpe={sharpe:.3f} best_exp={best_exp:.1f}", flush=True)
    except Exception as e:
        print(f"[XROUND] save error: {e}", file=sys.stderr)


class VolEstimator:
    def __init__(self, cfg):
        self._cfg  = cfg
        self._var  = cfg.SIGMA_FLOOR ** 2
        self._prev = None
        self.sigma = cfg.SIGMA_FLOOR
        self.n     = 0

    def update(self, mid):
        if self._prev is not None and self._prev > 0:
            ret        = (mid - self._prev) / self._prev
            lam        = self._cfg.EWMA_LAMBDA
            self._var  = lam * self._var + (1.0 - lam) * ret * ret
            self.sigma = math.sqrt(max(self._var, self._cfg.SIGMA_FLOOR ** 2))
            self.sigma = min(self.sigma, self._cfg.SIGMA_MAX)
            self.n    += 1
        self._prev = mid
        return self.sigma

    @property
    def warmed_up(self):
        return self.n >= self._cfg.VOL_WARMUP_TICKS


class MomentumFilter:
    def __init__(self, cfg):
        self._cfg     = cfg
        self._micro_h = deque(maxlen=cfg.MOMENTUM_WINDOW)
        self._mid_h   = deque(maxlen=cfg.SHOCK_WINDOW)

    def update(self, microprice, mid):
        self._micro_h.append(microprice)
        self._mid_h.append(mid)
        shock = (abs(self._mid_h[-1] - self._mid_h[0])
                 if len(self._mid_h) == self._cfg.SHOCK_WINDOW else 0.0)
        if shock > self._cfg.SHOCK_THRESH:
            return 0.0
        if len(self._micro_h) < 2:
            return 0.0
        raw = self._micro_h[-1] - self._micro_h[0]
        return max(-self._cfg.MOMENTUM_CAP,
                   min(self._cfg.MOMENTUM_CAP, self._cfg.MOMENTUM_BETA * raw))


class TrendBrake:
    def __init__(self, cfg):
        self._cfg      = cfg
        self._mids     = deque(maxlen=cfg.TREND_WINDOW)
        self._last_tbf = 1.0
        self._last_taf = 1.0

    def update(self, mid):
        self._mids.append(mid)
        if len(self._mids) < self._cfg.TREND_WINDOW:
            self._last_tbf, self._last_taf = 1.0, 1.0
            return 1.0, 1.0
        ref = self._mids[0]
        if ref <= 0:
            self._last_tbf, self._last_taf = 1.0, 1.0
            return 1.0, 1.0
        drift  = (self._mids[-1] - ref) / ref
        thresh = self._cfg.TREND_THRESH
        stop   = self._cfg.TREND_STOP_THRESH
        brake  = self._cfg.TREND_LOT_BRAKE
        if drift > stop:
            tbf, taf = 1.0, 0.0
        elif drift > thresh:
            tbf, taf = 1.0, brake
        elif drift < -stop:
            tbf, taf = 0.0, 1.0
        elif drift < -thresh:
            tbf, taf = brake, 1.0
        else:
            tbf, taf = 1.0, 1.0
        self._last_tbf, self._last_taf = tbf, taf
        return tbf, taf


class TelemetryLogger:
    def __init__(self, filename):
        self._f = open(filename, "a")

    def log(self, rec):
        try:
            self._f.write(json.dumps(rec) + "\n")
            self._f.flush()
        except Exception:
            pass

    def close(self):
        try:
            self._f.close()
        except Exception:
            pass


class QueueEstimator:
    def __init__(self, cfg):
        self._cfg        = cfg
        self._trade_rate = 500.0
        self._pending    = 0
        self._lock       = threading.Lock()

    def on_fill(self, shares):
        with self._lock:
            self._pending += shares

    def update(self):
        with self._lock:
            raw           = float(self._pending)
            self._pending = 0
        self._trade_rate = (self._cfg.TRADE_RATE_ALPHA * raw
                            + (1.0 - self._cfg.TRADE_RATE_ALPHA) * self._trade_rate)
        self._trade_rate = max(self._trade_rate, 1.0)

    def should_step_ahead(self, queue_size, spread):
        if spread < 0.02 or queue_size <= 0:
            return False
        expected_wait = queue_size / self._trade_rate
        fill_prob = 1.0 - math.exp(
            -self._cfg.QUEUE_FILL_THRESH_S / max(expected_wait, 1e-9))
        return fill_prob < 0.50

    def lot_scale(self):
        r = self._trade_rate
        hi = self._cfg.TRADE_RATE_HI
        lo = self._cfg.TRADE_RATE_LO
        hi_sc = self._cfg.LOT_SCALE_HI
        lo_sc = self._cfg.LOT_SCALE_LO
        if r >= hi:
            return hi_sc
        if r <= lo:
            return lo_sc
        frac = (r - lo) / (hi - lo)
        return lo_sc + frac * (hi_sc - lo_sc)


class RegimeDetector:
    CALM     = "CALM"
    VOLATILE = "VOLATILE"
    TOXIC    = "TOXIC"

    def __init__(self, cfg):
        self._cfg    = cfg
        self.regime  = self.CALM
        self._smooth = cfg.SIGMA_FLOOR

    def update(self, sigma, surge):
        cfg           = self._cfg
        a             = cfg.REGIME_SMOOTH_ALPHA
        self._smooth  = (1.0 - a) * self._smooth + a * sigma
        s             = self._smooth
        if surge > cfg.REGIME_TOXIC_SURGE or s > cfg.REGIME_TOXIC_THRESH:
            self.regime = self.TOXIC
        elif s > cfg.REGIME_VOL_THRESH:
            self.regime = self.VOLATILE
        else:
            self.regime = self.CALM
        return self.regime

    def gamma(self):
        cfg = self._cfg
        if self.regime == self.TOXIC:    return cfg.REGIME_TOXIC_GAMMA
        if self.regime == self.VOLATILE: return cfg.REGIME_VOL_GAMMA
        return cfg.REGIME_CALM_GAMMA

    def spread_mult(self):
        cfg = self._cfg
        if self.regime == self.TOXIC:    return cfg.REGIME_TOXIC_SMULT
        if self.regime == self.VOLATILE: return cfg.REGIME_VOL_SMULT
        return cfg.REGIME_CALM_SMULT

    def q_max(self):
        cfg = self._cfg
        if self.regime == self.TOXIC:    return cfg.REGIME_TOXIC_QMAX
        if self.regime == self.VOLATILE: return cfg.REGIME_VOL_QMAX
        return cfg.REGIME_CALM_QMAX

    def edge_scale(self):
        if self.regime == self.TOXIC:    return 0.20
        if self.regime == self.VOLATILE: return 0.60
        return 1.0


class SpreadExplorer:
    def __init__(self, cfg):
        self._options     = list(cfg.SPREAD_EXP_OPTIONS)
        self._epsilon     = cfg.SPREAD_EXP_EPSILON
        self._switch_ticks = max(50, int(cfg.SPREAD_EXP_SWITCH_S
                                         * cfg.SIM_TICKS_PER_SESSION
                                         / max(cfg.SESSION_SECONDS, 1)))
        self._scores      = {o: 0.0 for o in self._options}
        self._counts      = {o: 1   for o in self._options}
        self.current      = 1.0
        self._last_switch_tick = 0
        self._tick        = 0

    def seed(self, best_exp):
        if best_exp in self._options:
            self.current = best_exp
            for o in self._options:
                self._scores[o] = 1.0 if o == best_exp else 0.0

    def choose(self):
        self._tick += 1
        if self._tick - self._last_switch_tick < max(self._switch_ticks, 1):
            return self.current
        if random.random() < self._epsilon:
            self.current = random.choice(self._options)
        else:
            self.current = max(self._options,
                               key=lambda o: self._scores[o] / self._counts[o])
        self._last_switch_tick = self._tick
        return self.current

    def update(self, pnl_delta):
        o = self.current
        self._scores[o] += pnl_delta
        self._counts[o] += 1

    def best(self):
        return max(self._options, key=lambda o: self._scores[o] / self._counts[o])


class SignalPerformance:
    def __init__(self, cfg):
        self._alpha    = cfg.SIG_PERF_ALPHA
        self._w_min    = cfg.SIG_WEIGHT_MIN
        self._w_max    = cfg.SIG_WEIGHT_MAX
        self.qi_score  = 0.0
        self.fl_score  = 0.0
        self.mom_score = 0.0

    def update(self, qi, flow_norm, momentum, rs):
        a = self._alpha
        self.qi_score  = (1.0 - a) * self.qi_score  + a * rs * qi
        self.fl_score  = (1.0 - a) * self.fl_score  + a * rs * flow_norm
        self.mom_score = (1.0 - a) * self.mom_score + a * rs * momentum

    def weights(self):
        total = (abs(self.qi_score) + abs(self.fl_score)
                 + abs(self.mom_score) + 1e-9)
        w_qi  = self.qi_score  / total
        w_fl  = self.fl_score  / total
        w_mom = self.mom_score / total
        def clamp(w):
            if w < 0:
                return max(0.0, 1.0 + w)
            return max(self._w_min, min(self._w_max, 1.0 + w))
        return clamp(w_qi), clamp(w_fl), clamp(w_mom)


class SharpeTracker:
    def __init__(self, cfg):
        self._snap_s      = cfg.SHARPE_SNAPSHOT_S
        self._snap_ticks  = cfg.SHARPE_SNAP_TICKS
        self._use_ticks   = cfg.SHARPE_USE_TICKS
        self._min_s       = cfg.SHARPE_MIN_SNAPS
        self._cfg         = cfg
        self._snaps       = deque(maxlen=500)
        self._last_pnl    = 0.0
        self._last_t      = time.time()
        self._tick_count  = 0
        self._last_tick   = 0
        self.sharpe       = 0.0

    def update(self, pnl):
        self._tick_count += 1
        if self._use_ticks:
            if self._tick_count - self._last_tick >= self._snap_ticks:
                self._snaps.append(pnl - self._last_pnl)
                self._last_pnl  = pnl
                self._last_tick = self._tick_count
                self._recalc()
        else:
            now = time.time()
            if now - self._last_t >= self._snap_s:
                self._snaps.append(pnl - self._last_pnl)
                self._last_pnl = pnl
                self._last_t   = now
                self._recalc()

    def _recalc(self):
        n = len(self._snaps)
        if n < self._min_s:
            return
        vals = list(self._snaps)
        mean = sum(vals) / n
        var  = sum((x - mean) ** 2 for x in vals) / n
        std  = math.sqrt(max(var, 1e-12))
        if self._use_ticks:
            snaps_per_session = (self._cfg.SIM_TICKS_PER_SESSION
                                 / max(self._snap_ticks, 1))
        else:
            snaps_per_session = (self._cfg.SESSION_SECONDS
                                 / max(self._snap_s, 1))
        periods_per_year = 252.0 * snaps_per_session
        self.sharpe = (mean / std) * math.sqrt(periods_per_year)


class ASSolver:
    TICK = 0.01

    def __init__(self, cfg):
        self._cfg = cfg

    def compute(self, microprice, sigma, q_lots, tau,
                qi_lean=0.0, flow_lean=0.0, momentum_adj=0.0, qi_accel=0.0,
                widen_mult=1.0, pace_mult=1.0,
                regime_gamma=None, w_qi=1.0, w_flow=1.0, w_mom=1.0):
        cfg      = self._cfg
        gamma    = regime_gamma if regime_gamma is not None else cfg.GAMMA
        k        = cfg.K_BASE
        tau_norm = max(tau / max(cfg.SESSION_SECONDS, 1.0), 1e-4)
        sqrt_tau = math.sqrt(tau_norm)

        r = (microprice
             - q_lots * cfg.INV_SKEW_COEFF
             + cfg.ALPHA_QI    * w_qi   * qi_lean
             + cfg.LAMBDA_FLOW * w_flow * flow_lean
             + w_mom * momentum_adj
             + cfg.ALPHA_ACCEL * qi_accel)

        base_half = ((gamma * sigma * sigma * sqrt_tau) / 2.0
                     + math.log(1.0 + gamma / max(k, 0.001)) / gamma)
        base_half = max(cfg.SPREAD_MIN / 2.0, base_half - cfg.LIMIT_REBATE)

        half = base_half * widen_mult * pace_mult
        half = max(cfg.SPREAD_MIN / 2.0, min(cfg.SPREAD_MAX / 2.0, half))

        bid = round(math.floor((r - half) / self.TICK) * self.TICK, 2)
        ask = round(math.ceil( (r + half) / self.TICK) * self.TICK, 2)
        if ask <= bid:
            ask = round(bid + self.TICK, 2)
        return bid, ask


class HawkesSurge:
    def __init__(self, cfg):
        self._mu     = cfg.HAWKES_MU
        self._alpha  = cfg.HAWKES_ALPHA
        self._beta   = cfg.HAWKES_BETA
        self._lam    = cfg.HAWKES_MU
        self._last_t = None

    def on_event(self, sim_now=None):
        now          = sim_now if sim_now is not None else time.time()
        if self._last_t is None:
            self._last_t = now
        dt           = max(now - self._last_t, 1e-9)
        self._lam    = (self._mu
                        + (self._lam - self._mu) * math.exp(-self._beta * dt)
                        + self._alpha)
        self._last_t = now

    def tick(self, sim_now=None):
        now          = sim_now if sim_now is not None else time.time()
        if self._last_t is None:
            self._last_t = now
        dt           = max(now - self._last_t, 1e-9)
        self._lam    = self._mu + (self._lam - self._mu) * math.exp(-self._beta * dt)
        self._last_t = now
        return self._lam / max(self._mu, 1e-6)


class FillPaceController:
    def __init__(self, cfg):
        self._cfg  = cfg
        self.start = time.time()

    def _elapsed(self, tau=None):
        if tau is not None:
            return max(self._cfg.SESSION_SECONDS - tau, 1.0)
        return max(time.time() - self.start, 1.0)

    def multiplier(self, fill_count, tau=None):
        elapsed  = self._elapsed(tau)
        expected = self._cfg.TARGET_FILLS * (elapsed / self._cfg.SESSION_SECONDS)
        error    = expected - fill_count
        mult     = 1.0 - self._cfg.FILL_PACE_K * (error / max(self._cfg.TARGET_FILLS, 1))
        return max(self._cfg.FILL_PACE_MIN_MULT, min(self._cfg.FILL_PACE_MAX_MULT, mult))

    def projected_fills(self, fill_count, tau=None):
        elapsed = self._elapsed(tau)
        return fill_count / elapsed * self._cfg.SESSION_SECONDS


class RealizedSpreadMonitor:
    def __init__(self, cfg):
        self._cfg     = cfg
        self._pending = deque()
        self._history = deque(maxlen=cfg.RS_MAX_HISTORY)
        self._lock    = threading.Lock()

    def record(self, side, price, mid_at_fill, sim_now=None):
        now = sim_now if sim_now is not None else time.time()
        with self._lock:
            self._pending.append((side, price, mid_at_fill, now))

    def update(self, current_mid, sim_now=None):
        now = sim_now if sim_now is not None else time.time()
        with self._lock:
            while (self._pending
                   and (now - self._pending[0][3]) > self._cfg.RS_WINDOW_S):
                side, price, mid_at_fill, _ = self._pending.popleft()
                rs = (mid_at_fill - price) if side == 'BID' else (price - mid_at_fill)
                self._history.append(rs)

    def mean_rs(self):
        if not self._history:
            return 0.005
        return sum(self._history) / len(self._history)

    def is_toxic(self):
        return self.mean_rs() < self._cfg.RS_TOXIC_THRESH


class TournamentController:
    def __init__(self, cfg):
        self._cfg   = cfg
        self.mode   = "NORMAL"

    def update(self, pnl, tau):
        cfg     = self._cfg
        elapsed = max(cfg.SESSION_SECONDS - tau, 0.0)
        exp     = cfg.TC_TARGET_PNL * (elapsed / max(cfg.SESSION_SECONDS, 1.0))
        delta   = pnl - exp
        if delta < cfg.TC_ATTACK_BELOW:
            self.mode = "ATTACK"
        elif delta > cfg.TC_DEFENSE_ABOVE:
            self.mode = "DEFENSE"
        elif tau < cfg.TC_SPRINT_TAU_S and pnl < cfg.TC_SPRINT_PNL_THRESH:
            self.mode = "SPRINT"
        else:
            self.mode = "NORMAL"

    def spread_mult(self):
        m = self.mode
        if m == "ATTACK":  return self._cfg.TC_ATTACK_SPREAD_MULT
        if m == "DEFENSE": return self._cfg.TC_DEFENSE_SPREAD_MULT
        if m == "SPRINT":  return self._cfg.TC_SPRINT_SPREAD_MULT
        return 1.0

    def lot_mult(self):
        m = self.mode
        if m == "ATTACK":  return self._cfg.TC_ATTACK_LOT_MULT
        if m == "DEFENSE": return self._cfg.TC_DEFENSE_LOT_MULT
        if m == "SPRINT":  return self._cfg.TC_SPRINT_LOT_MULT
        return 1.0


class AutoCalibrator:
    def __init__(self, cfg):
        self._cfg   = cfg
        self._ticks = 0
        # FIX-NEW5: Shared lock between AutoCalibrator and IntraSessionKAdapter
        # to prevent K_BASE race conditions when both fire near-simultaneously.
        self._lock  = None  # assigned after construction — see PredatorBot.__init__

    def update(self, rs, fill_proj, inv_var):
        self._ticks += 1
        if self._ticks % self._cfg.AUTOCALIB_INTERVAL != 0:
            return
        cfg = self._cfg
        lock = self._lock
        with (lock if lock else _nullcontext()):
            if rs < 0:
                cfg.K_BASE = max(cfg.AUTOCALIB_K_MIN, cfg.K_BASE * 0.97)
            elif rs > 0.003 and fill_proj >= cfg.TARGET_FILLS:
                cfg.K_BASE = min(cfg.AUTOCALIB_K_MAX, cfg.K_BASE * 1.02)
            elif fill_proj < cfg.TARGET_FILLS:
                cfg.K_BASE = min(cfg.AUTOCALIB_K_MAX, cfg.K_BASE * 1.01)
            elif fill_proj > cfg.TARGET_FILLS * 1.3 and rs > 0:
                cfg.K_BASE = max(cfg.AUTOCALIB_K_MIN, cfg.K_BASE * 0.99)
            cfg.K_BASE = max(cfg.AUTOCALIB_K_MIN, min(cfg.AUTOCALIB_K_MAX, cfg.K_BASE))
            if inv_var > 4.0:
                cfg.GAMMA = min(cfg.AUTOCALIB_G_MAX, cfg.GAMMA * 1.03)
            else:
                cfg.GAMMA = max(cfg.AUTOCALIB_G_MIN, cfg.GAMMA * 0.995)


class _nullcontext:
    """Minimal context manager shim for Python < 3.7 (no contextlib.nullcontext)."""
    def __enter__(self): return self
    def __exit__(self, *a): pass


class IntraSessionKAdapter:
    def __init__(self, cfg):
        self._cfg   = cfg
        self._ticks = 0
        self._lock  = None  # assigned after construction

    def update(self, fill_proj, rs_mean):
        self._ticks += 1
        if self._ticks % self._cfg.INTRA_K_INTERVAL != 0:
            return
        cfg = self._cfg
        lock = self._lock
        with (lock if lock else _nullcontext()):
            if rs_mean < 0:
                cfg.K_BASE = max(cfg.AUTOCALIB_K_MIN, cfg.K_BASE * cfg.INTRA_K_DOWN)
            elif rs_mean > 0.003 and fill_proj >= cfg.TARGET_FILLS:
                cfg.K_BASE = min(cfg.AUTOCALIB_K_MAX, cfg.K_BASE * cfg.INTRA_K_UP)
            if fill_proj < cfg.INTRA_K_FILL_RATIO * cfg.TARGET_FILLS:
                cfg.K_BASE = min(cfg.AUTOCALIB_K_MAX, cfg.K_BASE * cfg.INTRA_K_UP)
            elif fill_proj > cfg.TARGET_FILLS * 1.3 and rs_mean > 0:
                cfg.K_BASE = max(cfg.AUTOCALIB_K_MIN, cfg.K_BASE * cfg.INTRA_K_DOWN)
            cfg.K_BASE = max(cfg.AUTOCALIB_K_MIN, min(cfg.AUTOCALIB_K_MAX, cfg.K_BASE))


class QuoteEngine:
    @staticmethod
    def _order_type(side_or_str, is_market=False):
        buy = side_or_str == 'BID'
        if _HAS_SHIFT:
            if is_market:
                return (_ShiftOrder.Type.MARKET_BUY if buy
                        else _ShiftOrder.Type.MARKET_SELL)
            return (_ShiftOrder.Type.LIMIT_BUY if buy
                    else _ShiftOrder.Type.LIMIT_SELL)
        else:
            from multi_agent_sim import Order as _O
            if is_market:
                return _O.Type.MARKET_BUY if buy else _O.Type.MARKET_SELL
            return _O.Type.LIMIT_BUY if buy else _O.Type.LIMIT_SELL

    @staticmethod
    def _make_order(otype, ticker, lots, price=None):
        if _HAS_SHIFT:
            if price is not None:
                return _ShiftOrder(otype, ticker, lots, price)
            return _ShiftOrder(otype, ticker, lots)
        else:
            from multi_agent_sim import Order as _O
            if price is not None:
                return _O(otype, ticker, lots, price)
            return _O(otype, ticker, lots)

    @staticmethod
    def cancel_all(trader, ticker):
        try:
            if trader.get_waiting_list_size() == 0:
                return
            for o in list(trader.get_waiting_list()):
                if o.symbol == ticker:
                    trader.submit_cancellation(o)
        except Exception as e:
            print(f"[QE.cancel] {e}", file=sys.stderr)

    @staticmethod
    def submit_limit(trader, ticker, side, price, lots):
        if price <= 0 or lots <= 0:
            return
        try:
            otype = QuoteEngine._order_type(side, is_market=False)
            trader.submit_order(QuoteEngine._make_order(otype, ticker, lots, round(price, 2)))
        except Exception as e:
            print(f"[QE.limit] {e}", file=sys.stderr)

    @staticmethod
    def market_flatten(trader, ticker, q_lots):
        if q_lots == 0:
            return
        # FIX-NEW6: q_lots is already in LOTS (integer). SHIFT API submit_order
        # constructor takes lots, not shares. Pass abs(q_lots) directly.
        # Added explicit assertion + print to catch any future unit confusion.
        side = 'ASK' if q_lots > 0 else 'BID'
        lots_to_send = abs(q_lots)
        assert lots_to_send > 0, f"market_flatten: zero lots — q_lots={q_lots}"
        print(f"[QE.flatten] side={side} lots={lots_to_send}", flush=True)
        try:
            otype = QuoteEngine._order_type(side, is_market=True)
            trader.submit_order(QuoteEngine._make_order(otype, ticker, lots_to_send))
        except Exception as e:
            print(f"[QE.flatten] {e}", file=sys.stderr)


class FillCallback:
    def __init__(self, hawkes, rs_monitor, queue_est, ticker):
        self._hawkes    = hawkes
        self._rs        = rs_monitor
        self._qe        = queue_est
        self._ticker    = ticker
        self.fill_count = 0
        self._lock      = threading.Lock()
        self._sim_now   = None
        self._current_mid = None

    def __call__(self, trader, order_id):
        try:
            order = trader.get_order(order_id)
            if order is None or order.symbol != self._ticker:
                return
            # FIX-NEW7: Robust qty extraction — try multiple SHIFT API attribute names.
            # If both return 0 or raise, skip this callback to avoid silently freezing
            # fill_count at 0, which would cause FillPaceController to always widen spreads.
            qty = 0
            for attr in ('executed_quantity', 'executed_size', 'quantity', 'size'):
                try:
                    v = getattr(order, attr, None)
                    if v is not None and int(v) > 0:
                        qty = int(v)
                        break
                except Exception:
                    continue
            if qty == 0:
                print(f"[FillCB] WARNING: qty=0 after trying all attrs for order {order_id} "
                      f"— fills may not be tracked!", file=sys.stderr)
                return
            self._hawkes.on_event(sim_now=self._sim_now)
            self._qe.on_fill(qty * 100)
            try:
                t = order.type
                type_str = t.name if hasattr(t, 'name') else str(t)
                is_buy = 'BUY' in type_str.upper()
                side = 'BID' if is_buy else 'ASK'
            except Exception:
                side = 'BID'
            mid_now = self._current_mid if self._current_mid is not None else float(order.price)
            self._rs.record(side, float(order.price), mid_now, sim_now=self._sim_now)
            with self._lock:
                self.fill_count += 1
        except Exception as e:
            print(f"[FillCB] {e}", file=sys.stderr)


class PredatorBot:
    def __init__(self, ticker, cfg, fill_cb):
        self._ticker  = ticker
        self._cfg     = cfg
        self._fill_cb = fill_cb

        self._vol        = VolEstimator(cfg)
        self._solver     = ASSolver(cfg)
        self._momentum   = MomentumFilter(cfg)
        self._hawkes     = fill_cb._hawkes
        self._rs         = fill_cb._rs
        self._qe         = fill_cb._qe
        self._qeng       = QuoteEngine()
        self._pace       = FillPaceController(cfg)
        self._calib      = AutoCalibrator(cfg)
        self._tc         = TournamentController(cfg)
        self._k_adapter  = IntraSessionKAdapter(cfg)
        self._regime     = RegimeDetector(cfg)
        self._spread_exp = SpreadExplorer(cfg)
        self._sig_perf   = SignalPerformance(cfg)
        self._sharpe     = SharpeTracker(cfg)
        self._trend_brake = TrendBrake(cfg)

        # FIX-NEW5: Shared K_BASE lock wired into both calibrators
        self._k_lock = threading.Lock()
        self._calib._lock     = self._k_lock
        self._k_adapter._lock = self._k_lock

        telem_file = f"predator_telem_{datetime.now().strftime('%Y%m%d')}.jsonl"
        self._logger     = TelemetryLogger(telem_file)
        self._last_telem_t = 0.0

        self._flow_ema    = 0.0
        self._prev_bid_sz = None
        self._prev_ask_sz = None
        self._prev_qi     = 0.0
        self._qi_accel    = 0.0

        self._last_bid_sz = None
        self._last_ask_sz = None

        self._mid_window = deque(maxlen=cfg.FLASH_CRASH_WINDOW)
        self._inv_window = deque(maxlen=60)

        self._pnl_cache  = 0.0
        self._q_cache    = 0
        self._api_t      = 0.0
        self._last_pnl   = 0.0

        self._tick_n          = 0
        self._last_quote_t    = 0.0
        self._quote_expiry    = 2.0
        self._halted          = False
        self._surge_halt      = False
        self._flash_until     = 0.0
        self._scalper_mode    = False
        self._toxic_until     = 0.0
        self._queue_hold_until = 0.0

        self._last_bid     = None
        self._last_ask     = None
        self._rl_offset    = 0.0
        self._next_rl_tick = random.randint(cfg.ANTI_RL_INTERVAL_LO,
                                            cfg.ANTI_RL_INTERVAL_HI)
        self._eod_flatten_done = False
        self._eod_flatten_last_t = 0.0
        self._inv_emerg_flatten_done = False
        self._inv_emerg_flatten_last_t = 0.0

        # FIX-NEW3: Track minimum pnl seen this session for drawdown buffering
        self._pnl_min_seen = 0.0

    @property
    def _last_bid_price(self):
        return self._last_bid

    @property
    def _last_ask_price(self):
        return self._last_ask

    @property
    def _flash_crash_until(self):
        return self._flash_until

    @property
    def _churn_pause_until(self):
        return self._queue_hold_until

    @property
    def fill_count(self):
        return self._fill_cb.fill_count

    @property
    def tick_count(self):
        return self._tick_n

    def _sim_now(self):
        if _HAS_SHIFT:
            return time.time()
        tick_s = self._cfg.SESSION_SECONDS / max(self._cfg.SIM_TICKS_PER_SESSION, 1)
        return self._pace.start + self._tick_n * tick_s

    @staticmethod
    def _eastern_now():
        utc_now = datetime.now(timezone.utc)
        m = utc_now.month
        d = utc_now.day
        h = utc_now.hour
        if m in range(4, 11):
            offset_h = -4
        elif m == 3:
            first_sunday  = 1 + (6 - datetime(utc_now.year, 3, 1).weekday()) % 7
            second_sunday = first_sunday + 7
            # DST springs forward at 2:00 AM local (= 7:00 AM UTC)
            if d > second_sunday:
                offset_h = -4
            elif d == second_sunday:
                offset_h = -4 if h >= 7 else -5
            else:
                offset_h = -5
        elif m == 11:
            first_sunday = 1 + (6 - datetime(utc_now.year, 11, 1).weekday()) % 7
            # DST falls back at 2:00 AM local (= 6:00 AM UTC)
            if d > first_sunday:
                offset_h = -5
            elif d == first_sunday:
                offset_h = -5 if h >= 6 else -4
            else:
                offset_h = -4
        else:
            offset_h = -5
        eastern = utc_now + timedelta(hours=offset_h)
        return eastern

    def _tau(self):
        if _HAS_SHIFT:
            t     = self._eastern_now().time()
            now_s = t.hour * 3600 + t.minute * 60 + t.second
            cls_s = (self._cfg.MARKET_CLOSE.hour * 3600
                     + self._cfg.MARKET_CLOSE.minute * 60)
            return float(max(cls_s - now_s, 0))
        elapsed = self._tick_n * (self._cfg.SESSION_SECONDS
                                  / max(self._cfg.SIM_TICKS_PER_SESSION, 1))
        return float(max(self._cfg.SESSION_SECONDS - elapsed, 0))

    def _market_open(self):
        if _HAS_SHIFT:
            t = self._eastern_now().time()
            return self._cfg.MARKET_OPEN <= t < self._cfg.MARKET_CLOSE
        return self._tick_n < self._cfg.SIM_TICKS_PER_SESSION

    def _refresh_portfolio(self, trader):
        now = time.time()
        if now - self._api_t < self._cfg.API_REFRESH_S:
            return
        try:
            if hasattr(trader, '_mark_to_market'):
                self._pnl_cache = trader._mark_to_market()
            else:
                realized = float(trader.get_portfolio_summary().get_total_realized_pl())
                try:
                    unrealized = float(
                        trader.get_portfolio_item(self._ticker).get_unrealized_pl())
                except (AttributeError, Exception):
                    unrealized = 0.0
                self._pnl_cache = realized + unrealized
            # FIX-NEW3: Track minimum pnl to detect intra-refresh drawdown spikes
            self._pnl_min_seen = min(self._pnl_min_seen, self._pnl_cache)
        except Exception as e:
            print(f"[PORTFOLIO] PnL refresh error: {e}", file=sys.stderr)
        try:
            shares = trader.get_portfolio_item(self._ticker).get_shares()
            self._q_cache = int(shares // self._cfg.SHARES_PER_LOT)
            if self._q_cache == 0:
                self._inv_emerg_flatten_done = False
            if self._q_cache == 0:
                self._eod_flatten_done = False
        except Exception as e:
            print(f"[PORTFOLIO] inventory refresh error: {e}", file=sys.stderr)
        self._api_t = now

    def _ofi(self, bid_sz, ask_sz):
        if self._prev_bid_sz is None:
            self._prev_bid_sz = bid_sz
            self._prev_ask_sz = ask_sz
            return 0.0
        delta             = ((bid_sz - self._prev_bid_sz)
                             - (ask_sz - self._prev_ask_sz))
        a                 = self._cfg.FLOW_EMA_ALPHA
        self._flow_ema    = a * delta + (1.0 - a) * self._flow_ema
        self._prev_bid_sz = bid_sz
        self._prev_ask_sz = ask_sz
        return self._flow_ema

    def _microprice(self, bid_p, ask_p, bid_sz, ask_sz):
        total = bid_sz + ask_sz
        if total <= 0:
            return (bid_p + ask_p) / 2.0
        return (ask_p * bid_sz + bid_p * ask_sz) / total

    def _qi(self, bid_sz, ask_sz):
        total = bid_sz + ask_sz
        if total <= 0:
            return 0.0
        return (bid_sz - ask_sz) / total

    def _qi_acceleration(self, qi):
        raw            = qi - self._prev_qi
        a              = self._cfg.QI_ACCEL_ALPHA
        self._qi_accel = a * raw + (1.0 - a) * self._qi_accel
        self._prev_qi  = qi
        cap            = self._cfg.ACCEL_CAP
        return max(-cap, min(cap, self._qi_accel))

    def _normalize_flow(self, flow_raw):
        cap = self._cfg.FLOW_NORM_CAP
        return max(-1.0, min(1.0, flow_raw / cap))

    def _edge_score(self, qi, flow_norm, momentum_adj, toxic_active):
        cfg   = self._cfg
        score = min(1.0,
                    0.40 * abs(qi)
                    + 0.35 * abs(flow_norm)
                    + 0.25 * abs(momentum_adj) / max(cfg.MOMENTUM_CAP, 1e-9))
        if toxic_active:
            score = min(score, 0.25)
        return score

    def _apply_liquidity_skew(self, bid, ask, qi, flow_norm, w_qi, w_flow):
        cfg         = self._cfg
        skew_signal = w_qi * qi + w_flow * flow_norm
        skew        = max(-cfg.SKEW_MAX, min(cfg.SKEW_MAX, cfg.SKEW_COEFF * skew_signal))
        skew        = round(skew, 3)
        if skew > 0:
            bid = round(bid + skew, 2)
            ask = round(ask + skew * 0.5, 2)
        elif skew < 0:
            bid = round(bid + skew * 0.5, 2)
            ask = round(ask + skew, 2)
        if ask <= bid:
            ask = round(bid + 0.01, 2)
        return bid, ask

    def _lots(self, q_lots, tc_lot_mult, q_max, toxic_penalty):
        cfg      = self._cfg
        ratio    = q_lots / max(q_max, 1)
        base_bid = cfg.LOT_BASE * max(0.2, 1.0 - ratio)
        base_ask = cfg.LOT_BASE * max(0.2, 1.0 + ratio)
        liq_sc   = self._qe.lot_scale()
        eff_mult = tc_lot_mult * toxic_penalty
        bid_l    = max(cfg.LOT_MIN, int(round(base_bid * liq_sc * eff_mult)))
        ask_l    = max(cfg.LOT_MIN, int(round(base_ask * liq_sc * eff_mult)))

        add_bid_cap = max(0, q_max - q_lots)
        add_ask_cap = max(0, q_max + q_lots)
        if toxic_penalty < 1.0:
            bid_l = int(round(base_bid * liq_sc * eff_mult))
            ask_l = int(round(base_ask * liq_sc * eff_mult))

        if add_bid_cap > 0:
            bid_l = min(max(0 if toxic_penalty < 1.0 else cfg.LOT_MIN, bid_l), add_bid_cap)
        else:
            bid_l = 0
        if add_ask_cap > 0:
            ask_l = min(max(0 if toxic_penalty < 1.0 else cfg.LOT_MIN, ask_l), add_ask_cap)
        else:
            ask_l = 0
        return bid_l, ask_l

    def _is_flash_crash(self, mid):
        self._mid_window.append(mid)
        if len(self._mid_window) < self._cfg.FLASH_CRASH_WINDOW:
            return False
        first = self._mid_window[0]
        last  = self._mid_window[-1]
        if first <= 0:
            return False
        # Detect both flash crashes (drops) and flash rallies (spikes)
        net_move = abs(first - last) / first
        return net_move > self._cfg.FLASH_CRASH_PCT

    def _detect_competing_mm(self, spread, bid_sz, ask_sz):
        return (spread <= 0.02
                and bid_sz > self._cfg.MM_DETECT_SIZE
                and ask_sz > self._cfg.MM_DETECT_SIZE)

    def _apply_liquidity_dominance(self, bid_p, ask_p, bid_sz, ask_sz,
                                   my_bid, my_ask, spread):
        if self._detect_competing_mm(spread, bid_sz, ask_sz):
            stepped_bid = round(bid_p + 0.01, 2)
            if stepped_bid < ask_p:
                my_bid = max(my_bid, stepped_bid)
            stepped_ask = round(ask_p - 0.01, 2)
            if stepped_ask > my_bid:
                my_ask = min(my_ask, stepped_ask)

        toxic_now = self._sim_now() < self._toxic_until
        if not toxic_now and self._qe.should_step_ahead(bid_sz, spread):
            stepped = round(bid_p + 0.01, 2)
            if stepped < ask_p and stepped >= my_bid - 3 * 0.01:
                my_bid = max(my_bid, stepped)

        if not toxic_now and self._qe.should_step_ahead(ask_sz, spread):
            stepped = round(ask_p - 0.01, 2)
            if stepped > my_bid and stepped <= my_ask + 3 * 0.01:
                my_ask = min(my_ask, stepped)

        if my_ask <= my_bid:
            my_ask = round(my_bid + 0.01, 2)

        return my_bid, my_ask

    def _layering_ok(self, q_lots, sigma_eff, q_max):
        return (abs(q_lots) <= min(self._cfg.LAYER_MAX_INV, q_max - 1)
                and sigma_eff <= self._cfg.LAYER_MAX_SIGMA
                and not self._rs.is_toxic()
                and not self._scalper_mode
                and self._sim_now() >= self._toxic_until)

    def _scalper_sides(self, sigma, qi):
        if sigma <= self._cfg.SCALPER_SIGMA_THRESH:
            self._scalper_mode = False
            return True, True
        self._scalper_mode = True
        if qi > self._cfg.SCALPER_QI_THRESH:
            return False, True
        if qi < -self._cfg.SCALPER_QI_THRESH:
            return True, False
        return True, True

    def _inv_var(self):
        if len(self._inv_window) < 2:
            return 0.0
        vals = list(self._inv_window)
        mean = sum(vals) / len(vals)
        return sum((x - mean) ** 2 for x in vals) / len(vals)

    def _check_queue_collapse(self, bid_sz, ask_sz, now):
        cancel_now = False
        if self._last_bid_sz is not None and self._last_ask_sz is not None:
            cfg = self._cfg
            # FIX-NEW2: VACUUM_RATIO lowered to 0.10 — only cancel on genuine 90% wipe,
            # not normal tick-to-tick book size oscillation (which was ~25% routinely).
            if (bid_sz < self._last_bid_sz * cfg.VACUUM_RATIO
                    or ask_sz < self._last_ask_sz * cfg.VACUUM_RATIO):
                cancel_now = True
            elif (bid_sz < self._last_bid_sz * cfg.QCOLLAPSE_RATIO
                  or ask_sz < self._last_ask_sz * cfg.QCOLLAPSE_RATIO):
                self._queue_hold_until = now + cfg.QCOLLAPSE_HOLD_S
        self._last_bid_sz = bid_sz
        self._last_ask_sz = ask_sz
        return cancel_now

    def tick(self, trader):
        if not self._market_open():
            return

        self._tick_n += 1
        now_raw = self._sim_now()
        self._fill_cb._sim_now = now_raw

        try:
            bb     = trader.get_best_price(self._ticker)
            bid_p  = bb.get_bid_price()
            ask_p  = bb.get_ask_price()
            bid_sz = float(bb.get_bid_size())
            ask_sz = float(bb.get_ask_size())
        except Exception:
            return

        if bid_p <= 0 or ask_p <= 0 or bid_p >= ask_p:
            return

        if self._check_queue_collapse(bid_sz, ask_sz, now_raw):
            self._qeng.cancel_all(trader, self._ticker)
            return

        mid    = (bid_p + ask_p) / 2.0
        spread = ask_p - bid_p
        tau    = self._tau()
        self._fill_cb._current_mid = mid

        if self._is_flash_crash(mid):
            if now_raw > self._flash_until:
                print(f"[FLASH] mid={mid:.2f} pausing {self._cfg.FLASH_CRASH_PAUSE_S}s",
                      flush=True)
                self._qeng.cancel_all(trader, self._ticker)
                self._flash_until = now_raw + self._cfg.FLASH_CRASH_PAUSE_S
            return

        if now_raw < self._flash_until:
            return

        microprice   = self._microprice(bid_p, ask_p, bid_sz, ask_sz)
        qi           = self._qi(bid_sz, ask_sz)
        sigma        = self._vol.update(mid)
        flow_raw     = self._ofi(bid_sz, ask_sz)
        flow_norm    = self._normalize_flow(flow_raw)
        momentum_adj = self._momentum.update(microprice, mid)
        qi_accel     = self._qi_acceleration(qi)

        self._qe.update()
        self._rs.update(mid, sim_now=now_raw)
        surge  = self._hawkes.tick(sim_now=now_raw)
        regime = self._regime.update(sigma, surge)

        self._refresh_portfolio(trader)
        pnl    = self._pnl_cache
        q_lots = self._q_cache
        self._inv_window.append(q_lots)
        self._tc.update(pnl, tau)
        self._sharpe.update(pnl)

        rs_val = self._rs.mean_rs()
        if rs_val < self._cfg.RS_TOXIC_THRESH:
            self._toxic_until = now_raw + self._cfg.TOXIC_DURATION_S

        pnl_delta      = pnl - self._last_pnl
        self._spread_exp.update(pnl_delta)
        self._last_pnl = pnl

        self._sig_perf.update(qi, flow_norm, momentum_adj, rs_val)

        if abs(q_lots) >= self._cfg.INV_EMERGENCY_LOTS:
            self._qeng.cancel_all(trader, self._ticker)
            # Retry emergency flatten on a cooldown if inventory is still dangerous
            if (not self._inv_emerg_flatten_done
                    or (now_raw - self._inv_emerg_flatten_last_t
                        >= self._cfg.EMERG_FLATTEN_RETRY_S)):
                self._qeng.market_flatten(trader, self._ticker, q_lots)
                self._inv_emerg_flatten_last_t = now_raw
                if not self._inv_emerg_flatten_done:
                    self._inv_emerg_flatten_done = True
                    print(f"[EMERG] inventory q={q_lots:+d} — emergency flatten submitted",
                          flush=True)
                else:
                    print(f"[EMERG] inventory q={q_lots:+d} — retrying flatten",
                          flush=True)
            return

        # FIX-NEW3: Use drawdown buffer to account for pnl_cache lag.
        # Halt at DRAWDOWN_LIMIT + DRAWDOWN_BUFFER (earlier, tighter than v18).
        effective_drawdown_limit = self._cfg.DRAWDOWN_LIMIT + self._cfg.DRAWDOWN_BUFFER
        if pnl < effective_drawdown_limit:
            if not self._halted:
                print(f"[HALT] PnL={pnl:,.0f} (min_seen={self._pnl_min_seen:,.0f}) "
                      f"inv={q_lots:+d} — flattening", flush=True)
                self._qeng.cancel_all(trader, self._ticker)
                if q_lots != 0:
                    self._qeng.market_flatten(trader, self._ticker, q_lots)
                self._halted = True
            return
        elif self._halted:
            # Hysteresis: only resume trading when PnL recovers sufficiently above halt level
            resume_level = effective_drawdown_limit + self._cfg.DRAWDOWN_HYSTERESIS
            if pnl >= resume_level:
                print(f"[RESUME] PnL={pnl:,.0f} above resume level {resume_level:,.0f}",
                      flush=True)
                self._halted = False
            else:
                return  # stay halted until sufficient recovery

        if surge >= self._cfg.SURGE_HALT_THRESH:
            if not self._surge_halt:
                print(f"[SURGE HALT] surge={surge:.1f}", flush=True)
                self._qeng.cancel_all(trader, self._ticker)
                self._surge_halt = True
            # FIX-NEW8: Still refresh portfolio during surge halt so q_cache stays
            # current. In v18, the early return here skipped _refresh_portfolio → 
            # stale inventory on surge resolution → delayed emergency flatten.
            self._refresh_portfolio(trader)
            return

        if self._surge_halt:
            if surge < self._cfg.SURGE_RESUME_RATIO:
                self._surge_halt = False
                print(f"[SURGE RESUME] surge={surge:.1f}", flush=True)
            else:
                self._refresh_portfolio(trader)
                return

        if surge >= self._cfg.SURGE_CANCEL_THRESH:
            self._qeng.cancel_all(trader, self._ticker)
            return

        # FIX-NEW1: EOD wind-down phase — reduce lot sizes gradually in 300→120s window.
        # Replaces the old hard 600s blackout with a two-phase approach:
        #   Phase 1 (tau 300→120s): quote normally but halve lot sizes and skew flat
        #   Phase 2 (tau <120s): cancel all + market flatten
        eod_wind_down = tau <= self._cfg.EOD_WIND_DOWN_START_S
        if tau <= self._cfg.EOD_FLATTEN_START_S:
            self._qeng.cancel_all(trader, self._ticker)
            if abs(q_lots) > 0:
                # Retry flatten on a cooldown if inventory is still non-zero
                if (not self._eod_flatten_done
                        or (now_raw - self._eod_flatten_last_t
                            >= self._cfg.EOD_FLATTEN_RETRY_S)):
                    self._qeng.market_flatten(trader, self._ticker, q_lots)
                    self._eod_flatten_last_t = now_raw
                    if not self._eod_flatten_done:
                        self._eod_flatten_done = True
                        print(f"[EOD] flatten q={q_lots:+d}", flush=True)
                    else:
                        print(f"[EOD] retry flatten q={q_lots:+d}", flush=True)
            return

        regime_qmax = self._regime.q_max()
        if mid > 0:
            capital_risk_lots = int(
                self._cfg.INITIAL_CAPITAL * self._cfg.CAPITAL_RISK_FRACTION
                / (mid * self._cfg.SHARES_PER_LOT))
            regime_qmax = min(regime_qmax, max(1, capital_risk_lots))

        scalper_bid, scalper_ask = self._scalper_sides(sigma, qi)
        trend_bid_f, trend_ask_f = self._trend_brake.update(mid)

        now_w     = now_raw
        quote_age = now_w - self._last_quote_t

        if quote_age < self._cfg.QUOTE_INTERVAL_S:
            return

        if now_w < self._queue_hold_until:
            return

        sigma_eff = sigma if self._vol.warmed_up else self._cfg.SIGMA_MAX
        toxic_active = now_w < self._toxic_until
        edge = self._edge_score(qi, flow_norm, momentum_adj, toxic_active)

        widen = self._regime.spread_mult()
        if surge >= 2.0:
            widen = max(widen, 1.25)
        if sigma_eff > self._cfg.VOL_BURST_HI_SIGMA:
            widen = max(widen, self._cfg.VOL_BURST_HI_WIDEN)
        elif sigma_eff > self._cfg.VOL_BURST_LO_SIGMA:
            widen = max(widen, self._cfg.VOL_BURST_LO_WIDEN)
        if toxic_active:
            widen = max(widen, self._cfg.TOXIC_SPREAD_MULT)
        if rs_val < 0:
            widen = max(widen, self._cfg.RS_WIDEN_MULT)

        inv_ratio  = abs(q_lots) / max(regime_qmax, 1)
        inv_widen  = 1.0 + 0.25 * inv_ratio * inv_ratio
        widen      = max(widen, inv_widen)

        elapsed = now_w - self._pace.start
        if elapsed < self._cfg.EARLY_WIDEN_S:
            widen *= self._cfg.EARLY_WIDEN_MULT

        tc_sm      = self._tc.spread_mult()
        tc_lm      = self._tc.lot_mult()
        rg_es      = self._regime.edge_scale()
        tc_lm     *= (1.0 + self._cfg.EDGE_LOT_SCALE  * edge * rg_es)

        toxic_penalty  = self._cfg.TOXIC_LOT_PENALTY if toxic_active else 1.0
        widen          = max(0.5, widen * tc_sm * self._spread_exp.choose())
        widen          = max(self._cfg.WIDEN_MIN_MULT,
                             min(self._cfg.WIDEN_MAX_MULT, widen))
        regime_qmax    = min(self._cfg.EDGE_QMAX_CAP,
                             int(regime_qmax * (1.0 + self._cfg.EDGE_QMAX_SCALE * edge * rg_es)))

        # FIX-NEW1 (cont): During EOD wind-down, halve effective q_max to reduce exposure
        if eod_wind_down:
            regime_qmax = max(1, regime_qmax // 2)
            toxic_penalty = min(toxic_penalty, 0.5)   # force smaller lots

        allow_bid   = q_lots <  regime_qmax
        allow_ask   = q_lots > -regime_qmax
        allow_bid   = allow_bid and scalper_bid and (trend_bid_f > 0.0)
        allow_ask   = allow_ask and scalper_ask and (trend_ask_f > 0.0)
        half_qmax = regime_qmax * 0.5
        if q_lots > half_qmax and qi < -0.25:
            allow_bid = False
        if q_lots < -half_qmax and qi > 0.25:
            allow_ask = False

        bl_telem, al_telem = self._lots(q_lots, tc_lm, regime_qmax, toxic_penalty)

        _tnow = now_raw
        if _tnow - self._last_telem_t >= self._cfg.TELEM_INTERVAL_S:
            self._logger.log({
                "t":        round(_tnow - self._pace.start, 1),
                "pnl":      round(pnl, 2),
                "fills":    self._fill_cb.fill_count,
                "inv":      q_lots,
                "spread":   round(spread, 4),
                "edge":     round(edge, 3),
                "sigma":    round(sigma, 5),
                "qi":       round(qi, 3),
                "flow":     round(flow_norm, 3),
                "momentum": round(momentum_adj, 4),
                "regime":   regime,
                "k":        round(self._cfg.K_BASE, 3),
                "rs":       round(rs_val, 5),
                "surge":    round(surge, 2),
                "tbf":      round(self._trend_brake._last_tbf, 2),
                "taf":      round(self._trend_brake._last_taf, 2),
                "lot":      bl_telem,
                "widen":    round(widen, 3),
                "tc_mode":  self._tc.mode,
                "sharpe":   round(self._sharpe.sharpe, 3),
                "eod_wind": eod_wind_down,
                "half_spread_ticks": round(widen * math.log(1 + self._cfg.GAMMA / max(self._cfg.K_BASE, 1)) / self._cfg.GAMMA / 0.01, 2),
            })
            self._last_telem_t = _tnow

        fc        = self._fill_cb.fill_count
        pace_mult = self._pace.multiplier(fc, tau=tau)
        fill_proj = self._pace.projected_fills(fc, tau=tau)
        inv_var   = self._inv_var()
        self._calib.update(rs_val, fill_proj, inv_var)
        autocalib_fired = (self._tick_n % self._cfg.AUTOCALIB_INTERVAL == 0)
        if not autocalib_fired:
            self._k_adapter.update(fill_proj, rs_val)

        w_qi, w_flow, w_mom = self._sig_perf.weights()

        if self._tick_n >= self._next_rl_tick:
            self._rl_offset    = random.choice([-1, 0, 0, 1]) * 0.01
            self._next_rl_tick = (self._tick_n
                                  + random.randint(self._cfg.ANTI_RL_INTERVAL_LO,
                                                   self._cfg.ANTI_RL_INTERVAL_HI))

        new_bid, new_ask = self._solver.compute(
            microprice, sigma_eff, q_lots, tau,
            qi_lean=qi, flow_lean=flow_norm,
            momentum_adj=momentum_adj, qi_accel=qi_accel,
            widen_mult=widen, pace_mult=pace_mult,
            regime_gamma=self._regime.gamma(),
            w_qi=w_qi, w_flow=w_flow, w_mom=w_mom)

        new_bid, new_ask = self._apply_liquidity_skew(
            new_bid, new_ask, qi, flow_norm, w_qi, w_flow)

        new_bid, new_ask = self._apply_liquidity_dominance(
            bid_p, ask_p, bid_sz, ask_sz, new_bid, new_ask, spread)

        new_bid = round(new_bid + self._rl_offset, 2)
        new_ask = round(new_ask + self._rl_offset, 2)
        new_bid = min(new_bid, round(ask_p - 0.01, 2))
        new_ask = max(new_ask, round(bid_p + 0.01, 2))
        if new_ask <= new_bid:
            new_ask = round(new_bid + 0.01, 2)

        stale     = quote_age >= self._quote_expiry
        unchanged = (new_bid == self._last_bid and new_ask == self._last_ask)

        if unchanged and not stale:
            return

        if stale:
            self._quote_expiry = random.uniform(self._cfg.QUOTE_MAX_AGE_LO,
                                                self._cfg.QUOTE_MAX_AGE_HI)

        self._qeng.cancel_all(trader, self._ticker)

        if allow_bid:
            self._qeng.submit_limit(trader, self._ticker, 'BID', new_bid, bl_telem)
        if allow_ask:
            self._qeng.submit_limit(trader, self._ticker, 'ASK', new_ask, al_telem)

        # FIX-NEW9: Initialize layer vars to 0 BEFORE the do_layer block so that
        # the decoy block below always has valid values regardless of do_layer outcome.
        # v18 relied on Python short-circuit evaluation in the decoy condition, which
        # is safe but fragile — this makes the intent explicit.
        layer_bid_l = 0
        layer_ask_l = 0
        outer_bid   = 0.0
        outer_ask   = 0.0

        do_layer = self._layering_ok(q_lots, sigma_eff, regime_qmax)
        if do_layer:
            outer_bid = round(new_bid - 0.01, 2)
            outer_ask = round(new_ask + 0.01, 2)
            ll = self._cfg.LAYER_LOTS
            add_bid_cap = max(0, regime_qmax - q_lots)
            add_ask_cap = max(0, regime_qmax + q_lots)
            layer_bid_l = max(0, min(ll, add_bid_cap - bl_telem))
            layer_ask_l = max(0, min(ll, add_ask_cap - al_telem))
            if allow_bid and outer_bid > 0 and layer_bid_l > 0:
                self._qeng.submit_limit(trader, self._ticker, 'BID', outer_bid, layer_bid_l)
            if allow_ask and layer_ask_l > 0:
                self._qeng.submit_limit(trader, self._ticker, 'ASK', outer_ask, layer_ask_l)

        if (not toxic_active
                and tau > self._cfg.EOD_FLATTEN_START_S
                and regime == RegimeDetector.CALM
                and random.random() < self._cfg.DECOY_PROB):
            d_ticks = self._cfg.DECOY_TICKS * 0.01
            d_bid   = round(bid_p - d_ticks, 2)
            d_ask   = round(ask_p + d_ticks, 2)
            effective_bl = bl_telem if allow_bid else 0
            effective_al = al_telem if allow_ask else 0
            layer_bid_submitted = (layer_bid_l
                                   if (do_layer and allow_bid and outer_bid > 0 and layer_bid_l > 0)
                                   else 0)
            layer_ask_submitted = (layer_ask_l
                                   if (do_layer and allow_ask and layer_ask_l > 0)
                                   else 0)
            decoy_bid_cap = max(0, regime_qmax - q_lots) - effective_bl - layer_bid_submitted
            decoy_ask_cap = max(0, regime_qmax + q_lots) - effective_al - layer_ask_submitted
            if allow_bid and d_bid > 0 and decoy_bid_cap >= 1:
                self._qeng.submit_limit(trader, self._ticker, 'BID', d_bid, 1)
            if allow_ask and decoy_ask_cap >= 1:
                self._qeng.submit_limit(trader, self._ticker, 'ASK', d_ask, 1)

        self._last_bid     = new_bid
        self._last_ask     = new_ask
        self._last_quote_t = now_w

        if self._tick_n % self._cfg.LOG_EVERY == 0:
            realized = 0.0
            try:
                realized = float(
                    trader.get_portfolio_summary().get_total_realized_pl())
            except Exception:
                pass
            mode_s  = "SCALP" if self._scalper_mode else "MAKE"
            layer_s = "L" if do_layer else "-"
            tc_s    = self._tc.mode[:3]
            rg_s    = regime[:3]
            tok_s   = "TOX" if toxic_active else "---"
            eod_s   = "WIND" if eod_wind_down else "----"
            print(
                f"[{self._tick_n:7d}][{mode_s}{layer_s}][{tc_s}][{rg_s}][{tok_s}][{eod_s}] "
                f"mp={microprice:.2f} σ={sigma_eff:.4f} qi={qi:+.2f} "
                f"edge={edge:.2f} acc={qi_accel:+.3f} mom={momentum_adj*100:+.2f}c "
                f"q={q_lots:+d} fills={fc} "
                f"PnL=${pnl:,.0f}(R${realized:,.0f}) "
                f"Sh={self._sharpe.sharpe:.2f} RS={rs_val:.4f} "
                f"bid={new_bid:.2f} ask={new_ask:.2f} "
                f"τ={tau/60:.1f}m surge={surge:.1f} "
                f"k={self._cfg.K_BASE:.2f} exp={self._spread_exp.current:.1f} "
                f"wq={w_qi:.2f} wf={w_flow:.2f} wm={w_mom:.2f}",
                flush=True)
            elapsed_s = max(time.time() - self._pace.start, 1.0)
            if elapsed_s > 120 and fill_proj < self._cfg.MIN_FILLS_SESSION:
                print(f"  [WARN] proj={fill_proj:.0f} fills < {self._cfg.MIN_FILLS_SESSION}",
                      flush=True)

    def run(self, trader):
        print(
            f"[PREDATOR v19] {self._ticker} "
            f"γ={self._cfg.GAMMA:.3f} k={self._cfg.K_BASE:.2f} "
            f"αqi={self._cfg.ALPHA_QI} target_fills={self._cfg.TARGET_FILLS} "
            f"inv_skew={self._cfg.INV_SKEW_COEFF} emerg={self._cfg.INV_EMERGENCY_LOTS}",
            flush=True)
        try:
            while self._market_open():
                try:
                    self.tick(trader)
                except Exception as e:
                    print(f"[TICK ERR] {e}", file=sys.stderr, flush=True)
                time.sleep(0.01)
        finally:
            try:
                self._qeng.cancel_all(trader, self._ticker)
                # Refresh portfolio to get accurate final inventory
                self._refresh_portfolio(trader)
                q = self._q_cache
                if abs(q) > 0:
                    print(f"[PREDATOR v19] EOD flatten q={q}", flush=True)
                    self._qeng.market_flatten(trader, self._ticker, q)
            except Exception:
                pass
            save_session_log(
                self._cfg,
                self._fill_cb.fill_count,
                self._pnl_cache,
                self._rs.mean_rs(),
                self._sharpe.sharpe,
                self._spread_exp.best())
            self._logger.close()
            print(
                f"[PREDATOR v19] Final PnL=${self._pnl_cache:,.2f} "
                f"Fills={self._fill_cb.fill_count} "
                f"Sharpe={self._sharpe.sharpe:.3f} "
                f"BestExp={self._spread_exp.best():.1f}",
                flush=True)


def main():
    import argparse
    p = argparse.ArgumentParser(description="Predator v19 — HFTC-26 Competition Bot")
    p.add_argument("--ticker",   default="AAPL",      help="Ticker symbol")
    p.add_argument("--username", default=MY_USERNAME, help="SHIFT username")
    p.add_argument("--password", default=MY_PASSWORD, help="SHIFT password")
    p.add_argument("--cfg",      default=CFG_FILE,    help="SHIFT config file")
    args = p.parse_args()

    if not _HAS_SHIFT:
        print("[ERROR] shift library not found. "
              "Use predator_sim_runner.py for simulation.", file=sys.stderr)
        sys.exit(1)

    cfg = Config()

    try:
        with open(SESSION_LOG, "r") as f:
            s = json.load(f)
        best_exp = float(s.get("best_exp", 1.0))
    except Exception:
        best_exp = 1.0

    load_session_log(cfg)

    with shift.Trader(args.username) as trader:
        try:
            trader.connect(args.cfg, args.password)
            trader.sub_all_order_book()
        except shift.IncorrectPasswordError as e:
            print(f"[AUTH ERROR] {e}", file=sys.stderr)
            sys.exit(1)
        except shift.ConnectionTimeoutError as e:
            print(f"[TIMEOUT] {e}", file=sys.stderr)
            sys.exit(1)

        time.sleep(2)

        hawkes  = HawkesSurge(cfg)
        rs_mon  = RealizedSpreadMonitor(cfg)
        queue_e = QueueEstimator(cfg)
        fill_cb = FillCallback(hawkes, rs_mon, queue_e, args.ticker)
        bot     = PredatorBot(args.ticker, cfg, fill_cb)
        bot._spread_exp.seed(best_exp)

        _cbs = []

        def _exec_bridge(order):
            for c in _cbs:
                try:
                    c(trader, order.id)
                except Exception as e:
                    print(f"[ExecCB] {e}", file=sys.stderr)

        _cbs.append(fill_cb)
        try:
            trader.subExecutionNotice(_exec_bridge)
            print("[MAIN] Execution notice callback registered.", flush=True)
        except AttributeError:
            print("[WARN] subExecutionNotice unavailable — fills will not be tracked.",
                  file=sys.stderr)

        bot.run(trader)


if __name__ == "__main__":
    main()
