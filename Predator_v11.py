import sys
import json
import time
import math
import os
import random
import threading

# ----- Simulation seed control -----
SEED = int(os.environ.get("SIM_SEED", "0"))
random.seed(SEED)

print(f"[SIM] random seed = {SEED}")
# -----------------------------------

from collections import deque
from datetime import datetime, time as dtime

try:
    import shift                    # live SHIFT environment
    from shift import Order as _ShiftOrder
    _HAS_SHIFT = True
except ImportError:
    shift = None                    # simulation environment
    _ShiftOrder = None
    _HAS_SHIFT = False

MY_USERNAME = os.environ.get("SHIFT_USERNAME", "alpha-greeks")
MY_PASSWORD = os.environ.get("SHIFT_PASSWORD", "Tku3YFOO")
CFG_FILE    = os.environ.get("SHIFT_CFG",      "initiator.cfg")
SESSION_LOG = "predator_session_log.json"


class Config:
    INITIAL_CAPITAL      = 1_000_000.0
    SHARES_PER_LOT       = 100
    MARKET_OPEN          = dtime(9, 30)
    MARKET_CLOSE         = dtime(16, 0)
    SESSION_SECONDS      = 6.5 * 3600

    GAMMA                = 0.10
    K_BASE               = 8.0
    INV_SKEW_COEFF       = 0.010
    ALPHA_QI             = 0.025
    LAMBDA_FLOW          = 0.02
    FLOW_EMA_ALPHA       = 0.20
    FLOW_NORM_CAP        = 1000.0

    EWMA_LAMBDA          = 0.97
    SIGMA_FLOOR          = 0.001
    SIGMA_MAX            = 0.026
    VOL_WARMUP_TICKS     = 50

    SPREAD_MIN           = 0.02
    SPREAD_MAX           = 0.20

    Q_MAX_LOTS           = 5
    LOT_BASE             = 1
    LOT_MIN              = 1

    LIMIT_REBATE         = 0.002
    MARKET_FEE           = 0.003

    DRAWDOWN_LIMIT       = -15_000.0
    CAPITAL_RISK_FRACTION = 0.10   # FIX v12: max notional = capital * 10% / price
    SURGE_CANCEL_THRESH  = 4.0
    SURGE_HALT_THRESH    = 7.0
    SURGE_RESUME_RATIO   = 2.5

    FLASH_CRASH_WINDOW   = 30
    FLASH_CRASH_PCT      = 0.035
    FLASH_CRASH_PAUSE_S  = 8.0

    EOD_FLATTEN_START_S  = 900
    EOD_AGGRESS_START_S  = 300

    QUOTE_INTERVAL_S     = 0.25
    QUOTE_MAX_AGE_LO     = 1.5
    QUOTE_MAX_AGE_HI     = 2.5

    API_REFRESH_S        = 0.30

    HAWKES_MU            = 7.0
    HAWKES_ALPHA         = 5.0
    HAWKES_BETA          = 5.0

    TARGET_FILLS         = 400
    FILL_PACE_K          = 0.15
    FILL_PACE_MIN_MULT   = 0.75
    FILL_PACE_MAX_MULT   = 1.35

    RS_WINDOW_S          = 5.0
    RS_TOXIC_THRESH      = -0.001  # FIX v12: was -0.002, trigger toxic earlier
    RS_WIDEN_MULT        = 1.60   # FIX v12: was 1.25, now actually applied in tick()
    RS_MAX_HISTORY       = 50
    TOXIC_DURATION_S     = 15.0   # FIX v12: was 5.0 — toxic flows persist longer
    TOXIC_SPREAD_MULT    = 1.80   # FIX v12: was 1.50 — wider when toxic
    TOXIC_LOT_PENALTY    = 0.50   # FIX v12: was 0.60 — smaller lots when toxic

    MM_DETECT_SIZE       = 1000

    SCALPER_SIGMA_THRESH = 0.012
    SCALPER_QI_THRESH    = 0.25
    VOL_BURST_LO_SIGMA   = 0.015
    VOL_BURST_LO_WIDEN   = 1.30
    VOL_BURST_HI_SIGMA   = 0.020
    VOL_BURST_HI_WIDEN   = 1.50

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

    LAYER_MAX_INV        = 3
    LAYER_MAX_SIGMA      = 0.015
    LAYER_LOTS           = 1

    TC_TARGET_PNL          = 600.0
    TC_ATTACK_BELOW        = -200.0
    TC_DEFENSE_ABOVE       = 400.0
    TC_ATTACK_SPREAD_MULT  = 0.85
    TC_ATTACK_LOT_MULT     = 1.5
    TC_DEFENSE_SPREAD_MULT = 1.25
    TC_DEFENSE_LOT_MULT    = 0.7
    TC_SPRINT_TAU_S        = 600.0
    TC_SPRINT_PNL_THRESH   = 200.0
    TC_SPRINT_SPREAD_MULT  = 0.75
    TC_SPRINT_LOT_MULT     = 1.8

    ANTI_RL_INTERVAL_LO  = 350
    ANTI_RL_INTERVAL_HI  = 600

    AUTOCALIB_INTERVAL   = 300
    AUTOCALIB_K_MIN      = 3.0
    INTRA_K_INTERVAL     = 50
    INTRA_K_UP           = 1.020
    INTRA_K_DOWN         = 0.980
    INTRA_K_FILL_RATIO   = 0.80
    AUTOCALIB_K_MAX      = 20.0
    AUTOCALIB_G_MIN      = 0.07
    AUTOCALIB_G_MAX      = 0.16

    XROUND_K_MIN         = 3.0
    XROUND_K_MAX         = 20.0
    XROUND_G_MIN         = 0.07
    XROUND_G_MAX         = 0.20
    XROUND_AQI_MIN       = 0.015
    XROUND_AQI_MAX       = 0.050

    MIN_FILLS_SESSION    = 200
    LOG_EVERY            = 500

    REGIME_VOL_THRESH    = 0.012
    REGIME_TOXIC_THRESH  = 0.020
    REGIME_TOXIC_SURGE   = 5.0
    REGIME_SMOOTH_ALPHA  = 0.15

    REGIME_CALM_GAMMA    = 0.08
    REGIME_CALM_SMULT    = 1.05   # FIX v12: was 0.85 — compressed spreads caused adverse-sel in CALM
    REGIME_CALM_QMAX     = 6

    REGIME_VOL_GAMMA     = 0.12
    REGIME_VOL_SMULT     = 1.20
    REGIME_VOL_QMAX      = 4

    REGIME_TOXIC_GAMMA   = 0.18
    REGIME_TOXIC_SMULT   = 1.80
    REGIME_TOXIC_QMAX    = 2

    SPREAD_EXP_OPTIONS   = [0.9, 1.0, 1.1]
    SPREAD_EXP_EPSILON   = 0.10
    SPREAD_EXP_SWITCH_S  = 10.0

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

    EDGE_LOT_SCALE       = 0.80
    EDGE_QMAX_SCALE      = 0.50
    EDGE_QMAX_CAP        = 10

    DECOY_PROB           = 0.085
    DECOY_TICKS          = 6

    QCOLLAPSE_RATIO      = 0.35
    QCOLLAPSE_HOLD_S     = 0.35
    VACUUM_RATIO         = 0.25

    EARLY_COMPRESS_S     = 900.0
    EARLY_COMPRESS_MULT  = 0.82

    CASCADE_QI_THRESH    = 0.55
    CASCADE_MICRO_THRESH = 0.005
    CASCADE_FLOW_THRESH  = 0.30
    CASCADE_INV_MULT     = 1.8
    CASCADE_INV_TARGET   = 2
    QUEUE_DROP_STEP      = 200
    INV_TAPER_START_S    = 3600.0
    INV_TAPER_MIN        = 0.20


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
        if fills < cfg.TARGET_FILLS and rs >= 0:
            cfg.K_BASE = min(cfg.XROUND_K_MAX, cfg.K_BASE * 1.05)
        if rs < 0:
            # FIX v12: was * 0.92 (wrong! tightened spreads into adverse selection)
            # Negative RS means we're getting picked off → widen spreads → increase k
            cfg.K_BASE = min(cfg.XROUND_K_MAX, cfg.K_BASE * 1.10)
        if pnl < 0:
            cfg.K_BASE = max(cfg.XROUND_K_MIN, cfg.K_BASE * 0.96)
            cfg.GAMMA  = min(cfg.XROUND_G_MAX,  cfg.GAMMA  * 1.08)
        if 0 < rs < 0.004:
            cfg.ALPHA_QI = min(cfg.XROUND_AQI_MAX, cfg.ALPHA_QI * 1.05)
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
        self._last_tbf = 1.0   # initialised here — telem reads these before warmup
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
        if r > self._cfg.TRADE_RATE_HI:
            return self._cfg.LOT_SCALE_HI
        if r < self._cfg.TRADE_RATE_LO:
            return self._cfg.LOT_SCALE_LO
        return 1.0


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
        self._switch_s    = cfg.SPREAD_EXP_SWITCH_S
        self._scores      = {o: 0.0 for o in self._options}
        self._counts      = {o: 1   for o in self._options}
        self.current      = 1.0
        self._last_switch = time.time()

    def seed(self, best_exp):
        if best_exp in self._options:
            self.current = best_exp
            for o in self._options:
                self._scores[o] = 1.0 if o == best_exp else 0.0

    def choose(self):
        if time.time() - self._last_switch < self._switch_s:
            return self.current
        if random.random() < self._epsilon:
            self.current = random.choice(self._options)
        else:
            self.current = max(self._options,
                               key=lambda o: self._scores[o] / self._counts[o])
        self._last_switch = time.time()
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
        self.qi_score  += a * rs * qi
        self.fl_score  += a * rs * flow_norm
        self.mom_score += a * rs * momentum

    def weights(self):
        total = (abs(self.qi_score) + abs(self.fl_score)
                 + abs(self.mom_score) + 1e-9)
        w_qi  = self.qi_score  / total
        w_fl  = self.fl_score  / total
        w_mom = self.mom_score / total
        clamp = lambda w: max(self._w_min, min(self._w_max, 1.0 + w))
        return clamp(w_qi), clamp(w_fl), clamp(w_mom)


class SharpeTracker:
    def __init__(self, cfg):
        self._snap_s   = cfg.SHARPE_SNAPSHOT_S
        self._min_s    = cfg.SHARPE_MIN_SNAPS
        self._snaps    = deque(maxlen=500)
        self._last_pnl = 0.0
        self._last_t   = time.time()
        self.sharpe    = 0.0

    def update(self, pnl):
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
        self.sharpe = (mean / std) * math.sqrt(n)


class ASSolver:
    TICK = 0.01

    def __init__(self, cfg):
        self._cfg = cfg

    def compute(self, microprice, sigma, q_lots, tau,
                qi_lean=0.0, flow_lean=0.0, momentum_adj=0.0, qi_accel=0.0,
                widen_mult=1.0, pace_mult=1.0,
                regime_gamma=None, w_qi=1.0, w_flow=1.0, w_mom=1.0,
                inv_target=0):
        cfg      = self._cfg
        gamma    = regime_gamma if regime_gamma is not None else cfg.GAMMA
        k        = cfg.K_BASE
        tau_norm = max(tau / max(cfg.SESSION_SECONDS, 1.0), 1e-4)
        sqrt_tau = math.sqrt(tau_norm)

        r = (microprice
             - (q_lots - inv_target) * cfg.INV_SKEW_COEFF
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
        self._last_t = time.time()

    def on_event(self):
        now          = time.time()
        dt           = max(now - self._last_t, 1e-9)
        self._lam    = (self._mu
                        + (self._lam - self._mu) * math.exp(-self._beta * dt)
                        + self._alpha)
        self._last_t = now

    def tick(self):
        now          = time.time()
        dt           = max(now - self._last_t, 1e-9)
        self._lam    = self._mu + (self._lam - self._mu) * math.exp(-self._beta * dt)
        self._last_t = now
        return self._lam / max(self._mu, 1e-6)


class FillPaceController:
    def __init__(self, cfg):
        self._cfg  = cfg
        self.start = time.time()

    def multiplier(self, fill_count):
        elapsed  = max(time.time() - self.start, 1.0)
        expected = self._cfg.TARGET_FILLS * (elapsed / self._cfg.SESSION_SECONDS)
        error    = expected - fill_count
        mult     = 1.0 - self._cfg.FILL_PACE_K * (error / max(self._cfg.TARGET_FILLS, 1))
        return max(self._cfg.FILL_PACE_MIN_MULT, min(self._cfg.FILL_PACE_MAX_MULT, mult))

    def projected_fills(self, fill_count):
        elapsed = max(time.time() - self.start, 1.0)
        return fill_count / elapsed * self._cfg.SESSION_SECONDS


class RealizedSpreadMonitor:
    def __init__(self, cfg):
        self._cfg     = cfg
        self._pending = deque()
        self._history = deque(maxlen=cfg.RS_MAX_HISTORY)
        self._lock    = threading.Lock()

    def record(self, side, price):
        with self._lock:
            self._pending.append((side, price, time.time()))

    def update(self, current_mid):
        now = time.time()
        with self._lock:
            while (self._pending
                   and (now - self._pending[0][2]) > self._cfg.RS_WINDOW_S):
                side, price, _ = self._pending.popleft()
                rs = (current_mid - price) if side == 'BID' else (price - current_mid)
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
        self._start = time.time()

    def update(self, pnl, tau):
        cfg     = self._cfg
        elapsed = time.time() - self._start
        exp     = cfg.TC_TARGET_PNL * (elapsed / max(cfg.SESSION_SECONDS, 1.0))
        delta   = pnl - exp
        if tau < cfg.TC_SPRINT_TAU_S and pnl < cfg.TC_SPRINT_PNL_THRESH:
            self.mode = "SPRINT"
        elif delta < cfg.TC_ATTACK_BELOW:
            self.mode = "ATTACK"
        elif delta > cfg.TC_DEFENSE_ABOVE:
            self.mode = "DEFENSE"
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

    def update(self, rs, fill_proj, inv_var):
        self._ticks += 1
        if self._ticks % self._cfg.AUTOCALIB_INTERVAL != 0:
            return
        cfg = self._cfg
        if rs < 0:
            # FIX v12: was * 0.95 (wrong! adverse selection needs wider spreads → higher k)
            cfg.K_BASE = min(cfg.AUTOCALIB_K_MAX, cfg.K_BASE * 1.06)
        elif fill_proj < cfg.TARGET_FILLS:
            cfg.K_BASE = min(cfg.AUTOCALIB_K_MAX, cfg.K_BASE * 1.04)
        if inv_var > 6.0:
            cfg.GAMMA = min(cfg.AUTOCALIB_G_MAX, cfg.GAMMA * 1.04)
        else:
            cfg.GAMMA = max(cfg.AUTOCALIB_G_MIN, cfg.GAMMA * 0.99)


class IntraSessionKAdapter:
    def __init__(self, cfg):
        self._cfg   = cfg
        self._ticks = 0

    def update(self, fill_proj, rs_mean):
        self._ticks += 1
        if self._ticks % self._cfg.INTRA_K_INTERVAL != 0:
            return
        cfg = self._cfg
        if rs_mean < 0:
            # FIX v12: was INTRA_K_DOWN (0.980) — totally backwards!
            # Adverse selection → widen → raise k
            cfg.K_BASE = min(cfg.AUTOCALIB_K_MAX, cfg.K_BASE * cfg.INTRA_K_UP)
        if fill_proj < cfg.INTRA_K_FILL_RATIO * cfg.TARGET_FILLS:
            cfg.K_BASE = min(cfg.AUTOCALIB_K_MAX, cfg.K_BASE * cfg.INTRA_K_UP)


class QuoteEngine:
    @staticmethod
    def _order_type(side_or_str, is_market=False):
        """Return the correct Order.Type regardless of environment."""
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
        side = 'ASK' if q_lots > 0 else 'BID'    # selling to flatten long, buying to flatten short
        try:
            otype = QuoteEngine._order_type(side, is_market=True)
            trader.submit_order(QuoteEngine._make_order(otype, ticker, abs(q_lots)))
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

    def __call__(self, trader, order_id):
        try:
            order = trader.get_order(order_id)
            if order is None or order.symbol != self._ticker:
                return
            qty = getattr(order, 'executed_quantity',
                          getattr(order, 'executed_size', 0))
            if qty == 0:
                return
            self._hawkes.on_event()
            self._qe.on_fill(qty * 100)
            # Duck-type side detection: works with shift.Order.Type and sim Order.Type
            try:
                t = order.type
                type_str = t.name if hasattr(t, 'name') else str(t)
                is_buy = 'BUY' in type_str.upper()
                side = 'BID' if is_buy else 'ASK'
            except Exception:
                side = 'ASK'
            self._rs.record(side, float(order.price))
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
        self._bid_drop    = 0
        self._ask_drop    = 0
        self._cascade_active = False

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

    def _tau(self):
        t     = datetime.now().time()
        now_s = t.hour * 3600 + t.minute * 60 + t.second
        cls_s = (self._cfg.MARKET_CLOSE.hour * 3600
                 + self._cfg.MARKET_CLOSE.minute * 60)
        return float(max(cls_s - now_s, 0))

    def _market_open(self):
        t = datetime.now().time()
        return self._cfg.MARKET_OPEN <= t <= self._cfg.MARKET_CLOSE

    def _refresh_portfolio(self, trader):
        now = time.time()
        if now - self._api_t < self._cfg.API_REFRESH_S:
            return
        try:
            realized        = float(trader.get_portfolio_summary().get_total_realized_pl())
            unrealized      = float(trader.get_portfolio_item(self._ticker).get_unrealized_pl())
            self._pnl_cache = realized + unrealized
        except Exception:
            pass
        try:
            shares        = trader.get_portfolio_item(self._ticker).get_shares()
            self._q_cache = int(shares // self._cfg.SHARES_PER_LOT)
        except Exception:
            pass
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
        inv_cap  = max(1, q_max - abs(q_lots))
        bid_l    = min(bid_l, inv_cap)
        ask_l    = min(ask_l, inv_cap)
        return bid_l, ask_l

    def _is_flash_crash(self, mid):
        self._mid_window.append(mid)
        if len(self._mid_window) < self._cfg.FLASH_CRASH_WINDOW:
            return False
        lo = min(self._mid_window)
        hi = max(self._mid_window)
        if lo <= 0:
            return False
        return (hi - lo) / lo > self._cfg.FLASH_CRASH_PCT

    def _detect_competing_mm(self, spread, bid_sz, ask_sz):
        return (spread <= 0.02
                and bid_sz > self._cfg.MM_DETECT_SIZE
                and ask_sz > self._cfg.MM_DETECT_SIZE)

    def _apply_liquidity_dominance(self, bid_p, ask_p, bid_sz, ask_sz,
                                   my_bid, my_ask, spread):
        if spread > 0.02:
            dom_bid = round(bid_p + 0.01, 2)
            dom_ask = round(ask_p - 0.01, 2)
            if dom_ask > dom_bid:
                my_bid = max(my_bid, dom_bid)
                my_ask = min(my_ask, dom_ask)

        if self._detect_competing_mm(spread, bid_sz, ask_sz):
            stepped_bid = round(bid_p + 0.01, 2)
            if stepped_bid < ask_p:
                my_bid = max(my_bid, stepped_bid)

        # FIX v12: only step ahead of queue when NOT toxic — stepping ahead when adversely
        # selected feeds the problem (informed traders are sitting in that queue)
        toxic_now = time.time() < self._toxic_until
        if not toxic_now and self._qe.should_step_ahead(bid_sz, spread):
            stepped = round(bid_p + 0.01, 2)
            if stepped < ask_p:
                my_bid = max(my_bid, stepped)

        if not toxic_now and self._qe.should_step_ahead(ask_sz, spread):
            stepped = round(ask_p - 0.01, 2)
            if stepped > my_bid:
                my_ask = min(my_ask, stepped)

        if not toxic_now and self._bid_drop > self._cfg.QUEUE_DROP_STEP:
            stepped = round(bid_p + 0.01, 2)
            if stepped < ask_p:
                my_bid = max(my_bid, stepped)

        if not toxic_now and self._ask_drop > self._cfg.QUEUE_DROP_STEP:
            stepped = round(ask_p - 0.01, 2)
            if stepped > my_bid:
                my_ask = min(my_ask, stepped)

        if my_ask <= my_bid:
            my_ask = round(my_bid + 0.01, 2)

        return my_bid, my_ask

    def _layering_ok(self, q_lots, sigma_eff, q_max):
        return (abs(q_lots) <= min(self._cfg.LAYER_MAX_INV, q_max - 1)
                and sigma_eff <= self._cfg.LAYER_MAX_SIGMA
                and not self._rs.is_toxic()
                and not self._scalper_mode
                and time.time() >= self._toxic_until)

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
        self._bid_drop = max(0, (self._last_bid_sz or bid_sz) - bid_sz)
        self._ask_drop = max(0, (self._last_ask_sz or ask_sz) - ask_sz)
        if self._last_bid_sz is not None and self._last_ask_sz is not None:
            cfg = self._cfg
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
        now_raw = time.time()

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
        micro_dev    = microprice - mid

        self._cascade_active = (
            abs(qi) > self._cfg.CASCADE_QI_THRESH
            and abs(micro_dev) > self._cfg.CASCADE_MICRO_THRESH
            and abs(flow_norm) > self._cfg.CASCADE_FLOW_THRESH
            and not self._rs.is_toxic()
        )

        self._qe.update()
        self._rs.update(mid)
        surge  = self._hawkes.tick()
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

        # ── TELEMETRY written later, after edge is computed ──

        pnl_delta      = pnl - self._last_pnl
        self._spread_exp.update(pnl_delta)
        self._last_pnl = pnl

        self._sig_perf.update(qi, flow_norm, momentum_adj, rs_val)

        if pnl < self._cfg.DRAWDOWN_LIMIT:
            if not self._halted:
                print(f"[HALT] PnL={pnl:,.0f} inv={q_lots:+d} — flattening", flush=True)
                self._qeng.cancel_all(trader, self._ticker)
                # FIX v12: was cancel-only → position bled indefinitely after halt.
                # Market-flatten immediately to stop the loss from open inventory.
                if q_lots != 0:
                    self._qeng.market_flatten(trader, self._ticker, q_lots)
                self._halted = True
            return

        self._halted = False

        if surge >= self._cfg.SURGE_HALT_THRESH:
            if not self._surge_halt:
                print(f"[SURGE HALT] surge={surge:.1f}", flush=True)
                self._qeng.cancel_all(trader, self._ticker)
                self._surge_halt = True
            return

        if self._surge_halt:
            if surge < self._cfg.SURGE_RESUME_RATIO:
                self._surge_halt = False
                print(f"[SURGE RESUME] surge={surge:.1f}", flush=True)
            else:
                return

        if surge >= self._cfg.SURGE_CANCEL_THRESH:
            self._qeng.cancel_all(trader, self._ticker)
            return

        if tau <= self._cfg.EOD_AGGRESS_START_S:
            self._qeng.cancel_all(trader, self._ticker)
            if abs(q_lots) > 0:
                self._qeng.market_flatten(trader, self._ticker, q_lots)
            return

        if tau <= self._cfg.EOD_FLATTEN_START_S:
            self._qeng.cancel_all(trader, self._ticker)
            if abs(q_lots) > 0:
                eod_bid, eod_ask = self._solver.compute(
                    microprice, sigma, q_lots, tau, qi, flow_norm,
                    momentum_adj=0.0, widen_mult=2.0)
                self._qeng.submit_limit(trader, self._ticker, 'BID', eod_bid, 1)
                self._qeng.submit_limit(trader, self._ticker, 'ASK', eod_ask, 1)
            return

        regime_qmax = self._regime.q_max()
        if mid > 0:
            capital_risk_lots = int(
                self._cfg.INITIAL_CAPITAL * self._cfg.CAPITAL_RISK_FRACTION
                / (mid * self._cfg.SHARES_PER_LOT))
            regime_qmax = min(regime_qmax, max(1, capital_risk_lots))
        if tau < self._cfg.INV_TAPER_START_S:
            taper = max(self._cfg.INV_TAPER_MIN, tau / self._cfg.INV_TAPER_START_S)
            regime_qmax = max(1, int(regime_qmax * taper))
        if self._cascade_active:
            regime_qmax = min(self._cfg.Q_MAX_LOTS,
                              int(regime_qmax * self._cfg.CASCADE_INV_MULT))
        allow_bid   = q_lots <  regime_qmax
        allow_ask   = q_lots > -regime_qmax

        scalper_bid, scalper_ask = self._scalper_sides(sigma, qi)
        allow_bid = allow_bid and scalper_bid
        allow_ask = allow_ask and scalper_ask

        trend_bid_f, trend_ask_f = self._trend_brake.update(mid)
        allow_bid = allow_bid and (trend_bid_f > 0.0)
        allow_ask = allow_ask and (trend_ask_f > 0.0)

        now_w     = time.time()
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
            widen = max(widen, 1.50)
        if sigma_eff > self._cfg.VOL_BURST_HI_SIGMA:
            widen = max(widen, self._cfg.VOL_BURST_HI_WIDEN)
        elif sigma_eff > self._cfg.VOL_BURST_LO_SIGMA:
            widen = max(widen, self._cfg.VOL_BURST_LO_WIDEN)
        if toxic_active:
            widen = max(widen, self._cfg.TOXIC_SPREAD_MULT)
        # FIX v12: apply RS_WIDEN_MULT when realized spread is negative (was defined, never used)
        if rs_val < 0:
            widen = max(widen, self._cfg.RS_WIDEN_MULT)

        elapsed = now_w - self._pace.start
        if elapsed < self._cfg.EARLY_COMPRESS_S:
            widen *= self._cfg.EARLY_COMPRESS_MULT

        tc_sm      = self._tc.spread_mult()
        tc_lm      = self._tc.lot_mult()
        rg_es      = self._regime.edge_scale()
        tc_lm     *= (1.0 + self._cfg.EDGE_LOT_SCALE  * edge * rg_es)

        toxic_penalty  = self._cfg.TOXIC_LOT_PENALTY if toxic_active else 1.0
        widen          = max(0.5, widen * tc_sm * self._spread_exp.choose())
        regime_qmax    = min(self._cfg.EDGE_QMAX_CAP,
                             int(regime_qmax * (1.0 + self._cfg.EDGE_QMAX_SCALE * edge * rg_es)))

        # BUG-3 FIX: _lots() was defined but never called; bl_telem/al_telem were undefined
        bl_telem, al_telem = self._lots(q_lots, tc_lm, regime_qmax, toxic_penalty)

        # ── TELEMETRY ── placed here (post-edge, post-lot) so all fields are valid.
        # Fires regardless of subsequent guard/return (unchanged, queue-hold etc.).
        _tnow = time.time()
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
            })
            self._last_telem_t = _tnow

        fc        = self._fill_cb.fill_count
        pace_mult = self._pace.multiplier(fc)
        fill_proj = self._pace.projected_fills(fc)
        inv_var   = self._inv_var()
        self._calib.update(rs_val, fill_proj, inv_var)
        self._k_adapter.update(fill_proj, rs_val)

        w_qi, w_flow, w_mom = self._sig_perf.weights()

        if self._tick_n >= self._next_rl_tick:
            self._rl_offset    = random.choice([-1, 0, 0, 1]) * 0.01
            self._next_rl_tick = (self._tick_n
                                  + random.randint(self._cfg.ANTI_RL_INTERVAL_LO,
                                                   self._cfg.ANTI_RL_INTERVAL_HI))
        else:
            self._rl_offset = 0.0

        inv_target = 0
        if self._cascade_active:
            inv_target = int(self._cfg.CASCADE_INV_TARGET * (1.0 if qi > 0 else -1.0))

        new_bid, new_ask = self._solver.compute(
            microprice, sigma_eff, q_lots, tau,
            qi_lean=qi, flow_lean=flow_norm,
            momentum_adj=momentum_adj, qi_accel=qi_accel,
            widen_mult=widen, pace_mult=pace_mult,
            regime_gamma=self._regime.gamma(),
            w_qi=w_qi, w_flow=w_flow, w_mom=w_mom,
            inv_target=inv_target)

        new_bid, new_ask = self._apply_liquidity_skew(
            new_bid, new_ask, qi, flow_norm, w_qi, w_flow)

        new_bid, new_ask = self._apply_liquidity_dominance(
            bid_p, ask_p, bid_sz, ask_sz, new_bid, new_ask, spread)

        new_bid = round(new_bid + self._rl_offset, 2)
        new_ask = round(new_ask + self._rl_offset, 2)
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

        do_layer = self._layering_ok(q_lots, sigma_eff, regime_qmax)
        if do_layer:
            outer_bid = round(new_bid - 0.01, 2)
            outer_ask = round(new_ask + 0.01, 2)
            ll = self._cfg.LAYER_LOTS
            if allow_bid and outer_bid > 0:
                self._qeng.submit_limit(trader, self._ticker, 'BID', outer_bid, ll)
            if allow_ask:
                self._qeng.submit_limit(trader, self._ticker, 'ASK', outer_ask, ll)

        if (not toxic_active
                and tau > self._cfg.EOD_FLATTEN_START_S
                and random.random() < self._cfg.DECOY_PROB):
            d_ticks = self._cfg.DECOY_TICKS * 0.01
            d_bid   = round(bid_p - d_ticks, 2)
            d_ask   = round(ask_p + d_ticks, 2)
            if d_bid > 0:
                self._qeng.submit_limit(trader, self._ticker, 'BID', d_bid, 1)
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
            print(
                f"[{self._tick_n:7d}][{mode_s}{layer_s}][{tc_s}][{rg_s}][{tok_s}] "
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
        # ── end of tick() ──────────────────────────────────────────────────────

    def run(self, trader):
        """Main loop — called once from main() on the SHIFT live platform."""
        print(
            f"[PREDATOR v12] {self._ticker} "
            f"γ={self._cfg.GAMMA:.3f} k={self._cfg.K_BASE:.2f} "
            f"αqi={self._cfg.ALPHA_QI} target_fills={self._cfg.TARGET_FILLS}",
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
                q = self._q_cache
                if abs(q) > 0:
                    print(f"[PREDATOR v12] EOD flatten q={q}", flush=True)
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
                f"[PREDATOR v12] Final PnL=${self._pnl_cache:,.2f} "
                f"Fills={self._fill_cb.fill_count} "
                f"Sharpe={self._sharpe.sharpe:.3f} "
                f"BestExp={self._spread_exp.best():.1f}",
                flush=True)


def main():
    import argparse
    p = argparse.ArgumentParser(description="Predator v12 — HFTC-26 Competition Bot")
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

        def _on_exec(cb):
            _cbs.append(cb)
            def _bridge(order):
                for c in _cbs:
                    try:
                        c(trader, order.id)
                    except Exception as e:
                        print(f"[ExecCB] {e}", file=sys.stderr)
            try:
                trader.subExecutionNotice(_bridge)
            except AttributeError:
                print("[WARN] subExecutionNotice unavailable", file=sys.stderr)

        trader.on_execution_updated = _on_exec
        trader.on_execution_updated(fill_cb)

        bot.run(trader)


if __name__ == "__main__":
    main()
