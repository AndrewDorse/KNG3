"""Evaluate SHAMAN v1 pattern rules (family + pattern_key) at bar index ``t`` (last bar of suffix = closed)."""

from __future__ import annotations

import re
from typing import Any, Literal

Pred = Literal["R", "G"]


def _rg(i: int, o: list[float], c: list[float]) -> str | None:
    if c[i] > o[i]:
        return "G"
    if c[i] < o[i]:
        return "R"
    return None


def _build_aux(o: list[float], c: list[float], v: list[float], hi: list[float], lo: list[float]) -> dict[str, Any]:
    n = len(o)
    vol_ma: list[float] = []
    for i in range(n):
        lo_i = max(0, i - 20)
        s = sum(v[lo_i:i])
        ln = max(1, i - lo_i)
        vol_ma.append(s / ln)
    body_pct = [abs(c[i] - o[i]) / max(o[i], 1e-12) for i in range(n)]
    bodies = sorted(body_pct)
    t1 = bodies[int(0.33 * (len(bodies) - 1))]
    t2 = bodies[int(0.66 * (len(bodies) - 1))]
    rng_pct = [(hi[i] - lo[i]) / max(o[i], 1e-12) for i in range(n)]
    rq = sorted(rng_pct)
    r1 = rq[int(0.33 * (len(rq) - 1))]
    r2 = rq[int(0.66 * (len(rq) - 1))]
    return {
        "body_pct": body_pct,
        "vol_ma": vol_ma,
        "t1": t1,
        "t2": t2,
        "rng_pct": rng_pct,
        "r1": r1,
        "r2": r2,
    }


def _body_bucket(i: int, aux: dict[str, Any]) -> str:
    b = aux["body_pct"][i]
    t1, t2 = aux["t1"], aux["t2"]
    if b <= t1 + 1e-12:
        return "S"
    if b <= t2 + 1e-12:
        return "M"
    return "L"


def _vol_bucket(i: int, v: list[float], aux: dict[str, Any]) -> str:
    vm = aux["vol_ma"][i]
    if vm <= 1e-12:
        return "l"
    r = v[i] / vm
    if r < 0.85:
        return "l"
    if r > 1.15:
        return "h"
    return "m"


def _range_bucket(i: int, aux: dict[str, Any]) -> str:
    r = aux["rng_pct"][i]
    r1, r2 = aux["r1"], aux["r2"]
    if r <= r1 + 1e-12:
        return "n"
    if r <= r2 + 1e-12:
        return "w"
    return "W"


def _trend_bucket(i: int, c: list[float], lookback: int) -> str:
    if i < lookback:
        return "F"
    ret = (c[i] - c[i - lookback]) / max(c[i - lookback], 1e-12)
    if lookback <= 6:
        th = 0.0008
    elif lookback <= 12:
        th = 0.0015
    else:
        th = 0.0025
    if ret > th:
        return "U"
    if ret < -th:
        return "D"
    return "F"


def _vol_regime_bucket(i: int, v: list[float], aux: dict[str, Any]) -> str:
    vm = aux["vol_ma"][i]
    if vm <= 1e-12:
        return "N"
    r = v[i] / vm
    if r < 0.7:
        return "D"
    if r > 1.5:
        return "S"
    return "N"


def _combo_feature_value(name: str, i: int, o: list[float], c: list[float], v: list[float], aux: dict[str, Any]) -> str | None:
    if name == "rg2":
        if i < 1:
            return None
        a, b = _rg(i - 1, o, c), _rg(i, o, c)
        if a is None or b is None:
            return None
        return f"{a}{b}"
    if name == "rg3":
        if i < 2:
            return None
        a, b, d = _rg(i - 2, o, c), _rg(i - 1, o, c), _rg(i, o, c)
        if a is None or b is None or d is None:
            return None
        return f"{a}{b}{d}"
    if name == "rg4":
        if i < 3:
            return None
        a, b, d, e = _rg(i - 3, o, c), _rg(i - 2, o, c), _rg(i - 1, o, c), _rg(i, o, c)
        if a is None or b is None or d is None or e is None:
            return None
        return f"{a}{b}{d}{e}"
    if name == "rng":
        return _range_bucket(i, aux)
    if name == "vr":
        return _vol_regime_bucket(i, v, aux)
    if name == "tr4":
        return _trend_bucket(i, c, 4)
    if name == "tr8":
        return _trend_bucket(i, c, 8)
    if name == "tr16":
        return _trend_bucket(i, c, 16)
    if name == "tr6":
        return _trend_bucket(i, c, 6)
    if name == "tr12":
        return _trend_bucket(i, c, 12)
    if name == "tr24":
        return _trend_bucket(i, c, 24)
    if name == "tr32":
        return _trend_bucket(i, c, 32)
    if name == "tr48":
        return _trend_bucket(i, c, 48)
    if name == "color":
        return _rg(i, o, c)
    if name == "body":
        return _body_bucket(i, aux)
    return None


def _token(i: int, o: list[float], c: list[float], v: list[float], aux: dict[str, Any]) -> str | None:
    x = _rg(i, o, c)
    if x is None:
        return None
    return f"{x}{_body_bucket(i, aux)}{_vol_bucket(i, v, aux)}"


def _valid_tok(s: str) -> bool:
    return len(s) == 3 and s[0] in "RG" and s[1] in "SML" and s[2] in "lmh"


def match_rule(
    family: str,
    pattern_key: str,
    o: list[float],
    c: list[float],
    v: list[float],
    hi: list[float],
    lo: list[float],
    t: int,
    *,
    aux: dict[str, Any] | None = None,
) -> bool:
    """Return True iff pattern matches at closed bar index ``t``."""
    if aux is None:
        aux = _build_aux(o, c, v, hi, lo)
    n = len(o)
    if t < 0 or t >= n:
        return False

    if family == "A_token_chain":
        parts = pattern_key.split(">")
        L = len(parts)
        if t < L - 1:
            return False
        for j, p in enumerate(parts):
            if not _valid_tok(p):
                return False
            got = _token(t - L + 1 + j, o, c, v, aux)
            if got != p:
                return False
        return True

    if family == "B_rg_suffix_lastTok":
        if "|last=" not in pattern_key:
            return False
        rgs, last = pattern_key.split("|last=", 1)
        if not last or not _valid_tok(last):
            return False
        Ls = len(rgs)
        if Ls < 1 or t < Ls - 1:
            return False
        for j in range(Ls):
            if _rg(t - Ls + 1 + j, o, c) != rgs[j]:
                return False
        return _token(t, o, c, v, aux) == last

    if family == "C_rg_suffix_last2Tok":
        m = re.match(r"^(.+)\|t-1=([^|]+)\|t=(.+)$", pattern_key)
        if not m:
            return False
        rgs, ta, tb = m.group(1), m.group(2), m.group(3)
        if not _valid_tok(ta) or not _valid_tok(tb):
            return False
        ls = len(rgs)
        if t < ls or t < 1:
            return False
        for j in range(ls):
            if _rg(t - ls + 1 + j, o, c) != rgs[j]:
                return False
        return _token(t - 1, o, c, v, aux) == ta and _token(t, o, c, v, aux) == tb

    if family == "D_rg_rng_lastTok":
        rgs = None
        last = None
        rng_need = None
        for part in pattern_key.split("|"):
            if part.startswith("last="):
                last = part[5:]
            elif part.startswith("rng="):
                rng_need = part[4:]
            elif "=" not in part and part:
                rgs = part
        if rgs is None or last is None or rng_need is None or not _valid_tok(last):
            return False
        Ls = len(rgs)
        if t < Ls - 1:
            return False
        for j in range(Ls):
            if _rg(t - Ls + 1 + j, o, c) != rgs[j]:
                return False
        if _token(t, o, c, v, aux) != last:
            return False
        return _range_bucket(t, aux) == rng_need

    if family in {
        "M_combo2", "N_combo3", "M15_combo2", "N15_combo3",
        "Z5_combo2", "Z5_combo3", "Z5_combo4", "Z5_combo5", "Z5_combo6",
        "V5_combo2", "V5_combo3", "V5_combo4", "V5_combo5", "V5_combo6",
        "Z15_combo2", "Z15_combo3", "Z15_combo4", "Z15_combo5", "Z15_combo6",
        "V15_combo2", "V15_combo3", "V15_combo4", "V15_combo5", "V15_combo6",
    }:
        parts = [p for p in pattern_key.split("&") if p]
        if family in {
            "M_combo2", "M15_combo2",
            "Z5_combo2", "Z15_combo2", "V5_combo2", "V15_combo2",
        } and len(parts) != 2:
            return False
        if family in {
            "N_combo3", "N15_combo3",
            "Z5_combo3", "Z15_combo3", "V5_combo3", "V15_combo3",
        } and len(parts) != 3:
            return False
        if family in {"Z5_combo4", "Z15_combo4", "V5_combo4", "V15_combo4"} and len(parts) != 4:
            return False
        if family in {"Z5_combo5", "Z15_combo5", "V5_combo5", "V15_combo5"} and len(parts) != 5:
            return False
        if family in {"Z5_combo6", "Z15_combo6", "V5_combo6", "V15_combo6"} and len(parts) != 6:
            return False
        for p in parts:
            if "=" not in p:
                return False
            name, expect = p.split("=", 1)
            got = _combo_feature_value(name.strip(), t, o, c, v, aux)
            if got is None or got != expect.strip():
                return False
        return True

    return False


def aggregate_signals(
    rules: list[dict[str, Any]],
    o: list[float],
    c: list[float],
    v: list[float],
    hi: list[float],
    lo: list[float],
    t: int,
) -> tuple[int, int]:
    """Returns (n_pred_g, n_pred_r) for rules that match at ``t``."""
    aux = _build_aux(o, c, v, hi, lo)
    ng = nr = 0
    for rule in rules:
        if not match_rule(
            str(rule["family"]),
            str(rule["pattern_key"]),
            o,
            c,
            v,
            hi,
            lo,
            t,
            aux=aux,
        ):
            continue
        if rule.get("pred") == "G":
            ng += 1
        elif rule.get("pred") == "R":
            nr += 1
    return ng, nr
