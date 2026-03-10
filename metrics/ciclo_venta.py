"""
metrics/ciclo_venta.py — Cálculos estadísticos del ciclo de venta
"""
import numpy as np
import pandas as pd

def stats_ciclo(df, col="ciclo_dias_limpio"):
    s = df[col].dropna() if col in df.columns else pd.Series(dtype=float)
    if s.empty:
        return {k: None for k in ["n","mean","median","std","min","max","p10","q1","q3","p90","iqr","skewness","kurtosis"]}
    return {
        "n":        int(len(s)),
        "mean":     round(float(s.mean()), 2),
        "median":   round(float(s.median()), 2),
        "std":      round(float(s.std()), 2),
        "min":      round(float(s.min()), 1),
        "max":      round(float(s.max()), 1),
        "p10":      round(float(s.quantile(.10)), 2),
        "q1":       round(float(s.quantile(.25)), 2),
        "q3":       round(float(s.quantile(.75)), 2),
        "p90":      round(float(s.quantile(.90)), 2),
        "iqr":      round(float(s.quantile(.75) - s.quantile(.25)), 2),
        "skewness": round(float(s.skew()), 3),
        "kurtosis": round(float(s.kurtosis()), 3),
    }

def kpis_principales(df):
    firmados = df[df["convirtio"]] if "convirtio" in df.columns else df[df["contract_id"].notna()]
    total  = len(df)
    n_firm = len(firmados)
    ciclo  = firmados["ciclo_dias_limpio"].dropna() if "ciclo_dias_limpio" in firmados.columns else pd.Series()
    monto  = firmados["monto_credito"].dropna()     if "monto_credito" in firmados.columns else pd.Series()
    ingreso= firmados["ingresos"].dropna()           if "ingresos" in firmados.columns else pd.Series()
    ratio  = firmados["ratio_deuda_ingreso"].dropna()if "ratio_deuda_ingreso" in firmados.columns else pd.Series()
    plazo  = firmados["plazo_credito"].dropna()      if "plazo_credito" in firmados.columns else pd.Series()
    return {
        "total_leads":    total,
        "contratos":      n_firm,
        "conversion_pct": round(n_firm/total*100,1) if total else 0,
        "ciclo_mean":     round(float(ciclo.mean()),1)   if len(ciclo) else None,
        "ciclo_median":   round(float(ciclo.median()),1) if len(ciclo) else None,
        "ciclo_std":      round(float(ciclo.std()),1)    if len(ciclo) else None,
        "monto_mean":     round(float(monto.mean()),0)   if len(monto) else None,
        "ingreso_mean":   round(float(ingreso.mean()),0) if len(ingreso) else None,
        "ratio_di_mean":  round(float(ratio.mean()),2)   if len(ratio) else None,
        "plazo_mean":     round(float(plazo.mean()),0)   if len(plazo) else None,
    }

def tendencia_semanal(df):
    firmados = df[df["convirtio"]].copy() if "convirtio" in df.columns else df[df["contract_id"].notna()].copy()
    if "semana_firma" not in firmados.columns or firmados["semana_firma"].isna().all():
        return pd.DataFrame()
    agg = firmados.groupby("semana_firma").agg(
        contratos=("contract_id","count"),
        ciclo_prom=("ciclo_dias_limpio","mean"),
        ciclo_med=("ciclo_dias_limpio","median"),
        monto_prom=("monto_credito","mean"),
    ).reset_index().sort_values("semana_firma")
    agg["ciclo_prom"]    = agg["ciclo_prom"].round(1)
    agg["monto_prom"]    = agg["monto_prom"].round(0)
    agg["contratos_wow"] = agg["contratos"].pct_change().round(3)*100
    return agg

def ciclo_por_dimension(df, dimension):
    firmados = df[df["convirtio"]].copy() if "convirtio" in df.columns else df[df["contract_id"].notna()].copy()
    if dimension not in firmados.columns:
        return pd.DataFrame()
    r = (firmados.dropna(subset=[dimension,"ciclo_dias_limpio"])
         .groupby(dimension)["ciclo_dias_limpio"]
         .agg(n="count",mean="mean",median="median",std="std",
              q1=lambda x: x.quantile(.25), q3=lambda x: x.quantile(.75),
              p90=lambda x: x.quantile(.90))
         .reset_index().round(2).sort_values("mean"))
    r["iqr"] = (r["q3"]-r["q1"]).round(2)
    return r
