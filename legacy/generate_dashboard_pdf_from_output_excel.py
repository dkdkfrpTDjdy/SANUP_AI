# -*- coding: utf-8 -*-
"""
기존 파이프라인이 생성한 output Excel을 입력으로 받아,
추가적인 API 요약 + 차트 + 표를 포함한 PDF 대시보드를 생성하는 별도 스크립트.

의도:
- 기존 RAW 처리 / 대시보드 생성 로직은 건드리지 않는다.
- 기존 코드가 만든 output Excel만 downstream 입력으로 사용한다.
- 정제RAW / 팀별 대시보드 / 개인별 대시보드 시트를 읽어 보고서용 지표를 재구성한다.
- Bedrock Converse API로 설명 문구를 생성하고, 실패 시 로컬 fallback을 사용한다.
- matplotlib + WeasyPrint로 PDF를 생성한다.
"""

from __future__ import annotations
import matplotlib.ticker as mtick
import seaborn as sns
import argparse
import base64
import io
import json
import math
import os
import re
import traceback
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

try:
    import boto3
    from botocore.config import Config
except Exception:
    boto3 = None
    Config = None

# matplotlib은 import 시 GUI backend 문제가 날 수 있어 미리 Agg 지정
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm


AWS_REGION = "ap-northeast-2"
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "")
BEDROCK_API_KEY = os.getenv("AWS_BEARER_TOKEN_BEDROCK", "")
WEASYPRINT_DLL_DIR = os.getenv("WEASYPRINT_DLL_DIRECTORIES", r"C:\msys64\mingw64\bin")

# =========================================================
# 공통 유틸
# =========================================================
def print_title(title: str):
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)

def normalize_summary_json(
    raw: Dict[str, Any],
    payload: Dict[str, Any],
    source: str,
) -> Dict[str, Any]:
    fallback = build_fallback_summary_json(payload)
    raw = raw if isinstance(raw, dict) else {}

    summary_lines = raw.get("summary_lines", fallback["summary_lines"])
    numeric_observations = raw.get("numeric_observations", fallback["numeric_observations"])
    composition_notes = raw.get("composition_notes", fallback["composition_notes"])
    footnotes = raw.get("footnotes", fallback["footnotes"])

    if not isinstance(summary_lines, list):
        summary_lines = fallback["summary_lines"]
    if not isinstance(numeric_observations, list):
        numeric_observations = fallback["numeric_observations"]
    if not isinstance(composition_notes, list):
        composition_notes = fallback["composition_notes"]
    if not isinstance(footnotes, list):
        footnotes = fallback["footnotes"]

    while len(summary_lines) < 3:
        summary_lines.append(fallback["summary_lines"][min(len(summary_lines), len(fallback['summary_lines']) - 1)])
    while len(numeric_observations) < 3:
        numeric_observations.append(
            fallback["numeric_observations"][min(len(numeric_observations), len(fallback["numeric_observations"]) - 1)]
        )
    while len(composition_notes) < 2:
        composition_notes.append(
            fallback["composition_notes"][min(len(composition_notes), len(fallback["composition_notes"]) - 1)]
        )
    while len(footnotes) < 1:
        footnotes.append(fallback["footnotes"][0])

    return {
        "headline": raw.get("headline") or fallback["headline"],
        "subheadline": raw.get("subheadline") or fallback["subheadline"],
        "summary_lines": summary_lines[:3],
        "numeric_observations": numeric_observations[:3],
        "composition_notes": composition_notes[:2],
        "footnotes": footnotes[:1],
        "_meta": {
            "summary_source": source,
        },
    }

def normalize_item_section_json(
    raw: Dict[str, Any],
    block: Dict[str, Any],
    cur_label: str,
    source: str,
) -> Dict[str, Any]:
    fallback_item = build_fallback_item_section(block, cur_label)
    raw = raw if isinstance(raw, dict) else {}

    item_name = normalize_text(raw.get("item")) or fallback_item["item"]

    quant_lines = raw.get("quant_lines", fallback_item["quant_lines"])
    creative_lines = raw.get("creative_lines", fallback_item["creative_lines"])

    if not isinstance(quant_lines, list):
        quant_lines = fallback_item["quant_lines"]
    if not isinstance(creative_lines, list):
        creative_lines = fallback_item["creative_lines"]

    quant_lines = [normalize_text(x) for x in quant_lines if normalize_text(x)]
    creative_lines = [normalize_text(x) for x in creative_lines if normalize_text(x)]

    def has_bad_ratio_text(lines: List[str]) -> bool:
        for line in lines:
            if "비중" in line and "%" not in line:
                return True
        return False

    if has_bad_ratio_text(quant_lines + creative_lines):
        quant_lines = fallback_item["quant_lines"]
        creative_lines = fallback_item["creative_lines"]
        source = "fallback"

    while len(quant_lines) < 3:
        quant_lines.append(fallback_item["quant_lines"][len(quant_lines)])
    while len(creative_lines) < 3:
        creative_lines.append(fallback_item["creative_lines"][len(creative_lines)])

    return {
        "item": item_name,
        "lines": quant_lines[:3] + creative_lines[:3],
        "_source": source,
    }

def normalize_text(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x)
    s = s.replace("\u00A0", " ")
    s = s.replace("\n", " ")
    s = s.replace("\r", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def normalize_colname(c) -> str:
    if pd.isna(c):
        return ""
    s = normalize_text(c)
    if s in ("1.0", "1"):
        return "1"
    return s


def normalize_colnames(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [normalize_colname(c) for c in out.columns]
    return out


def coalesce_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)
    dupes = [c for c in pd.Index(cols).unique().tolist() if cols.count(c) > 1]
    if not dupes:
        return df

    out = pd.DataFrame(index=df.index)
    handled = set()
    for c in cols:
        if c in handled:
            continue
        same_cols = [x for x in cols if x == c]
        if len(same_cols) == 1:
            out[c] = df[c]
        else:
            temp = df.loc[:, [col for col in df.columns if col == c]].copy()
            merged = temp.iloc[:, 0]
            for i in range(1, temp.shape[1]):
                merged = merged.combine_first(temp.iloc[:, i])
            out[c] = merged
        handled.add(c)
    return out


def safe_divide(a, b):
    if b is None or pd.isna(b) or b == 0:
        return np.nan
    if a is None or pd.isna(a):
        return np.nan
    return a / b

def decorate_prompt_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for r in records:
        rr = dict(r)

        if "당월 렌탈료" in rr:
            rr["당월 렌탈료_fmt"] = won(rr.get("당월 렌탈료"))
        if "전월 렌탈료" in rr:
            rr["전월 렌탈료_fmt"] = won(rr.get("전월 렌탈료"))
        if "당월 임대대수" in rr:
            rr["당월 임대대수_fmt"] = count_unit(rr.get("당월 임대대수"))
        if "전월 임대대수" in rr:
            rr["전월 임대대수_fmt"] = count_unit(rr.get("전월 임대대수"))
        if "아이템 내 비중" in rr:
            rr["아이템 내 비중_fmt"] = pct_fmt(rr.get("아이템 내 비중"), 1)

        if "당월 팀 렌탈료" in rr:
            rr["당월 팀 렌탈료_fmt"] = won(rr.get("당월 팀 렌탈료"))
        if "전월 팀 렌탈료" in rr:
            rr["전월 팀 렌탈료_fmt"] = won(rr.get("전월 팀 렌탈료"))
        if "당월 팀 임대대수" in rr:
            rr["당월 팀 임대대수_fmt"] = count_unit(rr.get("당월 팀 임대대수"))
        if "전월 팀 임대대수" in rr:
            rr["전월 팀 임대대수_fmt"] = count_unit(rr.get("전월 팀 임대대수"))
        if "아이템 내 팀 비중" in rr:
            rr["아이템 내 팀 비중_fmt"] = pct_fmt(rr.get("아이템 내 팀 비중"), 1)

        if "당월 대당렌탈료" in rr:
            rr["당월 대당렌탈료_fmt"] = won(rr.get("당월 대당렌탈료"))

        if "당월 세부항목 렌탈료" in rr:
            rr["당월 세부항목 렌탈료_fmt"] = won(rr.get("당월 세부항목 렌탈료"))
        if "전월 세부항목 렌탈료" in rr:
            rr["전월 세부항목 렌탈료_fmt"] = won(rr.get("전월 세부항목 렌탈료"))
        if "당월 세부항목 임대대수" in rr:
            rr["당월 세부항목 임대대수_fmt"] = count_unit(rr.get("당월 세부항목 임대대수"))
        if "전월 세부항목 임대대수" in rr:
            rr["전월 세부항목 임대대수_fmt"] = count_unit(rr.get("전월 세부항목 임대대수"))
        if "세부항목 내 비중" in rr:
            rr["세부항목 내 비중_fmt"] = pct_fmt(rr.get("세부항목 내 비중"), 1)

        if "당월 현장 렌탈료" in rr:
            rr["당월 현장 렌탈료_fmt"] = won(rr.get("당월 현장 렌탈료"))
        if "당월 현장 임대대수" in rr:
            rr["당월 현장 임대대수_fmt"] = count_unit(rr.get("당월 현장 임대대수"))
        if "당월 팀 총렌탈료" in rr:
            rr["당월 팀 총렌탈료_fmt"] = won(rr.get("당월 팀 총렌탈료"))
        if "당월 팀 총임대대수" in rr:
            rr["당월 팀 총임대대수_fmt"] = count_unit(rr.get("당월 팀 총임대대수"))
        if "팀내 현장 렌탈료 비중" in rr:
            rr["팀내 현장 렌탈료 비중_fmt"] = pct_fmt(rr.get("팀내 현장 렌탈료 비중"), 1)
        if "팀내 현장 임대대수 비중" in rr:
            rr["팀내 현장 임대대수 비중_fmt"] = pct_fmt(rr.get("팀내 현장 임대대수 비중"), 1)

        out.append(rr)

    return out

def month_label_from_yymm(yymm: str) -> str:
    return f"{int(yymm[2:])}월"


def sort_yymm(values: List[str]) -> List[str]:
    return sorted(values, key=lambda x: (int(x[:2]), int(x[2:])))


def won(v: float | int | None) -> str:
    if v is None or pd.isna(v):
        return "-"
    x = float(v)
    s = f"{x:,.2f}".rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return f"{s}원"


def count_fmt(v: float | int | None) -> str:
    if v is None or pd.isna(v):
        return "-"
    return f"{int(v):,}"


def pct_fmt(v: float | int | None, digits: int = 2) -> str:
    if v is None or pd.isna(v):
        return "-"
    s = f"{float(v) * 100:.{digits}f}".rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return f"{s}%"


def setup_matplotlib_font():
    candidates = [
        "Malgun Gothic",
        "AppleGothic",
        "NanumGothic",
        "Noto Sans CJK KR",
        "DejaVu Sans",
    ]
    installed = {f.name for f in fm.fontManager.ttflist}
    for name in candidates:
        if name in installed:
            plt.rcParams["font.family"] = name
            break
    plt.rcParams["axes.unicode_minus"] = False




# =========================================================
# Excel 로드
# =========================================================
def discover_processed_months(output_excel: str) -> List[str]:
    xls = pd.ExcelFile(output_excel)
    months: List[str] = []
    for name in xls.sheet_names:
        m = re.match(r"^(\d{4})\s*정제RAW$", str(name).strip())
        if m:
            months.append(m.group(1))
    months = sort_yymm(months)
    if len(months) < 2:
        raise ValueError("정제RAW 시트가 최소 2개 이상 필요합니다.")
    return months


def load_processed_df(output_excel: str, yymm: str) -> pd.DataFrame:
    df = pd.read_excel(output_excel, sheet_name=f"{yymm} 정제RAW")
    df = normalize_colnames(df)
    df = coalesce_columns(df)
    return df


def make_bases(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    all_base = df[df["대상여부"] == 1].copy()
    asset_base = all_base[all_base["자산구분_std"].isin(["당사자산", "임차자산"])].copy()
    return {"all_base": all_base, "asset_base": asset_base}

SITE_KEY_COLS = ["고객사", "BP번호", "현장", "배송지 주소"]

ITEM_SITE_KEY_COLS = ["아이템", "고객사", "BP번호", "현장", "배송지 주소"]


def map_insight_item(item_value: Any) -> str:
    s = normalize_text(item_value)
    s_upper = s.upper().replace(" ", "")

    if not s:
        return "그외"

    # 아이템 컬럼 기준
    if "AWP" in s_upper:
        return "AWP"

    if s_upper == "FL" or s_upper.startswith("FL"):
        return "FL"

    return "그외"

def normalize_site_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for c in ["아이템", "고객사", "BP번호", "현장", "배송지 주소"]:
        if c not in out.columns:
            out[c] = ""
        out[c] = out[c].apply(normalize_text)

    out["아이템"] = out["아이템"].replace("", "아이템 미기재")
    out["고객사"] = out["고객사"].replace("", "고객사 미기재")
    out["BP번호"] = out["BP번호"].replace("", "-")

    # 현장이 없으면 고객사 사용
    out["현장"] = out["현장"].replace("", np.nan)
    out.loc[out["현장"].isna(), "현장"] = out.loc[out["현장"].isna(), "고객사"]
    out["현장"] = out["현장"].fillna("현장 미기재")

    out["배송지 주소"] = out["배송지 주소"].replace("", "주소 미기재")

    # 아이템 컬럼 기준으로 AWP / FL / 그외 분류
    out["인사이트아이템"] = out["아이템"].apply(map_insight_item)

    return out

def ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    return out


def mode_text(series: pd.Series) -> str:
    vals = [normalize_text(x) for x in series if normalize_text(x)]
    if not vals:
        return ""
    return pd.Series(vals).mode().iloc[0]


def won_thousand(v: float | int | None) -> str:
    if v is None or pd.isna(v):
        return "-"
    return f"{int(float(v) // 1000):,}천원"


def count_unit(v: float | int | None) -> str:
    if v is None or pd.isna(v):
        return "-"
    return f"{int(v):,}대"


def site_label(customer: Any, site: Any) -> str:
    customer = normalize_text(customer)
    site = normalize_text(site)
    if customer and site and customer != site:
        return f"{customer} / {site}"
    return customer or site or "미상 현장"


def annotate_line_values(ax, formatter, y_offset: int = 8):
    if not ax.lines:
        return

    line = ax.lines[0]
    xdata = line.get_xdata()
    ydata = line.get_ydata()

    for x, y in zip(xdata, ydata):
        if pd.isna(y):
            continue
        ax.annotate(
            formatter(y),
            (x, float(y)),
            textcoords="offset points",
            xytext=(0, y_offset),
            ha="center",
            fontsize=8,
        )


def annotate_barh_values(ax, formatter):
    xmin, xmax = ax.get_xlim()
    pad = (xmax - xmin) * 0.015
    for p in ax.patches:
        width = p.get_width()
        y = p.get_y() + p.get_height() / 2
        ax.text(
            width + pad,
            y,
            formatter(width),
            va="center",
            ha="left",
            fontsize=8,
        )

# =========================================================
# 지표 생성
# =========================================================
def build_monthly_kpi_rows(monthly_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    required = ["대상여부", "자산구분_std", "렌탈료", "연장 여부", "연장대상구분"]

    for yymm in sort_yymm(list(monthly_dfs.keys())):
        df = ensure_columns(monthly_dfs[yymm], required).copy()
        bases = make_bases(df)

        all_base = bases["all_base"].copy()
        asset_base = bases["asset_base"].copy()

        asset_base["렌탈료"] = pd.to_numeric(asset_base["렌탈료"], errors="coerce").fillna(0)

        rent = float(asset_base["렌탈료"].sum())
        ext_cnt = int((all_base["연장 여부"] == "연장").sum())
        ext_target = int((all_base["연장대상구분"] == "연장대상").sum())
        ext_rate = safe_divide(ext_cnt, ext_target)

        rows.append(
            {
                "yymm": yymm,
                "월": month_label_from_yymm(yymm),
                "임대대수": int(len(all_base)),
                "렌탈료": rent,
                "연장건수": ext_cnt,
                "연장대상": ext_target,
                "연장율": ext_rate,
            }
        )

    return pd.DataFrame(rows)

def build_top_items(cur_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    bases = make_bases(cur_df)
    all_base = bases["all_base"]
    asset_base = bases["asset_base"]

    count_df = (
        all_base.groupby("아이템", dropna=False)
        .agg(
            임대대수=("코드", "size"),
            연장건수=("연장 여부", lambda s: int((s == "연장").sum())),
            연장대상=("연장대상구분", lambda s: int((s == "연장대상").sum())),
        )
        .reset_index()
    )

    rent_df = (
        asset_base.groupby("아이템", dropna=False)
        .agg(
            렌탈료=("렌탈료", "sum"),
            취득가=("취득가_num", "sum"),
            자산구분_count=("코드", "size"),
        )
        .reset_index()
    )

    out = count_df.merge(rent_df, on="아이템", how="left")
    out["렌탈료"] = out["렌탈료"].fillna(0.0)
    out["취득가"] = out["취득가"].fillna(0.0)
    out["자산구분_count"] = out["자산구분_count"].fillna(0).astype(int)
    out["연장율"] = out.apply(lambda r: safe_divide(r["연장건수"], r["연장대상"]), axis=1)
    out["회수율"] = out.apply(lambda r: safe_divide(r["렌탈료"], r["취득가"]), axis=1)
    out["대당렌탈료"] = out.apply(lambda r: safe_divide(r["렌탈료"], r["자산구분_count"]), axis=1)
    out = out.sort_values(["렌탈료", "임대대수"], ascending=[False, False]).reset_index(drop=True)
    return out.head(top_n)

def build_top_teams(cur_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    required = ["영업팀", "코드", "신규여부", "연장 여부", "연장대상구분", "렌탈료", "취득가_num"]
    df = ensure_columns(cur_df, required).copy()

    bases = make_bases(df)
    all_base = bases["all_base"].copy()
    asset_base = bases["asset_base"].copy()

    all_base["영업팀"] = all_base["영업팀"].apply(normalize_text).replace("", "영업팀 미기재")
    asset_base["영업팀"] = asset_base["영업팀"].apply(normalize_text).replace("", "영업팀 미기재")

    asset_base["렌탈료"] = pd.to_numeric(asset_base["렌탈료"], errors="coerce").fillna(0)
    asset_base["취득가_num"] = pd.to_numeric(asset_base["취득가_num"], errors="coerce").fillna(0)

    count_df = (
        all_base.groupby("영업팀", dropna=False)
        .agg(
            임대대수=("코드", "size"),
            신규건수=("신규여부", lambda s: int((s == "신규").sum())),
            연장건수=("연장 여부", lambda s: int((s == "연장").sum())),
            연장대상=("연장대상구분", lambda s: int((s == "연장대상").sum())),
        )
        .reset_index()
    )

    rent_df = (
        asset_base.groupby("영업팀", dropna=False)
        .agg(
            렌탈료=("렌탈료", "sum"),
            취득가=("취득가_num", "sum"),
            자산구분_count=("코드", "size"),
        )
        .reset_index()
    )

    out = count_df.merge(rent_df, on="영업팀", how="outer")
    out["임대대수"] = out["임대대수"].fillna(0).astype(int)
    out["신규건수"] = out["신규건수"].fillna(0).astype(int)
    out["연장건수"] = out["연장건수"].fillna(0).astype(int)
    out["연장대상"] = out["연장대상"].fillna(0).astype(int)
    out["렌탈료"] = out["렌탈료"].fillna(0.0)
    out["취득가"] = out["취득가"].fillna(0.0)
    out["자산구분_count"] = out["자산구분_count"].fillna(0).astype(int)

    out["연장율"] = out.apply(lambda r: safe_divide(r["연장건수"], r["연장대상"]), axis=1)
    out["회수율"] = out.apply(lambda r: safe_divide(r["렌탈료"], r["취득가"]), axis=1)
    out["대당렌탈료"] = out.apply(lambda r: safe_divide(r["렌탈료"], r["자산구분_count"]), axis=1)

    out = out.sort_values(["렌탈료", "임대대수"], ascending=[False, False]).reset_index(drop=True)
    return out.head(top_n)

def build_item_site_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "아이템", "고객사", "BP번호", "현장", "배송지 주소",
        "코드", "영업팀", "신규여부", "연장 여부", "연장대상구분", "렌탈료"
    ]
    df = ensure_columns(df, required)
    df = normalize_site_keys(df)

    df = df[df["인사이트아이템"].isin(["AWP", "FL", "그외"])].copy()

    bases = make_bases(df)
    all_base = ensure_columns(bases["all_base"], required + ["인사이트아이템"])
    asset_base = ensure_columns(bases["asset_base"], required + ["인사이트아이템"])

    group_cols = ["인사이트아이템", "고객사", "BP번호", "현장", "배송지 주소"]

    count_df = (
        all_base.groupby(group_cols, dropna=False)
        .agg(
            대표영업팀=("영업팀", mode_text),
            임대대수=("코드", "size"),
            신규건수=("신규여부", lambda s: int((s == "신규").sum())),
            연장건수=("연장 여부", lambda s: int((s == "연장").sum())),
            연장대상=("연장대상구분", lambda s: int((s == "연장대상").sum())),
        )
        .reset_index()
    )

    rent_df = (
        asset_base.groupby(group_cols, dropna=False)
        .agg(
            렌탈료=("렌탈료", "sum"),
        )
        .reset_index()
    )

    out = count_df.merge(rent_df, on=group_cols, how="outer")
    for c in ["임대대수", "신규건수", "연장건수", "연장대상"]:
        out[c] = out[c].fillna(0).astype(int)
    out["렌탈료"] = out["렌탈료"].fillna(0.0)
    out["대표영업팀"] = out["대표영업팀"].fillna("")
    out["연장율"] = out.apply(lambda r: safe_divide(r["연장건수"], r["연장대상"]), axis=1)

    out = out.rename(columns={"인사이트아이템": "아이템분류"})
    return out.sort_values(["아이템분류", "렌탈료", "임대대수"], ascending=[True, False, False]).reset_index(drop=True)

def build_item_team_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    required = ["아이템", "영업팀", "코드", "연장 여부", "연장대상구분", "렌탈료"]
    df = ensure_columns(df, required)
    df = normalize_site_keys(df)

    df = df[df["인사이트아이템"].isin(["AWP", "FL", "그외"])].copy()

    bases = make_bases(df)
    all_base = ensure_columns(bases["all_base"], required + ["인사이트아이템"])
    asset_base = ensure_columns(bases["asset_base"], required + ["인사이트아이템"])

    count_df = (
        all_base.groupby(["인사이트아이템", "영업팀"], dropna=False)
        .agg(
            임대대수=("코드", "size"),
            연장건수=("연장 여부", lambda s: int((s == "연장").sum())),
            연장대상=("연장대상구분", lambda s: int((s == "연장대상").sum())),
        )
        .reset_index()
    )

    rent_df = (
        asset_base.groupby(["인사이트아이템", "영업팀"], dropna=False)
        .agg(
            렌탈료=("렌탈료", "sum"),
        )
        .reset_index()
    )

    out = count_df.merge(rent_df, on=["인사이트아이템", "영업팀"], how="outer")
    out["임대대수"] = out["임대대수"].fillna(0).astype(int)
    out["연장건수"] = out["연장건수"].fillna(0).astype(int)
    out["연장대상"] = out["연장대상"].fillna(0).astype(int)
    out["렌탈료"] = out["렌탈료"].fillna(0.0)
    out["연장율"] = out.apply(lambda r: safe_divide(r["연장건수"], r["연장대상"]), axis=1)

    out = out.rename(columns={"인사이트아이템": "아이템분류"})
    return out.sort_values(["아이템분류", "렌탈료"], ascending=[True, False]).reset_index(drop=True)

def build_item_site_insight_bundle(prev_df: pd.DataFrame, cur_df: pd.DataFrame, top_n: int = 6) -> Dict[str, Any]:
    prev = build_item_site_snapshot(prev_df).rename(columns={
        "대표영업팀": "전월 대표영업팀",
        "임대대수": "전월 임대대수",
        "신규건수": "전월 신규건수",
        "연장건수": "전월 연장건수",
        "연장대상": "전월 연장대상",
        "렌탈료": "전월 렌탈료",
        "연장율": "전월 연장율",
    })

    cur = build_item_site_snapshot(cur_df).rename(columns={
        "대표영업팀": "당월 대표영업팀",
        "임대대수": "당월 임대대수",
        "신규건수": "당월 신규건수",
        "연장건수": "당월 연장건수",
        "연장대상": "당월 연장대상",
        "렌탈료": "당월 렌탈료",
        "연장율": "당월 연장율",
    })

    merged = prev.merge(
        cur,
        on=["아이템분류", "고객사", "BP번호", "현장", "배송지 주소"],
        how="outer",
    )

    numeric_cols = [
        "전월 임대대수", "전월 신규건수", "전월 연장건수", "전월 연장대상", "전월 렌탈료",
        "당월 임대대수", "당월 신규건수", "당월 연장건수", "당월 연장대상", "당월 렌탈료",
    ]
    for c in numeric_cols:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    merged["전월 연장율"] = pd.to_numeric(merged["전월 연장율"], errors="coerce")
    merged["당월 연장율"] = pd.to_numeric(merged["당월 연장율"], errors="coerce")
    merged["전월 대표영업팀"] = merged["전월 대표영업팀"].fillna("")
    merged["당월 대표영업팀"] = merged["당월 대표영업팀"].fillna("")

    merged["증감 임대대수"] = merged["당월 임대대수"] - merged["전월 임대대수"]
    merged["증감 렌탈료"] = merged["당월 렌탈료"] - merged["전월 렌탈료"]
    merged["당월 대당렌탈료"] = merged.apply(lambda r: safe_divide(r["당월 렌탈료"], r["당월 임대대수"]), axis=1)
    merged["전월 대당렌탈료"] = merged.apply(lambda r: safe_divide(r["전월 렌탈료"], r["전월 임대대수"]), axis=1)

    prev_team = build_item_team_snapshot(prev_df).rename(columns={
        "임대대수": "전월 팀 임대대수",
        "연장건수": "전월 팀 연장건수",
        "연장대상": "전월 팀 연장대상",
        "렌탈료": "전월 팀 렌탈료",
        "연장율": "전월 팀 연장율",
    })
    cur_team = build_item_team_snapshot(cur_df).rename(columns={
        "임대대수": "당월 팀 임대대수",
        "연장건수": "당월 팀 연장건수",
        "연장대상": "당월 팀 연장대상",
        "렌탈료": "당월 팀 렌탈료",
        "연장율": "당월 팀 연장율",
    })

    team_merged = prev_team.merge(cur_team, on=["아이템분류", "영업팀"], how="outer")
    for c in [
        "전월 팀 임대대수", "전월 팀 연장건수", "전월 팀 연장대상", "전월 팀 렌탈료",
        "당월 팀 임대대수", "당월 팀 연장건수", "당월 팀 연장대상", "당월 팀 렌탈료"
    ]:
        team_merged[c] = pd.to_numeric(team_merged[c], errors="coerce").fillna(0)

    team_merged["증감 팀 임대대수"] = team_merged["당월 팀 임대대수"] - team_merged["전월 팀 임대대수"]
    team_merged["증감 팀 렌탈료"] = team_merged["당월 팀 렌탈료"] - team_merged["전월 팀 렌탈료"]

    def build_detail_snapshot(df: pd.DataFrame) -> pd.DataFrame:
        required = ["아이템", "아이템 세분화", "코드", "렌탈료"]
        x = ensure_columns(df, required)
        x = normalize_site_keys(x)

        if "아이템 세분화" not in x.columns:
            x["아이템 세분화"] = x["아이템"]
        x["아이템 세분화"] = x["아이템 세분화"].apply(normalize_text)
        x.loc[x["아이템 세분화"] == "", "아이템 세분화"] = x["아이템"]

        x = x[x["인사이트아이템"].isin(["AWP", "FL", "그외"])].copy()

        bases = make_bases(x)
        all_base = ensure_columns(bases["all_base"], required + ["인사이트아이템"])
        asset_base = ensure_columns(bases["asset_base"], required + ["인사이트아이템"])

        cnt = (
            all_base.groupby(["인사이트아이템", "아이템 세분화"], dropna=False)
            .agg(임대대수=("코드", "size"))
            .reset_index()
        )
        rent = (
            asset_base.groupby(["인사이트아이템", "아이템 세분화"], dropna=False)
            .agg(렌탈료=("렌탈료", "sum"))
            .reset_index()
        )

        out = cnt.merge(rent, on=["인사이트아이템", "아이템 세분화"], how="outer")
        out["임대대수"] = out["임대대수"].fillna(0).astype(int)
        out["렌탈료"] = out["렌탈료"].fillna(0.0)
        out = out.rename(columns={"인사이트아이템": "아이템분류"})
        return out

    prev_detail = build_detail_snapshot(prev_df).rename(columns={
        "임대대수": "전월 세부항목 임대대수",
        "렌탈료": "전월 세부항목 렌탈료",
    })
    cur_detail = build_detail_snapshot(cur_df).rename(columns={
        "임대대수": "당월 세부항목 임대대수",
        "렌탈료": "당월 세부항목 렌탈료",
    })

    detail_merged = prev_detail.merge(
        cur_detail,
        on=["아이템분류", "아이템 세분화"],
        how="outer",
    )
    for c in [
        "전월 세부항목 임대대수", "전월 세부항목 렌탈료",
        "당월 세부항목 임대대수", "당월 세부항목 렌탈료",
    ]:
        detail_merged[c] = pd.to_numeric(detail_merged[c], errors="coerce").fillna(0)

    detail_merged["증감 세부항목 임대대수"] = (
        detail_merged["당월 세부항목 임대대수"] - detail_merged["전월 세부항목 임대대수"]
    )
    detail_merged["증감 세부항목 렌탈료"] = (
        detail_merged["당월 세부항목 렌탈료"] - detail_merged["전월 세부항목 렌탈료"]
    )

    item_outputs = []
    for item, g in merged.groupby("아이템분류", dropna=False):
        g = g.copy()

        total_item_rent = float(g["당월 렌탈료"].sum())
        total_item_count = float(g["당월 임대대수"].sum())

        g["아이템 내 비중"] = np.where(
            total_item_rent > 0, g["당월 렌탈료"] / total_item_rent, np.nan
        )

        item_team = team_merged[team_merged["아이템분류"] == item].copy()
        total_team_rent = float(item_team["당월 팀 렌탈료"].sum()) if not item_team.empty else 0.0
        item_team["아이템 내 팀 비중"] = np.where(
            total_team_rent > 0, item_team["당월 팀 렌탈료"] / total_team_rent, np.nan
        )

        item_detail = detail_merged[detail_merged["아이템분류"] == item].copy()
        total_detail_rent = float(item_detail["당월 세부항목 렌탈료"].sum()) if not item_detail.empty else 0.0
        item_detail["세부항목 내 비중"] = np.where(
            total_detail_rent > 0, item_detail["당월 세부항목 렌탈료"] / total_detail_rent, np.nan
        )

        top_current_drivers = (
            g[g["당월 임대대수"] > 0]
            .sort_values(["당월 렌탈료", "당월 임대대수"], ascending=[False, False])
            .head(top_n)
            .to_dict(orient="records")
        )

        top_rent_up = (
            g[g["증감 렌탈료"] > 0]
            .sort_values(["증감 렌탈료", "증감 임대대수", "당월 렌탈료"], ascending=[False, False, False])
            .head(top_n)
            .to_dict(orient="records")
        )

        top_unit_rent = (
            g[(g["당월 임대대수"] > 0) & (g["당월 대당렌탈료"].notna())]
            .sort_values(["당월 대당렌탈료", "당월 렌탈료"], ascending=[False, False])
            .head(top_n)
            .to_dict(orient="records")
        )

        customer_rollup = (
            g.groupby(["고객사", "BP번호"], dropna=False)
            .agg(**{
                "현장수": ("현장", "nunique"),
                "당월 렌탈료": ("당월 렌탈료", "sum"),
                "전월 렌탈료": ("전월 렌탈료", "sum"),
                "증감 렌탈료": ("증감 렌탈료", "sum"),
                "당월 임대대수": ("당월 임대대수", "sum"),
            })
            .reset_index()
        )

        dominant_site = (
            g.sort_values(["고객사", "BP번호", "당월 렌탈료", "당월 임대대수"], ascending=[True, True, False, False])
            .drop_duplicates(["고객사", "BP번호"])
            [["고객사", "BP번호", "현장", "배송지 주소", "당월 렌탈료", "당월 임대대수", "당월 대표영업팀"]]
            .rename(columns={
                "현장": "대표현장",
                "배송지 주소": "대표현장 배송지 주소",
                "당월 렌탈료": "대표현장 렌탈료",
                "당월 임대대수": "대표현장 임대대수",
                "당월 대표영업팀": "대표현장 영업팀",
            })
        )

        customer_rollup = customer_rollup.merge(dominant_site, on=["고객사", "BP번호"], how="left")
        customer_rollup["대표현장 비중"] = np.where(
            customer_rollup["당월 렌탈료"] > 0,
            customer_rollup["대표현장 렌탈료"] / customer_rollup["당월 렌탈료"],
            np.nan,
        )

        top_customer_concentration = (
            customer_rollup.sort_values(
                ["당월 렌탈료", "현장수", "대표현장 비중"],
                ascending=[False, False, False]
            )
            .head(top_n)
            .to_dict(orient="records")
        )

        top_team_up = (
            item_team[item_team["증감 팀 렌탈료"] > 0]
            .sort_values(["증감 팀 렌탈료", "증감 팀 임대대수"], ascending=[False, False])
            .head(3)
            .to_dict(orient="records")
        )

        top_team_current = (
            item_team[item_team["당월 팀 렌탈료"] > 0]
            .sort_values(["당월 팀 렌탈료", "당월 팀 임대대수"], ascending=[False, False])
            .head(3)
            .to_dict(orient="records")
        )

        top_detail_current = (
            item_detail[item_detail["당월 세부항목 렌탈료"] > 0]
            .sort_values(["당월 세부항목 렌탈료", "당월 세부항목 임대대수"], ascending=[False, False])
            .head(top_n)
            .to_dict(orient="records")
        )

        top_detail_up = (
            item_detail[item_detail["증감 세부항목 렌탈료"] > 0]
            .sort_values(["증감 세부항목 렌탈료", "증감 세부항목 임대대수"], ascending=[False, False])
            .head(top_n)
            .to_dict(orient="records")
        )

        overview_stats = {
            "당월 아이템 렌탈료": total_item_rent,
            "당월 아이템 임대대수": total_item_count,
            "주요현장수": int((g["당월 임대대수"] > 0).sum()),
            "상위현장 비중": None if not top_current_drivers else top_current_drivers[0].get("아이템 내 비중"),
            "상위팀 비중": None if item_team.empty else item_team.sort_values("아이템 내 팀 비중", ascending=False).iloc[0]["아이템 내 팀 비중"],
            "대표세부항목": None if not top_detail_current else top_detail_current[0].get("아이템 세분화"),
            "대표세부항목 비중": None if not top_detail_current else top_detail_current[0].get("세부항목 내 비중"),
        }

        if not top_current_drivers and not top_rent_up and not top_customer_concentration and not top_detail_current:
            continue

        team_site_rollup = (
            g[g["당월 임대대수"] > 0]
            .groupby(["당월 대표영업팀", "고객사", "BP번호", "현장", "배송지 주소"], dropna=False)
            .agg(**{
                "당월 현장 렌탈료": ("당월 렌탈료", "sum"),
                "당월 현장 임대대수": ("당월 임대대수", "sum"),
            })
            .reset_index()
        )

        team_totals = (
            item_team.groupby("영업팀", dropna=False)
            .agg(**{
                "당월 팀 총렌탈료": ("당월 팀 렌탈료", "sum"),
                "당월 팀 총임대대수": ("당월 팀 임대대수", "sum"),
            })
            .reset_index()
            .rename(columns={"영업팀": "당월 대표영업팀"})
        )

        team_site_rollup = team_site_rollup.merge(
            team_totals,
            on="당월 대표영업팀",
            how="left",
        )

        team_site_rollup["팀내 현장 렌탈료 비중"] = np.where(
            team_site_rollup["당월 팀 총렌탈료"] > 0,
            team_site_rollup["당월 현장 렌탈료"] / team_site_rollup["당월 팀 총렌탈료"],
            np.nan,
        )

        team_site_rollup["팀내 현장 임대대수 비중"] = np.where(
            team_site_rollup["당월 팀 총임대대수"] > 0,
            team_site_rollup["당월 현장 임대대수"] / team_site_rollup["당월 팀 총임대대수"],
            np.nan,
        )

        # 너무 작은 현장은 제외: 팀내 의미 있는 기여만 남김
        representative_team_sites = team_site_rollup[
            (team_site_rollup["팀내 현장 렌탈료 비중"] >= 0.05) |
            (team_site_rollup["당월 현장 렌탈료"] >= 20_000_000) |
            (team_site_rollup["당월 현장 임대대수"] >= 10)
        ].copy()

        if representative_team_sites.empty:
            representative_team_sites = team_site_rollup.copy()

        top_team_site_contribution = (
            representative_team_sites.sort_values(
                ["팀내 현장 렌탈료 비중", "당월 현장 렌탈료", "당월 현장 임대대수", "당월 팀 총렌탈료"],
                ascending=[False, False, False, False]
            )
            .head(5)
            .to_dict(orient="records")
        )

        item_outputs.append({
            "item": item,
            "overview_stats": overview_stats,
            "top_current_drivers": top_current_drivers,
            "top_rent_up": top_rent_up,
            "top_unit_rent": top_unit_rent,
            "top_customer_concentration": top_customer_concentration,
            "top_team_up": top_team_up,
            "top_team_current": top_team_current,
            "top_detail_current": top_detail_current,
            "top_detail_up": top_detail_up,
            "top_team_site_contribution": top_team_site_contribution,
        })

    item_order = {"AWP": 0, "FL": 1, "그외": 2}
    item_outputs = sorted(item_outputs, key=lambda x: item_order.get(x["item"], 999))

    return {
        "item_site_insight_records": item_outputs
    }

def build_payload(monthly_dfs: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    months = sort_yymm(list(monthly_dfs.keys()))
    prev_yymm, cur_yymm = months[-2], months[-1]
    prev_df, cur_df = monthly_dfs[prev_yymm], monthly_dfs[cur_yymm]

    monthly_kpis = build_monthly_kpi_rows(monthly_dfs)
    prev_row = monthly_kpis[monthly_kpis["yymm"] == prev_yymm].iloc[0].to_dict()
    cur_row = monthly_kpis[monthly_kpis["yymm"] == cur_yymm].iloc[0].to_dict()

    item_bundle = build_item_site_insight_bundle(prev_df, cur_df, top_n=5)

    payload = {
        "months": months,
        "prev_month": prev_yymm,
        "cur_month": cur_yymm,
        "prev_month_label": month_label_from_yymm(prev_yymm),
        "cur_month_label": month_label_from_yymm(cur_yymm),
        "monthly_kpis": monthly_kpis.to_dict(orient="records"),
        "kpis": {
            "prev_임대대수": int(prev_row["임대대수"]),
            "cur_임대대수": int(cur_row["임대대수"]),
            "prev_렌탈료": float(prev_row["렌탈료"]),
            "cur_렌탈료": float(cur_row["렌탈료"]),
            "prev_연장건수": int(prev_row["연장건수"]),
            "cur_연장건수": int(cur_row["연장건수"]),
            "prev_연장대상": int(prev_row["연장대상"]),
            "cur_연장대상": int(cur_row["연장대상"]),
            "prev_연장율": None if pd.isna(prev_row["연장율"]) else float(prev_row["연장율"]),
            "cur_연장율": None if pd.isna(cur_row["연장율"]) else float(cur_row["연장율"]),
        },
        "top_items_cur": build_top_items(cur_df, top_n=10).to_dict(orient="records"),
        "top_teams_cur": build_top_teams(cur_df, top_n=10).to_dict(orient="records"),
        "item_site_insight_records": item_bundle["item_site_insight_records"],
    }
    return payload


# =========================================================
# Bedrock 요약
# =========================================================
def get_bedrock_client(region_name: str):
    if boto3 is None:
        raise ImportError("boto3가 설치되어 있지 않습니다.")
    if BEDROCK_API_KEY:
        os.environ["AWS_BEARER_TOKEN_BEDROCK"] = BEDROCK_API_KEY
    return boto3.client(
        "bedrock-runtime",
        region_name=region_name,
        config=Config(read_timeout=3600, retries={"max_attempts": 3, "mode": "standard"}),
    )


def _extract_json_text(text: str) -> dict:
    text = (text or "").strip()

    if not text:
        raise ValueError("LLM 응답이 비어 있습니다.")

    # 코드펜스 제거
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()

    # 1차: 그대로 시도
    try:
        return json.loads(text)
    except Exception:
        pass

    # 2차: 첫 JSON object 구간만 추출
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        candidate = text[start:end + 1].strip()
    else:
        raise ValueError(f"JSON object 구간을 찾지 못했습니다. 원문 일부: {text[:500]}")

    # 3차: 자주 나는 문제를 보정
    repaired = candidate

    # smart quote 정리
    repaired = repaired.replace("“", '"').replace("”", '"').replace("’", "'")

    # trailing comma 제거
    repaired = re.sub(r",\s*([}\]])", r"\1", repaired)

    # 불필요한 제어문자 제거
    repaired = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", " ", repaired)

    try:
        return json.loads(repaired)
    except Exception as e:
        raise ValueError(
            "LLM JSON 파싱 실패. 원문 일부:\n"
            f"{text[:1200]}\n\n"
            "정제 후 일부:\n"
            f"{repaired[:1200]}\n\n"
            f"원본 에러: {e}"
        )

def shrink_next_equipment_need_payload_from_block(
    block: Dict[str, Any],
    prev_month_label: str,
    cur_month_label: str,
) -> Dict[str, Any]:
    return {
        "prev_month_label": prev_month_label,
        "cur_month_label": cur_month_label,
        "item": block.get("item"),
        "overview_stats": block.get("overview_stats", {}),
        "top_current_drivers": decorate_prompt_records(block.get("top_current_drivers", [])[:5]),
        "top_customer_concentration": decorate_prompt_records(block.get("top_customer_concentration", [])[:5]),
        "top_team_current": decorate_prompt_records(block.get("top_team_current", [])[:3]),
        "top_team_up": decorate_prompt_records(block.get("top_team_up", [])[:3]),
        "top_team_site_contribution": decorate_prompt_records(block.get("top_team_site_contribution", [])[:5]),
        "top_unit_rent": decorate_prompt_records(block.get("top_unit_rent", [])[:5]),
        "top_detail_current": decorate_prompt_records(block.get("top_detail_current", [])[:5]),
    }

def build_fallback_next_equipment_need_section(payload: Dict[str, Any]) -> Dict[str, Any] | None:
    awp_block = next(
        (b for b in payload.get("item_site_insight_records", []) if normalize_text(b.get("item")) == "AWP"),
        None
    )
    if not awp_block:
        return None

    rows: List[Dict[str, Any]] = []
    seen = set()

    for r in awp_block.get("top_team_site_contribution", []):
        customer = normalize_text(r.get("고객사"))
        site = normalize_text(r.get("현장"))
        key = (customer, site)
        if not customer or not site or key in seen:
            continue
        seen.add(key)

        rows.append(
            {
                "우선순위": len(rows) + 1,
                "고객사": customer,
                "현장": site,
                "현재 사용 장비": "AWP",
                "다음 제안 장비": "AWP 추가 투입",
                "검토 시점": "고소작업 확장 구간에서 추가 장비 검토 우선순위가 높음",
                "근거": (
                    f"{site}는 팀내 현장 렌탈료 비중 {pct_fmt(r.get('팀내 현장 렌탈료 비중'), 1)}, "
                    f"당월 현장 렌탈료 {won(r.get('당월 현장 렌탈료'))}, "
                    f"당월 현장 임대대수 {count_unit(r.get('당월 현장 임대대수'))}로 확인됨."
                ),
            }
        )
        if len(rows) >= 5:
            break

    if not rows:
        for r in awp_block.get("top_current_drivers", []):
            customer = normalize_text(r.get("고객사"))
            site = normalize_text(r.get("현장"))
            key = (customer, site)
            if not customer or not site or key in seen:
                continue
            seen.add(key)

            rows.append(
                {
                    "우선순위": len(rows) + 1,
                    "고객사": customer,
                    "현장": site,
                    "현재 사용 장비": "AWP",
                    "다음 제안 장비": "AWP 추가 투입",
                    "검토 시점": "현재 장비 운영 확대 구간에서 추가 장비 검토 우선순위가 높음",
                    "근거": (
                        f"{site}는 당월 렌탈료 {won(r.get('당월 렌탈료'))}, "
                        f"당월 임대대수 {count_unit(r.get('당월 임대대수'))}, "
                        f"아이템 내 비중 {pct_fmt(r.get('아이템 내 비중'), 1)}로 확인됨."
                    ),
                }
            )
            if len(rows) >= 5:
                break

    if not rows:
        return None

    return {
        "section_title": "현장별 다음 장비 수요 후보",
        "summary_lines": [
            "현재 AWP 운영 규모와 현장 기여도를 기준으로 추가 장비 검토 후보를 정리함.",
            "확정 수요가 아니라 운영 강도와 현장 대표성을 기준으로 본 다음 제안 후보임.",
        ],
        "opportunity_table": rows,
    }

def shrink_payload_for_summary_prompt(payload: Dict[str, Any]) -> Dict[str, Any]:
    slim = {
        "prev_month_label": payload["prev_month_label"],
        "cur_month_label": payload["cur_month_label"],
        "kpis": payload["kpis"],
        "item_overview": [],
    }

    for block in payload.get("item_site_insight_records", []):
        slim["item_overview"].append({
            "item": block.get("item"),
            "overview_stats": block.get("overview_stats", {}),
            "top_current_driver": (block.get("top_current_drivers", [])[:1] or []),
            "top_team_current": (block.get("top_team_current", [])[:1] or []),
            "top_detail_current": (block.get("top_detail_current", [])[:2] or []),
        })
    return slim


def shrink_item_block_for_prompt(
    block: Dict[str, Any],
    prev_month_label: str,
    cur_month_label: str,
) -> Dict[str, Any]:
    return {
        "prev_month_label": prev_month_label,
        "cur_month_label": cur_month_label,
        "item": block.get("item"),
        "overview_stats": block.get("overview_stats", {}),
        "top_current_drivers": decorate_prompt_records(block.get("top_current_drivers", [])[:4]),
        "top_rent_up": decorate_prompt_records(block.get("top_rent_up", [])[:3]),
        "top_unit_rent": decorate_prompt_records(block.get("top_unit_rent", [])[:3]),
        "top_customer_concentration": decorate_prompt_records(block.get("top_customer_concentration", [])[:3]),
        "top_team_current": decorate_prompt_records(block.get("top_team_current", [])[:3]),
        "top_team_up": decorate_prompt_records(block.get("top_team_up", [])[:3]),
        "top_detail_current": decorate_prompt_records(block.get("top_detail_current", [])[:4]),
        "top_detail_up": decorate_prompt_records(block.get("top_detail_up", [])[:3]),
        "top_team_site_contribution": decorate_prompt_records(block.get("top_team_site_contribution", [])[:3]),
    }

def build_summary_prompt(payload: Dict[str, Any]) -> str:
    return f"""
당신은 nova-pro-v1:0 모델에서 동작하는 산업장비 렌탈 월간 보고서 상단 요약 작성자다.

역할:
- 경영진이 한눈에 이해할 수 있는 짧고 선명한 월간 요약을 작성한다.
- 반드시 payload 안의 수치와 레코드만 사용한다.
- payload에 없는 사실, 숫자, 원인, 해석은 만들지 않는다.
- 최종 출력은 JSON 객체 하나만 반환한다.
- 설명, 코드펜스, 마크다운, 주석, 머리말, 꼬리말은 금지한다.

핵심 목표:
1. 숫자 기반으로 명확하게 쓴다.
2. 문장은 전부 긍정적이거나 중립적으로 쓴다.
3. 감소, 하락, 부진, 정체, 약세, 위험, 편중 우려 같은 부정적 표현은 쓰지 않는다.
4. 특히 팀 단위에 대해 부정적으로 해석하는 문장은 절대 쓰지 않는다.
5. 창의적 해석은 가능하지만 반드시 payload 안의 수치 구조를 근거로 해야 한다.

출력 JSON 스키마:
{{
  "headline": "문자열",
  "subheadline": "문자열",
  "summary_lines": ["문자열", "문자열", "문자열"],
  "numeric_observations": ["문자열", "문자열", "문자열"],
  "composition_notes": ["문자열", "문자열"],
  "footnotes": ["문자열"]
}}

반드시 지킬 규칙:
- JSON 객체 하나만 반환
- summary_lines는 정확히 3개
- numeric_observations는 정확히 3개
- composition_notes는 정확히 2개
- footnotes는 정확히 1개
- item_insight_sections는 만들지 말 것
- 모든 문장은 짧고 명확하게 작성
- 추상어만 있는 문장 금지
- 가능한 한 문장마다 숫자, 대상, 비교축 중 2개 이상 포함
- 숫자는 payload 값을 그대로 사용하되 의미를 왜곡하지 말 것
- 부정형 서술 금지
- 대신 긍정형/중립형으로 표현:
  "핵심 비중을 형성", "주요 축으로 확인", "구성 중심에 위치", "실적 기반을 형성"

출력 전 자체 점검:
- JSON 외 다른 텍스트가 없는가?
- 문장 수가 정확한가?
- 부정적 표현이 들어가 있지 않은가?
- 모든 문장이 payload 근거 안에 있는가?

payload:
{json.dumps(payload, ensure_ascii=False, separators=(",", ":"))}
""".strip()

def build_item_prompt(item_payload: Dict[str, Any]) -> str:
    return f"""
당신은 nova-pro-v1:0 모델에서 동작하는 산업장비 렌탈 월간 보고서의 아이템별 상세 인사이트 작성자다.

역할:
- 이 payload는 단 하나의 아이템(AWP / FL / 그외)만 포함한다.
- 반드시 payload 안의 레코드만 사용한다.
- 없는 고객사, 현장, 주소, 팀, 숫자, 원인, 일정은 만들지 않는다.
- 최종 출력은 JSON 객체 하나만 반환한다.
- 설명, 코드펜스, 마크다운, 주석, 머리말, 꼬리말은 금지한다.

최우선 원칙:
1. 모든 문장은 긍정적 또는 중립적으로 작성한다.
2. 부정적 문장 작성 금지.
3. 특히 팀 단위에 대해 부정적으로 해석하거나 평가하는 문장은 절대 금지한다.
4. 창의성은 허용하지만 상상으로 쓰지 말고, payload 안의 수치 구조에서 사람들이 바로 못 보는 패턴을 꺼내라.
5. 단순 증감 나열이 아니라 "팀-현장-고객사-아이템 구조"를 보여줘라.
6. 같은 유형의 문장을 반복하지 말고, 각 문장이 다른 관찰 포인트를 가져야 한다.

출력 JSON 스키마:
{{
  "item": "AWP 또는 FL 또는 그외",
  "quant_lines": ["문자열", "문자열", "문자열"],
  "creative_lines": ["문자열", "문자열", "문자열"]
}}

공통 규칙:
- JSON 객체 하나만 반환
- quant_lines는 정확히 3개
- creative_lines는 정확히 3개
- 모든 문장은 짧고 또렷하게 작성
- payload 안의 *_fmt 필드가 있으면 반드시 그 값을 우선 사용
- 비중 raw 값(예: 0.03, 0.0048)을 그대로 쓰지 말고 반드시 퍼센트 형식으로 작성
- 렌탈료는 반드시 "원", 임대대수는 반드시 "대", 비중은 반드시 "%"를 붙여라
- "3500000", "0.03", "1" 같은 raw 숫자 표기 금지
- 감소, 하락, 이탈, 부진, 약세, 저조, 위험, 편중, 의존 리스크 같은 표현 금지
- 대신 "핵심 축", "대표 현장", "복수 현장 기반", "구성 중심", "실적 기반", "밀도", "결", "주요 기여" 같은 표현을 사용한다

매우 중요:
- 모든 아이템에서 반드시 "성과가 좋은 팀"과 "그 팀 성과를 받쳐주는 현장"을 1회 이상 명시해야 한다.
- 이 조건은 필수다.
- payload.top_team_current와 payload.top_team_site_contribution를 최우선 사용한다.
- 최소 1개 문장은 아래 3가지를 모두 포함해야 한다:
  1) 영업팀명
  2) 고객사 또는 현장명
  3) 팀내 현장 렌탈료 비중(%) 또는 팀내 현장 임대대수 비중(%)

아이템별 세부 규칙:
- item == "그외":
  - 반드시 payload.top_detail_current와 payload.top_detail_up를 적극적으로 사용한다.
  - 최소 2개 문장에서 반드시 "아이템 세분화"명을 직접 명시해야 한다.
  - "그외"라고만 쓰지 말고 실제 세부 항목명(예: COMP, 핸드파렛트 트럭 등)을 써야 한다.
  - 어떤 세부 항목이 렌탈료가 큰지, 어떤 세부 항목이 임대대수가 큰지, 어떤 세부 항목이 구조의 중심인지 드러내라.
  - 단순히 "그외가 많다"가 아니라 "그외 안에서 어떤 세부 항목이 실적의 결을 만들었는지"를 써라.

- item == "AWP":
  - AWP는 아이템 세분화 관점을 사용하지 말라.
  - payload.top_detail_current, payload.top_detail_up를 근거 문장에 사용하지 말라.
  - AWP는 현장, 고객사, 팀, 대당 렌탈료, 대표 현장 집중, 복수 현장 구조 중심으로만 해석하라.
  - AWP에 대해 세부 항목명이나 세분화 구조를 억지로 만들거나 암시하지 말라.

- item == "FL":
  - FL은 아이템 세분화 관점을 사용할 수 있다.
  - 특히 "FL"과 "FL 물류"를 구분해서 보면 더 의미가 잘 드러나므로, payload.top_detail_current / top_detail_up에 해당 값이 있으면 이를 우선 반영하라.
  - 가능하면 최소 1개 문장에서 "FL"과 "FL 물류" 중 어떤 구성이 중심인지 직접 밝혀라.
  - FL 공사용 성격과 FL 물류 성격이 구분되어 보이면, 어떤 세부 구성이 현장 구조와 더 맞물리는지 중립적으로 설명하라.
  - 단, payload 안에 없는 세분화명은 만들지 말라.

quant_lines 작성 규칙:
- 정확히 3개
- 관찰 중심 문장
- 고객사/현장/팀/대수/렌탈료 중 최소 3개 이상 포함
- 최소 1개 문장은 "성과 좋은 팀 + 그 팀의 대표 현장" 구조를 반드시 설명
- 최소 1개 문장은 팀 성과가 어떤 현장 때문에 커 보이는지를 설명
- payload.top_team_site_contribution를 가장 우선 사용
- 대표성이 높은 현장을 우선 선택
- 같은 패턴 반복 금지

creative_lines 작성 규칙:
- 정확히 3개
- 단순 증가 설명 금지
- 숫자 없는 추상 문장 금지
- 표면 숫자를 넘어서 구조를 읽어야 한다
- 아래 패턴을 우선 탐색:
  1) 성과 좋은 팀이 복수 현장을 기반으로 실적을 넓게 형성하는 패턴
  2) 성과 좋은 팀이 특정 대표 현장을 중심축으로 실적을 선명하게 형성하는 패턴
  3) 고객사-현장-팀 연결이 또렷한 패턴
  4) 대당 렌탈료 밀도가 높은 현장 패턴
  5) 세부항목이 아이템 내 결을 만드는 패턴
  6) 겉으로는 총액만 보이지만 실제로는 특정 구조가 실적 결을 만드는 패턴

creative_lines 추가 규칙:
- item == "그외" 이면 최소 2개 문장에서 실제 아이템 세분화명을 명시해야 한다.
- item == "FL" 이면 가능하면 "FL"과 "FL 물류" 중 어떤 세부 구성이 더 중심인지 1개 이상 문장에 반영하라.
- item == "AWP" 이면 세분화 언급 금지, 현장/고객사/팀 구조로만 해석하라.

문장 스타일 규칙:
- 한 문장에 너무 많은 사실을 넣지 말고 핵심이 선명하게 보이게 작성
- "~로 보인다", "~일 수 있다" 같은 약한 추측 표현보다 payload 근거를 바로 말하는 서술형을 우선 사용
- 같은 패턴 반복 금지

출력 전 자체 점검:
- JSON 외 다른 텍스트가 없는가?
- quant_lines 3개, creative_lines 3개가 맞는가?
- 모든 아이템에서 팀-현장 연결 문장이 최소 1개 이상 있는가?
- item == "그외"일 때 실제 아이템 세분화명이 최소 2회 이상 들어갔는가?
- item == "AWP"일 때 아이템 세분화 언급이 없는가?
- item == "FL"일 때 가능하면 "FL" / "FL 물류" 구분이 반영되었는가?
- 팀 단위 부정 문장이 없는가?
- 모든 문장이 payload 근거를 갖는가?
- 퍼센트/원/대 표기가 올바른가?

payload:
{json.dumps(item_payload, ensure_ascii=False, separators=(",", ":"))}
""".strip()

def build_opportunity_prompt(opportunity_payload: Dict[str, Any]) -> str:
    return f"""
당신은 nova-pro-v1:0 모델에서 동작하는 산업장비 렌탈 현장별 다음 장비 수요 예측 작성자다.

역할:
- 현재 우리 장비를 사용 중인 현장들 중에서,
  앞으로 어떤 장비 수요가 다음으로 열릴 가능성이 있는지 정리한다.
- 핵심은 "현재 사용 장비"를 기준으로 현장별 다음 장비 수요와 검토 시점을 추정하는 것이다.
- 반드시 payload 안의 실제 현장, 실제 사용 장비, 실제 수치만 사용한다.
- 없는 현장, 고객사, 장비, 일정, 공정, 숫자, 사실은 만들지 않는다.
- 최종 출력은 JSON 객체 하나만 반환한다.
- 설명, 코드펜스, 마크다운, 주석, 머리말, 꼬리말은 금지한다.

매우 중요:
- 이 결과는 확정 수요가 아니라 "다음 장비 제안 후보"다.
- 부정적 표현 금지
- 과장 금지
- 미래를 단정하지 말고, "다음 수요 가능성"과 "검토 시점" 중심으로 작성한다.
- 절대 날짜를 만들지 말 것
- "언제 필요"는 달력 날짜가 아니라 아래와 같은 운영/작업 단계 표현으로만 작성:
  - 현재 장비 운영 확대 구간
  - 장비 증설 검토 구간
  - 반입/상하차 집중 구간
  - 설치 작업 본격화 구간
  - 고소작업 확장 구간
  - 마감 전 정리 구간
  - 유지보수/후속 작업 구간

출력 JSON 스키마:
{{
  "section_title": "문자열",
  "summary_lines": ["문자열", "문자열"],
  "opportunity_table": [
    {{
      "우선순위": 1,
      "고객사": "문자열",
      "현장": "문자열",
      "현재 사용 장비": "문자열",
      "다음 제안 장비": "문자열",
      "검토 시점": "작업/운영 단계 기반 표현",
      "근거": "문자열"
    }}
  ]
}}

반드시 지킬 규칙:
- JSON 객체 하나만 반환
- summary_lines는 정확히 2개
- opportunity_table은 최대 5개 행
- payload 후보가 5개 미만이면 있는 만큼만 작성
- 모든 후보는 payload 안의 실제 현장만 사용
- 각 row에는 반드시 현재 사용 장비와 다음 제안 장비를 모두 적는다
- 각 row의 근거에는 반드시 현장명 + 현재 수치 정보 또는 비중 정보가 들어가야 한다
- "다음 제안 장비"는 payload 안의 현재 장비 흐름과 현장 규모를 보고 자연스럽게 연결되는 장비만 작성한다
- 부정적 해석 금지
- 숫자 없는 막연한 추천 금지
- 같은 현장을 중복해서 여러 행으로 쓰지 말 것

판단 기준:
- 현재 사용 장비의 렌탈료 또는 임대대수가 큰 현장
- 팀 실적을 받쳐주는 대표 현장
- 고객사 내 대표 현장 비중이 높은 현장
- 다현장 구조에서 중심축 역할을 하는 현장
- 대당 렌탈료 밀도가 높은 현장
- 현재 장비 사용 강도가 높아 다음 장비 수요 연결 가능성이 커 보이는 현장

"다음 제안 장비" 작성 방식:
- 현재 사용 장비와 다른 장비를 제안해도 되고,
  현재 장비의 증설/추가 투입 형태로 써도 된다.
- 예:
  - "지게차"
  - "COMP"
  - "AWP 추가 투입"
  - "지게차 + AWP"
  - "COMP + 지게차"
- 단, payload 근거 없이 과도하게 조합하지 말 것

"검토 시점" 작성 방식:
- 정확한 월/날짜 금지
- 현재 사용 장비의 성격과 현장 규모를 보고 다음 수요가 열릴 법한 운영 단계로 작성
- 예:
  - "현재 장비 운영 확대 구간에서 검토 우선순위가 높음"
  - "설치 작업 본격화 구간에서 연결 제안에 적합"
  - "반입/상하차 집중 구간에서 함께 제안하기 좋음"
  - "고소작업 확장 구간에서 추가 장비 제안 여지가 큼"

좋은 근거 문장 예시:
- "OO현장은 현재 AWP 렌탈료 000원, 임대대수 00대로 운영 규모가 커 다음 장비 제안 우선순위가 높음."
- "OO현장은 팀내 현장 렌탈료 비중 00.0%로 대표성이 높아 후속 장비 연결 제안에 적합함."
- "OO현장은 고객사 내 대표 현장 비중이 00.0%로 확인되어 다음 장비 수요 검토의 중심 후보로 볼 수 있음."

출력 전 자체 점검:
- JSON 외 다른 텍스트가 없는가?
- opportunity_table이 최대 5개인가?
- 모든 행이 실제 payload 현장인가?
- 절대 날짜를 쓰지 않았는가?
- 현재 사용 장비와 다음 제안 장비가 모두 들어갔는가?
- 부정적 문장이 없는가?
- 근거가 숫자 또는 비중과 연결되어 있는가?

payload:
{json.dumps(opportunity_payload, ensure_ascii=False, separators=(",", ":"))}
""".strip()


def invoke_json_prompt(
    prompt: str,
    model_id: str,
    region_name: str,
    *,
    max_tokens: int,
    repair_max_tokens: int,
    repair_rule: str = "",
) -> Dict[str, Any]:
    client = get_bedrock_client(region_name)

    last_error = None
    raw_text = ""

    response = client.converse(
        modelId=model_id,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": max_tokens, "temperature": 0.05},
    )
    raw_text = response["output"]["message"]["content"][0]["text"]

    try:
        return _extract_json_text(raw_text)
    except Exception as e:
        last_error = e
        print(f"[WARN] JSON 파싱 실패 (primary): {e}")

    repair_prompt = f"""
아래 응답은 JSON이 중간에서 끊겼거나 형식이 깨졌다.
동일한 의미를 유지하되 더 짧고 간결한 문장으로 줄여서,
유효한 JSON 객체 하나만 다시 작성하라.

규칙:
- 설명 금지
- 코드펜스 금지
- 주석 금지
- 반드시 JSON.parse 가능한 순수 JSON만 반환
{repair_rule}

원문:
{raw_text}
""".strip()

    try:
        response = client.converse(
            modelId=model_id,
            messages=[{"role": "user", "content": [{"text": repair_prompt}]}],
            inferenceConfig={"maxTokens": repair_max_tokens, "temperature": 0.0},
        )
        raw_text = response["output"]["message"]["content"][0]["text"]
        return _extract_json_text(raw_text)
    except Exception as e2:
        last_error = e2
        print(f"[WARN] JSON 재정렬 실패: {e2}")

    raise ValueError(f"최종 JSON 파싱 실패: {last_error}")

# =========================================================
# 차트 생성
# =========================================================
def setup_plot_style():
    sns.set_theme(style="whitegrid", context="notebook")

    candidates = [
        "Malgun Gothic",
        "AppleGothic",
        "NanumGothic",
        "Noto Sans CJK KR",
        "DejaVu Sans",
    ]
    installed = {f.name for f in fm.fontManager.ttflist}
    for name in candidates:
        if name in installed:
            plt.rcParams["font.family"] = name
            break

    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["axes.facecolor"] = "#fbfdff"
    plt.rcParams["figure.facecolor"] = "#ffffff"
    plt.rcParams["axes.edgecolor"] = "#d9e2f2"
    plt.rcParams["grid.color"] = "#dbe7f5"
    plt.rcParams["grid.alpha"] = 0.85

def recent_month_window(df: pd.DataFrame, n: int = 3, valid_col: str | None = None) -> pd.DataFrame:
    out = df.copy()
    if valid_col is not None:
        out = out[out[valid_col].notna()].copy()
    return out.tail(n).reset_index(drop=True)


def set_zoom_ylim(ax, values, pad_ratio: float = 0.12, min_pad: float = 0.0):
    s = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    if s.empty:
        return

    vmin = float(s.min())
    vmax = float(s.max())

    if math.isclose(vmin, vmax):
        pad = max(abs(vmax) * pad_ratio, min_pad, 1e-9)
    else:
        pad = max((vmax - vmin) * pad_ratio, min_pad)

    ax.set_ylim(vmin - pad, vmax + pad)

def fig_to_base64() -> str:
    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png", dpi=180, bbox_inches="tight")
    plt.close()
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def make_monthly_count_chart_base64(monthly_kpis: pd.DataFrame) -> str:
    setup_plot_style()
    df = recent_month_window(monthly_kpis, n=3)

    plt.figure(figsize=(8.2, 3.8))
    ax = sns.lineplot(
        data=df,
        x="월",
        y="임대대수",
        marker="o",
        linewidth=2.5,
        color=sns.color_palette("Blues", 6)[4],
    )
    ax.set_title("최근 3개월 임대대수", loc="left", fontsize=13, fontweight="bold")
    ax.set_xlabel("")
    ax.set_ylabel("")
    set_zoom_ylim(ax, df["임대대수"], pad_ratio=0.18, min_pad=40)
    annotate_line_values(ax, lambda v: count_unit(v))

    return fig_to_base64()

def make_monthly_rent_chart_base64(monthly_kpis: pd.DataFrame) -> str:
    setup_plot_style()
    df = recent_month_window(monthly_kpis, n=3)

    plt.figure(figsize=(8.2, 3.8))
    ax = sns.lineplot(
        data=df,
        x="월",
        y="렌탈료",
        marker="o",
        linewidth=2.5,
        color=sns.color_palette("Blues", 6)[4],
    )
    ax.set_title("최근 3개월 렌탈료", loc="left", fontsize=13, fontweight="bold")
    ax.set_xlabel("")
    ax.set_ylabel("")
    set_zoom_ylim(ax, df["렌탈료"], pad_ratio=0.20, min_pad=2_000_000)
    annotate_line_values(ax, lambda v: won_thousand(v))

    return fig_to_base64()

def make_monthly_extension_chart_base64(monthly_kpis: pd.DataFrame) -> str:
    setup_plot_style()
    df = recent_month_window(monthly_kpis, n=3, valid_col="연장율")

    plt.figure(figsize=(8.2, 3.8))
    ax = sns.lineplot(
        data=df,
        x="월",
        y="연장율",
        marker="o",
        linewidth=2.5,
        color=sns.color_palette("Blues", 6)[4],
    )
    ax.set_title("최근 3개월 연장율", loc="left", fontsize=13, fontweight="bold")
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=1))
    set_zoom_ylim(ax, df["연장율"], pad_ratio=0.20, min_pad=0.005)
    annotate_line_values(ax, lambda v: pct_fmt(v, 1))

    return fig_to_base64()

def make_top_items_chart_base64(top_items: pd.DataFrame) -> str:
    setup_plot_style()
    df = top_items.head(7).iloc[::-1].copy()

    plt.figure(figsize=(8.4, 4.8))
    ax = sns.barplot(
        data=df,
        x="렌탈료",
        y="아이템",
        hue="아이템",
        dodge=False,
        palette="Blues",
        legend=False,
    )
    ax.set_title("아이템별 렌탈료 집계", loc="left", fontsize=13, fontweight="bold")
    ax.set_xlabel("")
    ax.set_ylabel("")
    annotate_barh_values(ax, lambda v: won_thousand(v))
    return fig_to_base64()

def make_top_teams_chart_base64(top_teams: pd.DataFrame) -> str:
    setup_plot_style()
    df = top_teams.head(7).iloc[::-1].copy()

    plt.figure(figsize=(8.4, 4.8))
    ax = sns.barplot(
        data=df,
        x="렌탈료",
        y="영업팀",
        hue="영업팀",
        dodge=False,
        palette="Blues",
        legend=False,
    )
    ax.set_title("영업팀별 렌탈료 집계", loc="left", fontsize=13, fontweight="bold")
    ax.set_xlabel("")
    ax.set_ylabel("")
    annotate_barh_values(ax, lambda v: won_thousand(v))
    return fig_to_base64()

# =========================================================
# PDF 렌더링
# =========================================================
def render_pdf_weasy(
    report_json: Dict[str, Any],
    payload: Dict[str, Any],
    top_items: pd.DataFrame,
    top_teams: pd.DataFrame,
    out_pdf: str,
    dll_dir: str = WEASYPRINT_DLL_DIR,
):
    if dll_dir:
        os.environ["WEASYPRINT_DLL_DIRECTORIES"] = dll_dir
    from weasyprint import HTML

    monthly_kpis = pd.DataFrame(payload["monthly_kpis"])

    count_chart = make_monthly_count_chart_base64(monthly_kpis)
    rent_chart = make_monthly_rent_chart_base64(monthly_kpis)
    ext_chart = make_monthly_extension_chart_base64(monthly_kpis)
    items_chart = make_top_items_chart_base64(top_items)
    teams_chart = make_top_teams_chart_base64(top_teams)

    k = payload["kpis"]

    def esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    def li_block(items: List[str]) -> str:
        return "".join(f"<li>{esc(x)}</li>" for x in items)

    def kpi_card(title: str, value: str, sub: str) -> str:
        return f"""
        <div class="metric">
          <div class="metric-label">{esc(title)}</div>
          <div class="metric-value">{esc(value)}</div>
          <div class="metric-sub">{esc(sub)}</div>
        </div>
        """

    monthly_rows_html = "".join(
        f"<tr><td>{esc(r['월'])}</td><td class='num'>{count_fmt(r['임대대수'])}</td><td class='num'>{won(r['렌탈료'])}</td><td class='num'>{count_fmt(r['연장건수'])}</td><td class='num'>{count_fmt(r['연장대상'])}</td><td class='num'>{pct_fmt(r['연장율'])}</td></tr>"
        for _, r in monthly_kpis.iterrows()
    )

    item_rows_html = "".join(
        f"<tr><td>{esc(r['아이템'])}</td><td class='num'>{count_fmt(r['임대대수'])}</td><td class='num'>{won(r['렌탈료'])}</td><td class='num'>{count_fmt(r['연장건수'])}</td><td class='num'>{pct_fmt(r['연장율'])}</td><td class='num'>{pct_fmt(r['회수율'])}</td></tr>"
        for _, r in top_items.iterrows()
    )

    team_rows_html = "".join(
        f"<tr><td>{esc(r['영업팀'])}</td><td class='num'>{count_fmt(r['임대대수'])}</td><td class='num'>{won(r['렌탈료'])}</td><td class='num'>{count_fmt(r['신규건수'])}</td><td class='num'>{count_fmt(r['연장건수'])}</td><td class='num'>{pct_fmt(r['연장율'])}</td></tr>"
        for _, r in top_teams.iterrows()
    )

    item_sections_html = ""
    for sec in report_json.get("item_insight_sections", []):
        item = esc(sec.get("item", "기타"))
        lines = sec.get("lines", [])
        quant_lines = lines[:3]
        creative_lines = lines[3:6]

        quant_html = "".join(f"<li>{esc(x)}</li>" for x in quant_lines)
        creative_html = "".join(f"<li>{esc(x)}</li>" for x in creative_lines)

        item_sections_html += f"""
        <div class="section">
          <div class="section-title">{item} 실적 상세</div>
          <div class="card">
            <div style="font-weight:700; margin-bottom:6px;">수치 기반 인사이트</div>
            <ul class="clean">{quant_html}</ul>
            <div style="font-weight:700; margin:10px 0 6px 0;">구조적 인사이트</div>
            <ul class="clean">{creative_html}</ul>
          </div>
        </div>
        """

    next_need_section = (
        report_json.get("site_next_equipment_need_section")
        or report_json.get("site_opportunity_section")
    )

    next_need_section_html = ""
    if isinstance(next_need_section, dict):
        next_need_title = esc(
            next_need_section.get("section_title", "현장별 다음 장비 수요 후보")
        )
        next_need_summary = next_need_section.get("summary_lines", [])
        next_need_rows = next_need_section.get("opportunity_table", [])

        next_need_rows_html = "".join(
            f"""
            <tr>
              <td class='num'>{esc(r.get('우선순위', ''))}</td>
              <td>{esc(r.get('고객사', ''))}</td>
              <td>{esc(r.get('현장', ''))}</td>
              <td>{esc(r.get('현재 사용 장비', ''))}</td>
              <td>{esc(r.get('다음 제안 장비', r.get('검토 장비', '')))}</td>
              <td>{esc(r.get('검토 시점', ''))}</td>
              <td>{esc(r.get('근거', ''))}</td>
            </tr>
            """
            for r in next_need_rows
        )

        next_need_section_html = f"""
        <div class="section">
          <div class="section-title">{next_need_title}</div>
          <div class="card">
            <ul class="clean">{li_block(next_need_summary)}</ul>
          </div>
          <div class="card">
            <table>
              <thead>
                <tr>
                  <th class="num">우선순위</th>
                  <th>고객사</th>
                  <th>현장</th>
                  <th>현재 사용 장비</th>
                  <th>다음 제안 장비</th>
                  <th>검토 시점</th>
                  <th>근거</th>
                </tr>
              </thead>
              <tbody>{next_need_rows_html}</tbody>
            </table>
          </div>
        </div>
        """

    html = f"""
    <html>
    <head>
    <meta charset="utf-8">
    <style>
      @page {{
        size: A4;
        margin: 12mm 12mm 14mm 12mm;
      }}
      :root {{
        --bg: #f6f7fb;
        --paper: #ffffff;
        --ink: #1f2937;
        --muted: #6b7280;
        --line: #e5e7eb;
        --accent: #1f4b99;
        --accent-soft: #eaf0ff;
        --radius: 12px;
      }}
      body {{
        font-family: "Pretendard", "Noto Sans KR", "Malgun Gothic", Arial, sans-serif;
        color: var(--ink);
        font-size: 10px;
        line-height: 1.55;
        background: var(--bg);
      }}
      .page {{ background: var(--paper); }}
      .hero {{
        background: linear-gradient(135deg, #f7faff 0%, #eef4ff 100%);
        border: 1px solid #dbe5ff;
        border-radius: 16px;
        padding: 16px 18px 14px 18px;
        margin-bottom: 12px;
      }}
      .eyebrow {{
        font-size: 8.5px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: var(--accent);
        font-weight: 700;
        margin-bottom: 5px;
      }}
      h1 {{
        font-size: 22px;
        line-height: 1.2;
        margin: 0 0 4px 0;
        font-weight: 800;
      }}
      .subheadline {{
        color: var(--muted);
        font-size: 10px;
        margin: 0;
      }}
      .section {{
        margin-top: 12px;
        page-break-inside: avoid;
        break-inside: avoid;
      }}
      .section-title {{
        font-size: 11px;
        font-weight: 800;
        margin: 0 0 7px 0;
        color: #111827;
        padding-left: 8px;
        border-left: 3px solid var(--accent);
      }}
      .card {{
        background: var(--paper);
        border: 1px solid var(--line);
        border-radius: var(--radius);
        padding: 10px 11px;
        margin-bottom: 10px;
      }}
      .metrics {{
        display: flex;
        gap: 10px;
        margin-top: 10px;
      }}
      .metric {{
        flex: 1;
        background: var(--paper);
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 11px 12px;
      }}
      .metric-label {{
        color: var(--muted);
        font-size: 8.8px;
        margin-bottom: 5px;
      }}
      .metric-value {{
        font-size: 18px;
        font-weight: 800;
        line-height: 1.1;
        margin-bottom: 4px;
      }}
      .metric-sub {{
        font-size: 8.8px;
        color: var(--muted);
      }}
      .cols-2 {{
        display: flex;
        gap: 10px;
      }}
      .col {{ flex: 1; }}
      .chart-box {{
        background: #fcfcfd;
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 8px;
      }}
      .chart {{
        width: 100%;
        height: auto;
        display: block;
      }}
      ul.clean {{
        margin: 0;
        padding-left: 16px;
      }}
      ul.clean li {{
        margin: 0 0 5px 0;
      }}
      table {{
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        font-size: 9px;
        overflow: hidden;
        border: 1px solid var(--line);
        border-radius: 10px;
      }}
      thead th {{
        background: #f8fafc;
        color: #374151;
        font-weight: 700;
        border-bottom: 1px solid var(--line);
        padding: 7px 8px;
        text-align: left;
      }}
      tbody td {{
        padding: 6px 8px;
        border-bottom: 1px solid #eef2f7;
        vertical-align: top;
      }}
      tbody tr:nth-child(even) {{ background: #fbfcfe; }}
      tbody tr:last-child td {{ border-bottom: none; }}
      td.num, th.num {{
        text-align: right;
        white-space: nowrap;
      }}
      .footnote {{
        font-size: 8.4px;
        color: var(--muted);
      }}
      .chip-row {{ margin-top: 8px; }}
      .chip {{
        display: inline-block;
        padding: 4px 8px;
        margin-right: 6px;
        border-radius: 999px;
        background: var(--accent-soft);
        color: var(--accent);
        font-size: 8px;
        font-weight: 700;
      }}
    </style>
    </head>
    <body>
    <div class="page">
      <div class="hero">
        <div class="eyebrow">MONTHLY RENTAL DASHBOARD</div>
        <h1>{esc(report_json.get('headline', '산업장비 렌탈 월간 수치 보고서'))}</h1>
        <p class="subheadline">{esc(report_json.get('subheadline', '월별 수치와 현장 인사이트를 기준으로 정리한 내부 보고서'))}</p>

        <div class="chip-row">
          <span class="chip">{esc(payload['prev_month_label'])}</span>
          <span class="chip">{esc(payload['cur_month_label'])}</span>
        </div>
      </div>

      <div class="metrics">
        {kpi_card('임대대수', count_fmt(k['cur_임대대수']) + '대',
                  f"{payload['prev_month_label']} {count_fmt(k['prev_임대대수'])}대 / {payload['cur_month_label']} {count_fmt(k['cur_임대대수'])}대")}
        {kpi_card('렌탈료', won(k['cur_렌탈료']),
                  f"{payload['prev_month_label']} {won(k['prev_렌탈료'])} / {payload['cur_month_label']} {won(k['cur_렌탈료'])}")}
        {kpi_card('연장율', pct_fmt(k['cur_연장율']),
                  f"{payload['prev_month_label']} {pct_fmt(k['prev_연장율'])} / {payload['cur_month_label']} {pct_fmt(k['cur_연장율'])}")}
      </div>

      <div class="section">
        <div class="section-title">요약 문장</div>
        <div class="card">
          <ul class="clean">{li_block(report_json.get('summary_lines', []))}</ul>
        </div>
      </div>

      <div class="section">
        <div class="section-title">월별 추이</div>
        <div class="cols-2">
          <div class="col card"><div class="chart-box"><img class="chart" src="data:image/png;base64,{count_chart}"></div></div>
          <div class="col card"><div class="chart-box"><img class="chart" src="data:image/png;base64,{rent_chart}"></div></div>
        </div>
        <div class="card">
          <div class="chart-box"><img class="chart" src="data:image/png;base64,{ext_chart}"></div>
        </div>
      </div>

      <div class="section">
        <div class="section-title">수치 관찰</div>
        <div class="cols-2">
          <div class="col card">
            <ul class="clean">{li_block(report_json.get('numeric_observations', []))}</ul>
          </div>
          <div class="col card">
            <ul class="clean">{li_block(report_json.get('composition_notes', []))}</ul>
          </div>
        </div>
      </div>

      {item_sections_html}
      {next_need_section_html}

      <div class="section">
        <div class="section-title">품목 및 영업팀 집계</div>
        <div class="cols-2">
          <div class="col card"><div class="chart-box"><img class="chart" src="data:image/png;base64,{items_chart}"></div></div>
          <div class="col card"><div class="chart-box"><img class="chart" src="data:image/png;base64,{teams_chart}"></div></div>
        </div>
      </div>

      <div class="section">
        <div class="section-title">월별 집계 표</div>
        <div class="card">
          <table>
            <thead>
              <tr><th>월</th><th class="num">임대대수</th><th class="num">렌탈료</th><th class="num">연장건수</th><th class="num">연장대상</th><th class="num">연장율</th></tr>
            </thead>
            <tbody>{monthly_rows_html}</tbody>
          </table>
        </div>
      </div>

      <div class="section">
        <div class="section-title">품목별 집계 표</div>
        <div class="card">
          <table>
            <thead>
              <tr><th>품목</th><th class="num">임대대수</th><th class="num">렌탈료</th><th class="num">연장건수</th><th class="num">연장율</th><th class="num">회수율</th></tr>
            </thead>
            <tbody>{item_rows_html}</tbody>
          </table>
        </div>
      </div>

      <div class="section">
        <div class="section-title">영업팀별 집계 표</div>
        <div class="card">
          <table>
            <thead>
              <tr><th>영업팀</th><th class="num">임대대수</th><th class="num">렌탈료</th><th class="num">신규건수</th><th class="num">연장건수</th><th class="num">연장율</th></tr>
            </thead>
            <tbody>{team_rows_html}</tbody>
          </table>
        </div>
      </div>

      <div class="section">
        <div class="section-title">주석</div>
        <div class="card footnote">
          <ul class="clean">{li_block(report_json.get('footnotes', []))}</ul>
        </div>
      </div>
    </div>
    </body>
    </html>
    """
    HTML(string=html).write_pdf(out_pdf)

def _is_blank(v):
    return v is None or (isinstance(v, str) and v.strip() == "") or pd.isna(v)


def fmt_num(v, digits=2, use_comma=True):
    if _is_blank(v):
        return "-"
    if isinstance(v, str):
        return v

    x = float(v)
    if math.isinf(x) or math.isnan(x):
        return "-"

    s = f"{x:,.{digits}f}" if use_comma else f"{x:.{digits}f}"
    s = s.rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s


def fmt_int(v):
    if _is_blank(v):
        return "-"
    if isinstance(v, str):
        return v
    return f"{int(round(float(v))):,}"


def fmt_currency(v, suffix="원"):
    if _is_blank(v):
        return "-"
    if isinstance(v, str):
        return v
    x = float(v)
    s = f"{x:,.2f}".rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return f"{s}{suffix}"


def fmt_pct(v, multiply_if_ratio=True):
    if _is_blank(v):
        return "-"
    if isinstance(v, str):
        return v

    x = float(v)
    if multiply_if_ratio and abs(x) <= 1:
        x *= 100

    s = f"{x:,.2f}".rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return f"{s}%"


_DECIMAL_PATTERN = re.compile(r"(?<![\d])(-?\d[\d,]*\.\d+)(?!\d)")


def _round_decimal_text(s: str) -> str:
    def repl(match):
        raw = match.group(1)
        use_comma = "," in raw
        try:
            num = float(raw.replace(",", ""))
        except Exception:
            return raw
        return fmt_num(num, digits=2, use_comma=use_comma)

    return _DECIMAL_PATTERN.sub(repl, s)


def round_decimal_strings(obj):
    if isinstance(obj, dict):
        return {k: round_decimal_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [round_decimal_strings(v) for v in obj]
    if isinstance(obj, str):
        return _round_decimal_text(obj)
    return obj

def build_fallback_summary_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    k = payload["kpis"]
    prev_label = payload["prev_month_label"]
    cur_label = payload["cur_month_label"]

    item_overview = payload.get("item_site_insight_records", [])
    item_names = [x.get("item", "") for x in item_overview]

    return {
        "headline": "산업장비 렌탈 월간 수치 보고서",
        "subheadline": f"{cur_label} 주요 인사이트",
        "summary_lines": [
            f"임대대수는 {prev_label} {count_fmt(k['prev_임대대수'])}대, {cur_label} {count_fmt(k['cur_임대대수'])}대로 집계됨.",
            f"렌탈료는 {prev_label} {won(k['prev_렌탈료'])}, {cur_label} {won(k['cur_렌탈료'])}로 집계됨.",
            f"주요 인사이트 대상 아이템은 {', '.join([x for x in item_names if x])} 기준으로 정리함.",
        ],
        "numeric_observations": [
            f"연장건수는 {prev_label} {count_fmt(k['prev_연장건수'])}건, {cur_label} {count_fmt(k['cur_연장건수'])}건.",
            f"연장대상은 {prev_label} {count_fmt(k['prev_연장대상'])}건, {cur_label} {count_fmt(k['cur_연장대상'])}건.",
            f"연장율은 {prev_label} {pct_fmt(k['prev_연장율'])}, {cur_label} {pct_fmt(k['cur_연장율'])}로 정리됨.",
        ],
        "composition_notes": [
            "월별 추이는 최근 3개월 기준으로 표시함.",
            "아이템별 상세는 고객사, 현장, 팀, 세부항목 구조를 기준으로 작성함.",
        ],
        "footnotes": [
            "기준 데이터는 output Excel의 정제RAW 시트를 사용함."
        ],
    }

def build_fallback_item_section(block: Dict[str, Any], cur_label: str) -> Dict[str, Any]:
    item = normalize_text(block.get("item", "")) or "기타"
    quant_lines: List[str] = []
    creative_lines: List[str] = []

    if block.get("top_team_site_contribution"):
        ts = block["top_team_site_contribution"][0]
        creative_lines.append(
            f"{item}에서 {normalize_text(ts.get('당월 대표영업팀'))} 팀은 "
            f"{cur_label} 렌탈료 {won(ts.get('당월 팀 총렌탈료'))}를 기록했고, "
            f"그중 {normalize_text(ts.get('고객사'))} / {normalize_text(ts.get('현장'))} "
            f"({normalize_text(ts.get('배송지 주소'))})가 팀 렌탈료의 "
            f"{pct_fmt(ts.get('팀내 현장 렌탈료 비중'), 1)}를 차지함."
        )

    if block.get("top_current_drivers"):
        r = block["top_current_drivers"][0]
        quant_lines.append(
            f"{item}에서 {normalize_text(r.get('고객사'))} / {normalize_text(r.get('현장'))} "
            f"({normalize_text(r.get('배송지 주소'))})는 {cur_label} 렌탈료 {won(r.get('당월 렌탈료'))}, "
            f"임대대수 {count_fmt(r.get('당월 임대대수'))}대로 집계됨."
        )

    if block.get("top_rent_up"):
        r = block["top_rent_up"][0]
        quant_lines.append(
            f"{item}에서 {normalize_text(r.get('고객사'))} / {normalize_text(r.get('현장'))}는 "
            f"렌탈료 {won(r.get('전월 렌탈료'))} → {won(r.get('당월 렌탈료'))}, "
            f"임대대수 {count_fmt(r.get('전월 임대대수'))}대 → {count_fmt(r.get('당월 임대대수'))}대로 확대됨."
        )

    if block.get("top_team_current"):
        t = block["top_team_current"][0]
        quant_lines.append(
            f"{item}는 {normalize_text(t.get('영업팀'))} 팀에서 {cur_label} 렌탈료 {won(t.get('당월 팀 렌탈료'))}, "
            f"임대대수 {count_fmt(t.get('당월 팀 임대대수'))}대로 반영됨."
        )

    if block.get("top_customer_concentration"):
        c = block["top_customer_concentration"][0]
        creative_lines.append(
            f"{item}에서 {normalize_text(c.get('고객사'))} (BP {normalize_text(c.get('BP번호'))})는 "
            f"현장수 {count_fmt(c.get('현장수'))}개 구조이며, 대표현장 {normalize_text(c.get('대표현장'))} 비중이 "
            f"{pct_fmt(c.get('대표현장 비중'), 1)}로 집중됨."
        )

    if block.get("top_unit_rent"):
        u = block["top_unit_rent"][0]
        creative_lines.append(
            f"{item}에서 {normalize_text(u.get('고객사'))} / {normalize_text(u.get('현장'))}는 "
            f"{cur_label} 대당 렌탈료 {won(u.get('당월 대당렌탈료'))} 수준으로, 소수 대수 대비 매출 밀도가 높은 현장임."
        )

    if item == "그외" and block.get("top_detail_current"):
        d = block["top_detail_current"][0]
        creative_lines.append(
            f"그외에서는 아이템 세분화 {normalize_text(d.get('아이템 세분화'))}가 "
            f"{cur_label} 렌탈료 {won(d.get('당월 세부항목 렌탈료'))}, "
            f"임대대수 {count_fmt(d.get('당월 세부항목 임대대수'))}대로 핵심 비중을 형성함."
        )

    while len(quant_lines) < 3:
        quant_lines.append(f"{item} 수치형 인사이트는 payload 레코드 기준으로 정리함.")
    while len(creative_lines) < 3:
        creative_lines.append(f"{item} 구조형 인사이트는 payload 레코드 기준으로 정리함.")

    return {
        "item": item,
        "quant_lines": quant_lines[:3],
        "creative_lines": creative_lines[:3],
    }

def build_fallback_report_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    cur_label = payload["cur_month_label"]

    summary_json = normalize_summary_json(
        raw={},
        payload=payload,
        source="fallback",
    )

    item_sections = []
    used_fallback_items: List[str] = []

    for block in payload.get("item_site_insight_records", []):
        item_name = normalize_text(block.get("item")) or "기타"
        used_fallback_items.append(item_name)

        item_sections.append(
            normalize_item_section_json(
                raw={},
                block=block,
                cur_label=cur_label,
                source="fallback",
            )
        )

    summary_json["item_insight_sections"] = item_sections
    summary_json["site_next_equipment_need_section"] = build_fallback_next_equipment_need_section(payload)
    summary_json["_meta"] = {
        "used_fallback_items": used_fallback_items,
        "used_fallback_count": len(used_fallback_items),
        "summary_source": "fallback",
    }
    return summary_json

def generate_report_json_split(
    payload: Dict[str, Any],
    model_id: str,
    region_name: str,
) -> Dict[str, Any]:
    prev_label = payload["prev_month_label"]
    cur_label = payload["cur_month_label"]

    # 1) 전체 요약
    summary_payload = shrink_payload_for_summary_prompt(payload)
    summary_prompt = build_summary_prompt(summary_payload)

    try:
        raw_summary_json = invoke_json_prompt(
            summary_prompt,
            model_id=model_id,
            region_name=region_name,
            max_tokens=1800,
            repair_max_tokens=1400,
            repair_rule="""
- summary_lines는 정확히 3개 유지
- numeric_observations는 정확히 3개 유지
- composition_notes는 정확히 2개 유지
- footnotes는 정확히 1개 유지
""".strip(),
        )
        summary_json = normalize_summary_json(raw_summary_json, payload, source="llm")
        summary_source = "llm"
    except Exception as e:
        print(f"[WARN] summary 생성 실패, fallback 사용: {e}")
        summary_json = normalize_summary_json({}, payload, source="fallback")
        summary_source = "fallback"

    # 2) 아이템별 상세
    item_sections = []
    used_fallback_items: List[str] = []

    for block in payload.get("item_site_insight_records", []):
        item_name = normalize_text(block.get("item")) or "기타"
        item_payload = shrink_item_block_for_prompt(block, prev_label, cur_label)
        item_prompt = build_item_prompt(item_payload)

        try:
            raw_item_json = invoke_json_prompt(
                item_prompt,
                model_id=model_id,
                region_name=region_name,
                max_tokens=2200,
                repair_max_tokens=1800,
                repair_rule="""
- quant_lines는 정확히 3개 유지
- creative_lines는 정확히 3개 유지
- 모든 아이템에서 팀-현장 연결 문장을 최소 1개 포함
- 문장을 더 짧게 줄여도 좋지만 숫자와 고유명사는 유지
- item == "그외" 이면 creative_lines 중 최소 2개 문장에서 아이템 세분화를 명시
""".strip(),
            )

            item_sections.append(
                normalize_item_section_json(
                    raw=raw_item_json,
                    block=block,
                    cur_label=cur_label,
                    source="llm",
                )
            )

        except Exception as e:
            print(f"[WARN] {item_name} item section 생성 실패, fallback 사용: {e}")
            used_fallback_items.append(item_name)

            item_sections.append(
                normalize_item_section_json(
                    raw={},
                    block=block,
                    cur_label=cur_label,
                    source="fallback",
                )
            )

    # 3) 현장별 다음 장비 수요 후보
    site_next_equipment_need_section = None

    target_block = next(
        (b for b in payload.get("item_site_insight_records", [])
         if normalize_text(b.get("item")) == "AWP"),
        None
    )

    if target_block:
        next_need_payload = shrink_next_equipment_need_payload_from_block(
            target_block,
            prev_label,
            cur_label,
        )
        next_need_prompt = build_opportunity_prompt(next_need_payload)

        try:
            site_next_equipment_need_section = invoke_json_prompt(
                next_need_prompt,
                model_id=model_id,
                region_name=region_name,
                max_tokens=2200,
                repair_max_tokens=1800,
                repair_rule="""
- summary_lines는 정확히 2개 유지
- opportunity_table은 최대 5개 행 유지
- 절대 날짜를 만들지 말고 작업 단계 표현만 사용
- 모든 행은 payload 안의 실제 현장만 사용
""".strip(),
            )
        except Exception as e:
            print(f"[WARN] next equipment need section 생성 실패, fallback 사용: {e}")
            site_next_equipment_need_section = build_fallback_next_equipment_need_section(payload)

    summary_json["item_insight_sections"] = item_sections
    summary_json["site_next_equipment_need_section"] = site_next_equipment_need_section
    summary_json["_meta"] = {
        "used_fallback_items": used_fallback_items,
        "used_fallback_count": len(used_fallback_items),
        "summary_source": summary_source,
    }
    return summary_json

# =========================================================
# 메인
# =========================================================
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-excel", required=True, help="기존 코드가 생성한 output Excel 경로")
    parser.add_argument("--output-pdf", required=True, help="생성할 PDF 경로")
    parser.add_argument("--aws-region", default=AWS_REGION)
    parser.add_argument("--bedrock-model-id", default=BEDROCK_MODEL_ID)
    parser.add_argument("--bedrock-api-key", default=BEDROCK_API_KEY)
    parser.add_argument("--weasyprint-dll-dir", default=WEASYPRINT_DLL_DIR)
    return parser.parse_args()

def main():
    global AWS_REGION, BEDROCK_MODEL_ID, BEDROCK_API_KEY, WEASYPRINT_DLL_DIR
    args = parse_args()
    AWS_REGION = args.aws_region
    BEDROCK_MODEL_ID = args.bedrock_model_id
    BEDROCK_API_KEY = args.bedrock_api_key
    WEASYPRINT_DLL_DIR = args.weasyprint_dll_dir

    if WEASYPRINT_DLL_DIR:
        os.environ["WEASYPRINT_DLL_DIRECTORIES"] = WEASYPRINT_DLL_DIR
    if BEDROCK_API_KEY:
        os.environ["AWS_BEARER_TOKEN_BEDROCK"] = BEDROCK_API_KEY

    setup_matplotlib_font()

    print_title("Step 1. output Excel 로드")
    months = discover_processed_months(args.output_excel)
    print(f"발견된 정제RAW 월: {months}")

    monthly_dfs = {m: load_processed_df(args.output_excel, m) for m in months}

    print_title("Step 2. payload 생성")
    payload = build_payload(monthly_dfs)
    print(json.dumps({
        "prev_month": payload["prev_month"],
        "cur_month": payload["cur_month"],
        "top_items_cur_count": len(payload["top_items_cur"]),
        "top_teams_cur_count": len(payload["top_teams_cur"]),
    }, ensure_ascii=False, indent=2))

    print_title("Step 3. API 요약 생성")
    try:
        if not BEDROCK_MODEL_ID:
            raise ValueError("--bedrock-model-id 또는 BEDROCK_MODEL_ID 환경변수가 필요합니다.")

        report_json = generate_report_json_split(
            payload,
            model_id=BEDROCK_MODEL_ID,
            region_name=AWS_REGION,
        )

        summary_source = report_json.get("_meta", {}).get("summary_source", "unknown")
        used_fallback_items = report_json.get("_meta", {}).get("used_fallback_items", [])

        print(f"API 요약 생성 성공 (summary_source={summary_source})")
        if used_fallback_items:
            print(f"[INFO] item-level fallback 사용: {used_fallback_items}")

    except Exception as e:
        print(f"API 요약 실패, fallback 사용: {e}")
        report_json = build_fallback_report_json(payload)

    report_json = round_decimal_strings(report_json)

    print_title("Step 4. 차트/표 기반 PDF 생성")
    top_items = pd.DataFrame(payload["top_items_cur"])
    top_teams = pd.DataFrame(payload["top_teams_cur"])
    render_pdf_weasy(
        report_json=report_json,
        payload=payload,
        top_items=top_items,
        top_teams=top_teams,
        out_pdf=args.output_pdf,
        dll_dir=WEASYPRINT_DLL_DIR,
    )
    print(f"PDF 저장 완료: {args.output_pdf}")

    print_title("최종 결과")
    print(json.dumps({
        "output_excel": args.output_excel,
        "output_pdf": args.output_pdf,
        "months": months,
        "prev_month": payload["prev_month"],
        "cur_month": payload["cur_month"],
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print_title("실행 실패")
        print(str(e))
        traceback.print_exc()
        raise
