#!/usr/bin/env python3
"""
미국 의회 주식 거래 데이터 수집 스크립트
- House Stock Watcher S3에서 하원 거래 데이터 fetch
- Senate Stock Watcher에서 상원 거래 데이터 fetch
- 정당/위원회 매핑 추가
- 이해충돌(💎) 판별
- JSON 출력
"""

import json
import os
import sys
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ═══════════════════════════════════════════════
# DATA SOURCES
# ═══════════════════════════════════════════════
HOUSE_URL = "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"
SENATE_URL = "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json"

# ═══════════════════════════════════════════════
# PARTY MAPPING (주요 정치인)
# 실제 운영 시 https://theunitedstates.io/congress-legislators/ 에서
# 전체 의원 목록을 가져와 매핑하는 것을 권장
# ═══════════════════════════════════════════════
PARTY_MAP = {
    # Democrats
    "Nancy Pelosi": "D", "Hon. Nancy Pelosi": "D",
    "Josh Gottheimer": "D", "Ro Khanna": "D",
    "Daniel Goldman": "D", "Daniel S. Goldman": "D",
    "Debbie Wasserman Schultz": "D",
    "Lois Frankel": "D", "Suzan DelBene": "D",
    "Hakeem Jeffries": "D", "Adam Schiff": "D",
    "Sheldon Whitehouse": "D", "Mark Kelly": "D",
    "Gary Peters": "D", "Jon Ossoff": "D",
    "Mark Warner": "D", "Jacky Rosen": "D",
    # Republicans
    "Dan Crenshaw": "R", "Tommy Tuberville": "R",
    "Mark Green": "R", "Marjorie Taylor Greene": "R",
    "Michael McCaul": "R", "David Rouzer": "R",
    "Rick Scott": "R", "Kevin Hern": "R",
    "French Hill": "R", "John Curtis": "R",
    "Tim Scott": "R", "Bill Hagerty": "R",
    "Markwayne Mullin": "R", "Ted Cruz": "R",
}

# ═══════════════════════════════════════════════
# COMMITTEE / JURISDICTION DATA
# ═══════════════════════════════════════════════
POLITICIAN_INFO = {
    "Nancy Pelosi": {
        "name_ko": "낸시 펠로시",
        "committees": ["전 하원의장"],
        "subcommittees": [],
        "jurisdiction": ["입법 전반", "예산", "국방", "기술정책"],
        "sectors": ["테크", "반도체", "소프트웨어", "방산"],
        "note": "남편 Paul Pelosi 명의 거래로 주목. 기술주 매수 타이밍이 정책 발표와 근접해 논란"
    },
    "Dan Crenshaw": {
        "name_ko": "댄 크렌쇼",
        "committees": ["하원 에너지·상업위원회", "하원 정보위원회"],
        "subcommittees": [],
        "jurisdiction": ["에너지", "통신", "사이버보안", "정보기관 감독"],
        "sectors": ["소프트웨어", "에너지", "방산"],
        "note": "정보위원회 소속으로 방산·사이버 관련 기업 투자 주목"
    },
    "Tommy Tuberville": {
        "name_ko": "토미 터버빌",
        "committees": ["상원 군사위원회", "상원 농업위원회"],
        "subcommittees": [],
        "jurisdiction": ["국방예산", "군사계약", "농업정책"],
        "sectors": ["방산", "반도체"],
        "note": "상원 군사위 소속이면서 방산주 대량 매수로 윤리 조사 대상"
    },
    "Mark Green": {
        "name_ko": "마크 그린",
        "committees": ["하원 국토안보위원회 (위원장)", "하원 군사위원회"],
        "subcommittees": [],
        "jurisdiction": ["국토안보", "군사계약", "사이버보안", "방위산업"],
        "sectors": ["방산"],
        "note": "국토안보위 위원장으로서 방산 기업 직접 관할하면서 해당 종목 매수"
    },
    "Josh Gottheimer": {
        "name_ko": "조시 고트하이머",
        "committees": ["하원 금융서비스위원회"],
        "subcommittees": [],
        "jurisdiction": ["은행규제", "핀테크", "디지털자산", "금융시장"],
        "sectors": ["테크", "금융"],
        "note": "빅테크 규제 논의 중 기술주 매수"
    },
    "Marjorie Taylor Greene": {
        "name_ko": "마조리 테일러 그린",
        "committees": ["하원 국토안보위원회", "하원 감독·개혁위원회"],
        "subcommittees": [],
        "jurisdiction": ["국토안보", "정부 운영", "감독"],
        "sectors": ["전기차", "미디어"],
        "note": "DJT(트럼프미디어) 매수는 정치적 충성도 표현으로 해석"
    },
    "Ro Khanna": {
        "name_ko": "로 칸나",
        "committees": ["하원 군사위원회", "하원 감독·개혁위원회"],
        "subcommittees": [],
        "jurisdiction": ["국방기술", "정부 효율", "실리콘밸리 기술"],
        "sectors": ["테크", "소프트웨어"],
        "note": "실리콘밸리 지역구로 빅테크 본사 밀집, 기술주 투자 활발"
    },
    "Michael McCaul": {
        "name_ko": "마이클 맥콜",
        "committees": ["하원 외교위원회 (위원장)"],
        "subcommittees": [],
        "jurisdiction": ["외교정책", "대중국 규제", "반도체 수출통제", "군사원조"],
        "sectors": ["반도체", "소프트웨어"],
        "note": "CHIPS Act 등 반도체 정책 주도하면서 NVDA, AVGO 대량 매수로 논란"
    },
    "Daniel Goldman": {
        "name_ko": "다니엘 골드만",
        "committees": ["하원 국토안보위원회", "하원 감독·개혁위원회"],
        "subcommittees": [],
        "jurisdiction": ["국토안보", "정부감독", "기업규제"],
        "sectors": ["테크", "금융"],
        "note": "뉴욕 금융가 지역구, 금융·테크 기업 투자 활발"
    },
    "Debbie Wasserman Schultz": {
        "name_ko": "데비 워서먼 슐츠",
        "committees": ["하원 세출위원회"],
        "subcommittees": ["환경·제조·핵심광물 소위원회"],
        "jurisdiction": ["환경정책", "제조업", "핵심광물", "광업규제"],
        "sectors": ["광업", "에너지"],
        "note": "핵심광물 소위 소속이면서 Hecla Mining(HL) 매수 — 광업 규제 직접 관할"
    },
    "Rick Scott": {
        "name_ko": "릭 스콧",
        "committees": ["상원 상업·과학·교통위원회", "상원 국토안보위원회"],
        "subcommittees": [],
        "jurisdiction": ["에너지정책", "교통", "상업", "국토안보"],
        "sectors": ["에너지"],
        "note": "에너지 정책 관련 위원회 소속으로 석유 대기업 투자"
    },
    "Lois Frankel": {
        "name_ko": "로이스 프랭클",
        "committees": ["하원 세출위원회"],
        "subcommittees": [],
        "jurisdiction": ["예산배분", "보건예산", "국방예산"],
        "sectors": ["헬스케어"],
        "note": "세출위 소속으로 보건 예산에 영향력, 헬스케어 종목 거래"
    },
}

# ═══════════════════════════════════════════════
# TICKER → SECTOR MAPPING
# ═══════════════════════════════════════════════
SECTOR_MAP = {
    # 반도체
    "NVDA": "반도체", "AMD": "반도체", "AVGO": "반도체", "INTC": "반도체",
    "QCOM": "반도체", "TSM": "반도체", "MRVL": "반도체", "MU": "반도체",
    # 테크
    "AAPL": "테크", "GOOGL": "테크", "GOOG": "테크", "META": "테크",
    "AMZN": "테크", "NFLX": "테크",
    # 소프트웨어
    "MSFT": "소프트웨어", "CRM": "소프트웨어", "PLTR": "소프트웨어",
    "SNOW": "소프트웨어", "NOW": "소프트웨어", "ORCL": "소프트웨어",
    # 방산
    "RTX": "방산", "LMT": "방산", "GD": "방산", "NOC": "방산",
    "BA": "방산", "HII": "방산", "LHX": "방산",
    # 전기차
    "TSLA": "전기차", "RIVN": "전기차", "LCID": "전기차",
    # 금융
    "JPM": "금융", "BAC": "금융", "V": "금융", "MA": "금융",
    "GS": "금융", "MS": "금융", "BRK.B": "금융",
    # 에너지
    "XOM": "에너지", "CVX": "에너지", "COP": "에너지", "SLB": "에너지",
    # 헬스케어
    "UNH": "헬스케어", "JNJ": "헬스케어", "PFE": "헬스케어",
    "LLY": "헬스케어", "ABBV": "헬스케어", "MRK": "헬스케어",
    # 광업
    "HL": "광업", "NEM": "광업", "FCX": "광업", "GOLD": "광업",
    # 미디어
    "DJT": "미디어", "DIS": "미디어", "CMCSA": "미디어",
}

SECTOR_JURISDICTION_MAP = {
    "반도체": "반도체 수출통제·CHIPS Act",
    "테크": "빅테크 규제·독점금지",
    "소프트웨어": "사이버보안·기술정책",
    "방산": "국방예산·군사계약",
    "전기차": "친환경 정책·EV 보조금",
    "미디어": "통신·미디어 규제",
    "금융": "은행규제·핀테크",
    "에너지": "에너지 정책·화석연료",
    "헬스케어": "보건예산·의약품 규제",
    "광업": "광물 규제·환경정책",
}


def fetch_json(url, label=""):
    """URL에서 JSON 데이터 가져오기"""
    print(f"  📥 {label}: {url}")
    try:
        req = Request(url, headers={"User-Agent": "CongressTracker/1.0"})
        with urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(f"  ✅ {label}: {len(data)} 건 로드")
            return data
    except (URLError, HTTPError) as e:
        print(f"  ❌ {label} 실패: {e}")
        return []


def get_party(name):
    """정치인 이름으로 정당 조회"""
    if name in PARTY_MAP:
        return PARTY_MAP[name]
    # 이름 변형 시도
    clean = name.replace("Hon. ", "").replace("Rep. ", "").replace("Sen. ", "").strip()
    if clean in PARTY_MAP:
        return PARTY_MAP[clean]
    return None


def get_sector(ticker):
    """티커로 섹터 조회"""
    if not ticker or ticker == "--":
        return None
    return SECTOR_MAP.get(ticker.upper().strip(), "기타")


def check_conflict(rep_name, sector):
    """이해충돌 여부 판별"""
    if not sector or sector == "기타":
        return False
    # 이름 정규화
    clean = rep_name.replace("Hon. ", "").replace("Rep. ", "").replace("Sen. ", "").strip()
    info = POLITICIAN_INFO.get(clean)
    if not info:
        return False
    return sector in info.get("sectors", [])


def get_amount_mid(amount_str):
    """금액 범위 문자열 → 중간값"""
    amount_map = {
        "$1,001 - $15,000": 8000,
        "$15,001 - $50,000": 32500,
        "$50,001 - $100,000": 75000,
        "$100,001 - $250,000": 175000,
        "$250,001 - $500,000": 375000,
        "$500,001 - $1,000,000": 750000,
        "$1,000,001 - $5,000,000": 3000000,
        "$5,000,001 - $25,000,000": 15000000,
        "$25,000,001 - $50,000,000": 37500000,
        "$50,000,000 +": 50000000,
    }
    if not amount_str:
        return 0
    return amount_map.get(amount_str.strip(), 0)


def process_house_data(raw_data):
    """하원 데이터 가공"""
    trades = []
    for item in raw_data:
        ticker = item.get("ticker", "").strip()
        if not ticker or ticker == "--" or ticker == "N/A":
            continue

        rep = item.get("representative", "").strip()
        if not rep:
            continue

        tx_type = item.get("type", "").strip().lower()
        is_buy = "purchase" in tx_type
        is_sell = "sale" in tx_type
        if not is_buy and not is_sell:
            continue

        tx_date = item.get("transaction_date", "")
        if not tx_date or tx_date == "--":
            tx_date = item.get("disclosure_date", "")

        party = get_party(rep)
        sector = get_sector(ticker)
        conflict = check_conflict(rep, sector)

        trade = {
            "rep": rep,
            "party": party,
            "ticker": ticker.upper(),
            "asset": item.get("asset_description", "")[:60],
            "type": "buy" if is_buy else "sell",
            "amount": item.get("amount", ""),
            "amount_mid": get_amount_mid(item.get("amount", "")),
            "date": tx_date,
            "disclosure_date": item.get("disclosure_date", ""),
            "sector": sector,
            "conflict": conflict,
            "chamber": "house",
            "district": item.get("district", ""),
            "owner": item.get("owner", ""),
        }
        trades.append(trade)

    return trades


def process_senate_data(raw_data):
    """상원 데이터 가공"""
    trades = []
    for item in raw_data:
        ticker = item.get("ticker", "").strip()
        if not ticker or ticker == "--" or ticker == "N/A":
            continue

        rep = item.get("senator", "").strip()
        if not rep:
            rep = item.get("full_name", "").strip()
        if not rep:
            continue

        tx_type = item.get("type", "").strip().lower()
        is_buy = "purchase" in tx_type
        is_sell = "sale" in tx_type
        if not is_buy and not is_sell:
            continue

        tx_date = item.get("transaction_date", "")
        if not tx_date or tx_date == "--":
            tx_date = item.get("disclosure_date", "")

        party = get_party(rep)
        sector = get_sector(ticker)
        conflict = check_conflict(rep, sector)

        trade = {
            "rep": rep,
            "party": party,
            "ticker": ticker.upper(),
            "asset": item.get("asset_description", "")[:60],
            "type": "buy" if is_buy else "sell",
            "amount": item.get("amount", ""),
            "amount_mid": get_amount_mid(item.get("amount", "")),
            "date": tx_date,
            "disclosure_date": item.get("disclosure_date", ""),
            "sector": sector,
            "conflict": conflict,
            "chamber": "senate",
            "district": "",
            "owner": item.get("owner", ""),
        }
        trades.append(trade)

    return trades


def compute_stats(trades):
    """통계 데이터 계산"""
    # 인기 종목
    stock_map = {}
    for t in trades:
        if t["type"] != "buy":
            continue
        tk = t["ticker"]
        if tk not in stock_map:
            stock_map[tk] = {"ticker": tk, "asset": t["asset"], "count": 0, "volume": 0, "traders": set(), "conflicts": 0, "sector": t["sector"]}
        stock_map[tk]["count"] += 1
        stock_map[tk]["volume"] += t["amount_mid"]
        stock_map[tk]["traders"].add(t["rep"])
        if t["conflict"]:
            stock_map[tk]["conflicts"] += 1

    popular = sorted(stock_map.values(), key=lambda x: x["count"], reverse=True)[:20]
    for s in popular:
        s["traders"] = len(s["traders"])

    # 섹터 분포
    sector_map = {}
    for t in trades:
        if t["type"] != "buy" or not t["sector"]:
            continue
        sec = t["sector"]
        if sec not in sector_map:
            sector_map[sec] = {"name": sec, "value": 0, "count": 0, "conflicts": 0}
        sector_map[sec]["value"] += t["amount_mid"]
        sector_map[sec]["count"] += 1
        if t["conflict"]:
            sector_map[sec]["conflicts"] += 1

    sectors = sorted(sector_map.values(), key=lambda x: x["value"], reverse=True)

    # 정당별 통계
    party_stats = {"D": {"buy": 0, "sell": 0, "buy_vol": 0, "sell_vol": 0, "conflicts": 0},
                   "R": {"buy": 0, "sell": 0, "buy_vol": 0, "sell_vol": 0, "conflicts": 0}}
    for t in trades:
        p = t.get("party")
        if p not in party_stats:
            continue
        if t["type"] == "buy":
            party_stats[p]["buy"] += 1
            party_stats[p]["buy_vol"] += t["amount_mid"]
        else:
            party_stats[p]["sell"] += 1
            party_stats[p]["sell_vol"] += t["amount_mid"]
        if t["conflict"]:
            party_stats[p]["conflicts"] += 1

    # 주요 거래자
    trader_map = {}
    for t in trades:
        rep = t["rep"]
        if rep not in trader_map:
            trader_map[rep] = {"name": rep, "party": t["party"], "buys": 0, "sells": 0, "volume": 0, "tickers": set(), "conflicts": 0}
        if t["type"] == "buy":
            trader_map[rep]["buys"] += 1
        else:
            trader_map[rep]["sells"] += 1
        trader_map[rep]["volume"] += t["amount_mid"]
        trader_map[rep]["tickers"].add(t["ticker"])
        if t["conflict"]:
            trader_map[rep]["conflicts"] += 1

    top_traders = sorted(trader_map.values(), key=lambda x: x["volume"], reverse=True)[:20]
    for tr in top_traders:
        tr["tickers"] = len(tr["tickers"])

    return {
        "popular_stocks": popular,
        "sectors": sectors,
        "party_stats": party_stats,
        "top_traders": top_traders,
    }


def main():
    print("🏛️ 미국 의회 주식 거래 데이터 수집 시작")
    print(f"  📅 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # 1. 데이터 가져오기
    print("[1/4] 데이터 가져오기...")
    house_raw = fetch_json(HOUSE_URL, "하원 데이터")
    senate_raw = fetch_json(SENATE_URL, "상원 데이터")
    print()

    if not house_raw and not senate_raw:
        print("❌ 데이터를 가져올 수 없습니다.")
        sys.exit(1)

    # 2. 데이터 가공
    print("[2/4] 데이터 가공 중...")
    house_trades = process_house_data(house_raw)
    senate_trades = process_senate_data(senate_raw)
    all_trades = house_trades + senate_trades

    # 최근 1년 데이터만 필터
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    recent_trades = [t for t in all_trades if t.get("date", "") >= one_year_ago]
    recent_trades.sort(key=lambda x: x.get("date", ""), reverse=True)

    print(f"  ✅ 전체 거래: {len(all_trades)}건")
    print(f"  ✅ 최근 1년: {len(recent_trades)}건")
    print(f"  ✅ 이해충돌(💎): {sum(1 for t in recent_trades if t['conflict'])}건")
    print()

    # 3. 통계 계산
    print("[3/4] 통계 계산 중...")
    stats = compute_stats(recent_trades)
    print(f"  ✅ 인기 종목 TOP {len(stats['popular_stocks'])}")
    print(f"  ✅ 섹터 {len(stats['sectors'])}개")
    print(f"  ✅ 주요 거래자 TOP {len(stats['top_traders'])}")
    print()

    # 4. JSON 출력
    print("[4/4] JSON 저장 중...")
    output = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S KST"),
        "total_trades": len(recent_trades),
        "total_buy": sum(1 for t in recent_trades if t["type"] == "buy"),
        "total_sell": sum(1 for t in recent_trades if t["type"] == "sell"),
        "total_conflicts": sum(1 for t in recent_trades if t["conflict"]),
        "trades": recent_trades[:500],  # 최근 500건
        "stats": stats,
        "politician_info": POLITICIAN_INFO,
        "sector_jurisdiction": SECTOR_JURISDICTION_MAP,
    }

    output_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "congress_trades.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    file_size = os.path.getsize(output_path)
    print(f"  ✅ 저장 완료: {output_path} ({file_size/1024:.1f}KB)")
    print()
    print("🎉 완료!")


if __name__ == "__main__":
    main()
