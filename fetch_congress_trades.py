#!/usr/bin/env python3
"""
미국 의회 주식 거래 데이터 수집 스크립트
- Finnhub API (무료, 60콜/분)
- 주요 종목별 의회 거래 데이터 fetch
- 정당/위원회 매핑 + 이해충돌(💎) 판별
- JSON 출력

Finnhub 무료 가입: https://finnhub.io/register
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ═══════════════════════════════════════════════
# FINNHUB API 설정
# ═══════════════════════════════════════════════
FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "")
FINNHUB_BASE = "https://finnhub.io/api/v1"

# 추적할 주요 종목 (의원들이 자주 거래하는 종목)
TICKERS = [
    "NVDA", "AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA", "NFLX",
    "AVGO", "AMD", "INTC", "QCOM", "MU", "PLTR", "CRM", "ORCL", "NOW",
    "RTX", "LMT", "GD", "NOC", "BA", "HII", "LHX",
    "V", "MA", "JPM", "GS", "BAC", "MS",
    "XOM", "CVX", "COP", "SLB",
    "UNH", "JNJ", "PFE", "LLY", "ABBV", "MRK",
    "HL", "NEM", "FCX",
    "DJT", "DIS",
    "RIVN", "LCID",
    "SNOW",
]

# ═══════════════════════════════════════════════
# POLITICIAN INFO
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
        "committees": ["하원 에너지·상업위원회", "하원 정보위원회"],
        "subcommittees": [], "jurisdiction": ["에너지", "통신", "사이버보안"],
        "sectors": ["소프트웨어", "에너지", "방산"],
        "note": "정보위 소속으로 방산·사이버 기업 투자 주목"
    },
    "Tommy Tuberville": {
        "committees": ["상원 군사위원회", "상원 농업위원회"],
        "subcommittees": [], "jurisdiction": ["국방예산", "군사계약"],
        "sectors": ["방산", "반도체"],
        "note": "군사위 소속 + 방산주 대량 매수 → 윤리 조사 대상"
    },
    "Mark Green": {
        "committees": ["하원 국토안보위원회 (위원장)", "하원 군사위원회"],
        "subcommittees": [], "jurisdiction": ["국토안보", "군사계약", "방위산업"],
        "sectors": ["방산"],
        "note": "국토안보위 위원장으로서 방산 기업 직접 관할 + 매수"
    },
    "Josh Gottheimer": {
        "committees": ["하원 금융서비스위원회"],
        "subcommittees": [], "jurisdiction": ["은행규제", "핀테크", "디지털자산"],
        "sectors": ["테크", "금융"],
        "note": "빅테크 규제 논의 중 기술주 매수"
    },
    "Marjorie Taylor Greene": {
        "committees": ["하원 국토안보위원회"],
        "subcommittees": [], "jurisdiction": ["국토안보", "정부 운영"],
        "sectors": ["전기차", "미디어"],
        "note": "DJT 매수는 정치적 충성도 표현"
    },
    "Ro Khanna": {
        "committees": ["하원 군사위원회"],
        "subcommittees": [], "jurisdiction": ["국방기술", "실리콘밸리 기술"],
        "sectors": ["테크", "소프트웨어"],
        "note": "실리콘밸리 지역구, 기술주 활발"
    },
    "Michael McCaul": {
        "committees": ["하원 외교위원회 (위원장)"],
        "subcommittees": [], "jurisdiction": ["외교정책", "반도체 수출통제"],
        "sectors": ["반도체", "소프트웨어"],
        "note": "CHIPS Act 반도체 정책 주도 + NVDA, AVGO 대량 매수"
    },
    "Daniel Goldman": {
        "committees": ["하원 국토안보위원회"],
        "subcommittees": [], "jurisdiction": ["국토안보", "기업규제"],
        "sectors": ["테크", "금융"],
        "note": "뉴욕 금융가 지역구"
    },
    "Debbie Wasserman Schultz": {
        "committees": ["하원 세출위원회"],
        "subcommittees": ["환경·제조·핵심광물 소위원회"],
        "jurisdiction": ["환경정책", "핵심광물", "광업규제"],
        "sectors": ["광업", "에너지"],
        "note": "핵심광물 소위 소속 + Hecla Mining(HL) 매수"
    },
    "Rick Scott": {
        "committees": ["상원 상업·과학·교통위원회"],
        "subcommittees": [], "jurisdiction": ["에너지정책", "교통"],
        "sectors": ["에너지"],
        "note": "에너지 위원회 소속 + 석유 대기업 투자"
    },
    "Lois Frankel": {
        "committees": ["하원 세출위원회"],
        "subcommittees": [], "jurisdiction": ["예산배분", "보건예산"],
        "sectors": ["헬스케어"],
        "note": "세출위 소속 보건 예산 영향력"
    },
}

SECTOR_MAP = {
    "NVDA": "반도체", "AMD": "반도체", "AVGO": "반도체", "INTC": "반도체",
    "QCOM": "반도체", "TSM": "반도체", "MRVL": "반도체", "MU": "반도체",
    "AAPL": "테크", "GOOGL": "테크", "GOOG": "테크", "META": "테크",
    "AMZN": "테크", "NFLX": "테크",
    "MSFT": "소프트웨어", "CRM": "소프트웨어", "PLTR": "소프트웨어",
    "SNOW": "소프트웨어", "NOW": "소프트웨어", "ORCL": "소프트웨어",
    "RTX": "방산", "LMT": "방산", "GD": "방산", "NOC": "방산",
    "BA": "방산", "HII": "방산", "LHX": "방산",
    "TSLA": "전기차", "RIVN": "전기차", "LCID": "전기차",
    "JPM": "금융", "BAC": "금융", "V": "금융", "MA": "금융",
    "GS": "금융", "MS": "금융",
    "XOM": "에너지", "CVX": "에너지", "COP": "에너지", "SLB": "에너지",
    "UNH": "헬스케어", "JNJ": "헬스케어", "PFE": "헬스케어",
    "LLY": "헬스케어", "ABBV": "헬스케어", "MRK": "헬스케어",
    "HL": "광업", "NEM": "광업", "FCX": "광업",
    "DJT": "미디어", "DIS": "미디어",
}

SECTOR_JURISDICTION_MAP = {
    "반도체": "반도체 수출통제·CHIPS Act", "테크": "빅테크 규제·독점금지",
    "소프트웨어": "사이버보안·기술정책", "방산": "국방예산·군사계약",
    "전기차": "친환경·EV 보조금", "미디어": "통신·미디어 규제",
    "금융": "은행규제·핀테크", "에너지": "에너지·화석연료",
    "헬스케어": "보건예산·의약품 규제", "광업": "광물 규제·환경정책",
}


def fetch_json(url, label=""):
    try:
        req = Request(url, headers={"User-Agent": "CongressTracker/1.0"})
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (URLError, HTTPError) as e:
        print(f"    ⚠️ {label} 실패: {e}")
        return None


def get_sector(ticker):
    return SECTOR_MAP.get(ticker.upper().strip(), "기타")


def check_conflict(name, sector):
    if not sector or sector == "기타":
        return False
    clean = name.replace("Hon. ", "").replace("Rep. ", "").replace("Sen. ", "").strip()
    info = POLITICIAN_INFO.get(clean)
    if not info:
        # 부분 매칭 시도 (성으로)
        last = clean.split()[-1] if clean else ""
        for key, val in POLITICIAN_INFO.items():
            if last and last in key:
                return sector in val.get("sectors", [])
        return False
    return sector in info.get("sectors", [])


def get_amount_mid(amount_str):
    if not amount_str:
        return 0
    amount_map = {
        "$1,001 - $15,000": 8000, "$15,001 - $50,000": 32500,
        "$50,001 - $100,000": 75000, "$100,001 - $250,000": 175000,
        "$250,001 - $500,000": 375000, "$500,001 - $1,000,000": 750000,
        "$1,000,001 - $5,000,000": 3000000, "$5,000,001 - $25,000,000": 15000000,
        "$25,000,001 - $50,000,000": 37500000, "$50,000,000 +": 50000000,
    }
    return amount_map.get(amount_str.strip(), 0)


def fetch_all_trades():
    """Finnhub에서 주요 종목별로 의회 거래 데이터 가져오기"""
    date_from = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    date_to = datetime.now().strftime("%Y-%m-%d")
    all_trades = []
    seen = set()  # 중복 제거

    for i, ticker in enumerate(TICKERS):
        url = f"{FINNHUB_BASE}/stock/congressional-trading?symbol={ticker}&from={date_from}&to={date_to}&token={FINNHUB_KEY}"
        data = fetch_json(url, ticker)

        if data and isinstance(data, dict) and "data" in data:
            items = data["data"]
            for item in items:
                name = item.get("name", "").strip()
                tx_date = item.get("transactionDate", "")
                tx_type = item.get("transactionType", "").lower()
                amount = item.get("amountFrom", 0) or 0
                amount_to = item.get("amountTo", 0) or 0
                amount_text = item.get("transactionAmount", "")
                owner = item.get("ownerType", "")

                is_buy = "purchase" in tx_type or "buy" in tx_type
                is_sell = "sale" in tx_type or "sell" in tx_type
                if not is_buy and not is_sell:
                    continue

                # 중복 제거 키
                key = f"{name}|{ticker}|{tx_date}|{tx_type}"
                if key in seen:
                    continue
                seen.add(key)

                sector = get_sector(ticker)
                conflict = check_conflict(name, sector)
                mid = int((amount + amount_to) / 2) if amount_to else amount
                if not mid:
                    mid = get_amount_mid(amount_text)

                # 정당 판별 (Finnhub은 chamber 정보 제공)
                chamber = item.get("chamber", "").lower()
                party = None
                # 주요 정치인 정당 매핑
                party_map = {
                    "Pelosi": "D", "Gottheimer": "D", "Khanna": "D", "Goldman": "D",
                    "Wasserman": "D", "Frankel": "D", "Schiff": "D", "Jeffries": "D",
                    "Ossoff": "D", "Kelly": "D", "Warner": "D", "Peters": "D",
                    "Crenshaw": "R", "Tuberville": "R", "Green": "R", "Greene": "R",
                    "McCaul": "R", "Scott": "R", "Hern": "R", "Mullin": "R",
                    "Rouzer": "R", "Cruz": "R", "Hagerty": "R", "Hill": "R",
                }
                for surname, p in party_map.items():
                    if surname in name:
                        party = p
                        break

                trade = {
                    "rep": name,
                    "party": party,
                    "ticker": ticker,
                    "asset": item.get("assetName", ticker)[:60],
                    "type": "buy" if is_buy else "sell",
                    "amount": amount_text,
                    "amount_mid": mid,
                    "date": tx_date,
                    "disclosure_date": item.get("filingDate", ""),
                    "sector": sector,
                    "conflict": conflict,
                    "chamber": chamber or ("senate" if "Sen" in name else "house"),
                    "owner": owner,
                }
                all_trades.append(trade)

            if items:
                print(f"  ✅ {ticker}: {len(items)}건")
        elif data and isinstance(data, list):
            # Finnhub이 리스트로 반환하는 경우
            for item in data:
                name = item.get("name", "").strip()
                tx_date = item.get("transactionDate", "")
                tx_type = item.get("transactionType", "").lower()

                is_buy = "purchase" in tx_type or "buy" in tx_type
                is_sell = "sale" in tx_type or "sell" in tx_type
                if not is_buy and not is_sell:
                    continue

                key = f"{name}|{ticker}|{tx_date}|{tx_type}"
                if key in seen:
                    continue
                seen.add(key)

                sector = get_sector(ticker)
                conflict = check_conflict(name, sector)

                trade = {
                    "rep": name, "party": None, "ticker": ticker,
                    "asset": item.get("assetName", ticker)[:60],
                    "type": "buy" if is_buy else "sell",
                    "amount": item.get("transactionAmount", ""),
                    "amount_mid": 0, "date": tx_date,
                    "sector": sector, "conflict": conflict,
                    "chamber": "house", "owner": "",
                }
                all_trades.append(trade)

            if data:
                print(f"  ✅ {ticker}: {len(data)}건")

        # Rate limit: 60콜/분 → 1초 간격
        if (i + 1) % 10 == 0:
            print(f"  ⏳ {i+1}/{len(TICKERS)} 완료, 잠시 대기...")
        time.sleep(1.1)

    return all_trades


def compute_stats(trades):
    stock_map, sector_map, trader_map = {}, {}, {}
    ps = {"D": {"buy":0,"sell":0,"buy_vol":0,"sell_vol":0,"conflicts":0},
          "R": {"buy":0,"sell":0,"buy_vol":0,"sell_vol":0,"conflicts":0}}
    for t in trades:
        if t["type"] == "buy":
            tk = t["ticker"]
            if tk not in stock_map:
                stock_map[tk] = {"ticker":tk,"asset":t["asset"],"count":0,"volume":0,"traders":set(),"conflicts":0,"sector":t["sector"]}
            stock_map[tk]["count"] += 1
            stock_map[tk]["volume"] += t["amount_mid"]
            stock_map[tk]["traders"].add(t["rep"])
            if t["conflict"]: stock_map[tk]["conflicts"] += 1

            sec = t["sector"]
            if sec:
                if sec not in sector_map: sector_map[sec] = {"name":sec,"value":0,"count":0,"conflicts":0}
                sector_map[sec]["value"] += t["amount_mid"]
                sector_map[sec]["count"] += 1
                if t["conflict"]: sector_map[sec]["conflicts"] += 1

        p = t.get("party")
        if p in ps:
            if t["type"] == "buy": ps[p]["buy"] += 1; ps[p]["buy_vol"] += t["amount_mid"]
            else: ps[p]["sell"] += 1; ps[p]["sell_vol"] += t["amount_mid"]
            if t["conflict"]: ps[p]["conflicts"] += 1

        rep = t["rep"]
        if rep not in trader_map:
            trader_map[rep] = {"name":rep,"party":t["party"],"buys":0,"sells":0,"volume":0,"tickers":set(),"conflicts":0}
        if t["type"] == "buy": trader_map[rep]["buys"] += 1
        else: trader_map[rep]["sells"] += 1
        trader_map[rep]["volume"] += t["amount_mid"]
        trader_map[rep]["tickers"].add(t["ticker"])
        if t["conflict"]: trader_map[rep]["conflicts"] += 1

    popular = sorted(stock_map.values(), key=lambda x: x["count"], reverse=True)[:20]
    for s in popular: s["traders"] = len(s["traders"])
    sectors = sorted(sector_map.values(), key=lambda x: x["value"], reverse=True)
    top = sorted(trader_map.values(), key=lambda x: x["volume"], reverse=True)[:20]
    for tr in top: tr["tickers"] = len(tr["tickers"])
    return {"popular_stocks":popular, "sectors":sectors, "party_stats":ps, "top_traders":top}


def main():
    print("🏛️ 미국 의회 주식 거래 데이터 수집 시작")
    print(f"  📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    if not FINNHUB_KEY:
        print("❌ FINNHUB_API_KEY 환경변수가 설정되지 않았습니다.")
        print("   무료 가입: https://finnhub.io/register")
        print("   GitHub Settings → Secrets → FINNHUB_API_KEY 추가")
        sys.exit(1)

    # 1. 데이터 수집
    print(f"[1/3] Finnhub에서 {len(TICKERS)}개 종목 의회 거래 수집...")
    trades = fetch_all_trades()
    trades.sort(key=lambda x: x.get("date", ""), reverse=True)
    print(f"\n  📊 총 {len(trades)}건 수집")
    print(f"  💎 이해충돌: {sum(1 for t in trades if t['conflict'])}건")
    print()

    # 2. 통계
    print("[2/3] 통계 계산...")
    stats = compute_stats(trades)
    print()

    # 3. 저장
    print("[3/3] JSON 저장...")
    output = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S KST"),
        "total_trades": len(trades),
        "total_buy": sum(1 for t in trades if t["type"] == "buy"),
        "total_sell": sum(1 for t in trades if t["type"] == "sell"),
        "total_conflicts": sum(1 for t in trades if t["conflict"]),
        "trades": trades[:500],
        "stats": stats,
        "politician_info": POLITICIAN_INFO,
        "sector_jurisdiction": SECTOR_JURISDICTION_MAP,
    }

    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "congress_trades.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  ✅ {path} ({os.path.getsize(path)/1024:.1f}KB)")
    print("\n🎉 완료!")


if __name__ == "__main__":
    main()
