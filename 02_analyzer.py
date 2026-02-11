# -*- coding: utf-8 -*-
"""
02_analyzer.py
- raw_{today_str}.csv를 읽어 LLM 판단(번역, 영업 적합성, 한국 회사 여부)을 추가한 뒤
  final_{today_str}.csv로 저장하고 메일 발송.
- 실행: python 02_analyzer.py [raw_YYYY-MM-DD.csv]
"""

import asyncio
import datetime
import json
import os
import re
import smtplib
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd

# --- 설정 ---
SKIP_TRANSLATION = False  # True면 번역 스킵 (테스트용)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
LLM_RATE_LIMIT_CALLS = 15
LLM_RATE_LIMIT_WINDOW_SEC = 60
_llm_call_times = deque(maxlen=LLM_RATE_LIMIT_CALLS)
_llm_lock = asyncio.Lock()

SAVE_INTERVAL = 5  # N건마다 중간 저장

# 1차 로컬 필터: "일어 기사 제목"에 포함 시 즉시 부적합 (LLM 미호출)
NEGATIVE_KEYWORDS = [
    "回収", "お詫び", "訂正", "不適合", "中止", "誤記",
    "決算", "人事", "株価", "訃報", "事件", "事故",
    "アンケート", "調査", "実施", "共同",
]

# 번역용 스레드 풀
_executor = ThreadPoolExecutor(max_workers=2)

# 최종 CSV 열 순서
FINAL_COLUMNS = [
    "일어 기사 제목", "한국어 번역", "영업 적합성", "판단 근거",
    "기사 링크", "게재 일시", "회사명(원문)", "회사명(한국어)",
    "한국 회사 여부", "한국 회사 판단 근거", "회사 프로필 링크",
    "개요", "비즈니스카테고리", "키워드", "위치정보", "관련링크",
    "첨부PDF명", "첨부PDF링크", "소재파일명", "소재파일링크",
    "업종", "본사 주소", "전화번호", "대표자명", "상장 여부",
    "자본금", "설립일", "공식 URL", "SNS X", "SNS Facebook", "SNS YouTube",
    "이메일", "문의 웹사이트 URL",
]


def _translate_ja_to_ko(text: str) -> str:
    """일본어 → 한국어 번역 (동기). 실패 시 빈 문자열."""
    if SKIP_TRANSLATION or not text or not str(text).strip() or str(text) == "NULL":
        return ""
    try:
        from googletrans import Translator
        t = Translator()
        result = t.translate(str(text).strip(), src="ja", dest="ko")
        return (result.text or "").strip()
    except Exception:
        return ""


async def translate_title_async(ja_title: str) -> str:
    """제목 번역 (이벤트 루프 블로킹 방지)."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _translate_ja_to_ko, ja_title or "")


async def translate_company_async(ja_company: str) -> str:
    """회사명 한국어 (비동기 래퍼)."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _translate_ja_to_ko, ja_company or "")


def local_suitability_filter(title_jp: str) -> bool:
    """로컬 1차 필터. 부적합 키워드 포함 시 False."""
    if not title_jp or not str(title_jp).strip():
        return True
    for word in NEGATIVE_KEYWORDS:
        if word in str(title_jp):
            return False
    return True


async def _wait_llm_rate_limit():
    """1분 15회 제한: 최근 15회가 60초 안에 있으면 대기."""
    async with _llm_lock:
        now = time.monotonic()
        while len(_llm_call_times) >= LLM_RATE_LIMIT_CALLS:
            wait_sec = (_llm_call_times[0] + LLM_RATE_LIMIT_WINDOW_SEC) - now
            if wait_sec > 0:
                await asyncio.sleep(wait_sec)
            _llm_call_times.popleft()
            now = time.monotonic()


def _record_llm_call():
    _llm_call_times.append(time.monotonic())


def _call_openai_sync(prompt: str) -> str:
    """동기 OpenAI Chat Completion 호출 (스레드에서 실행)."""
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
    )
    content = response.choices[0].message.content if response.choices else ""
    return (content or "").strip()


async def judge_suitability(title_jp: str, title_ko: str) -> tuple:
    """영업 적합성 판단. (is_suitable: bool, reason: str)"""
    if not local_suitability_filter(title_jp):
        return False, "부적합 키워드 포함 (로컬 필터)"

    if not OPENAI_API_KEY:
        return None, "API Key 없음 (OPENAI_API_KEY)"

    prompt = f'''# Role: 일본 뷰티 시장 전문 영업 컨설턴트
# Task: 뉴스 제목이 '신규 영업 메일의 첫인사'로 적절한지 판단
# 판단 기준:
1. 화장품/뷰티 산업 관련성 (패션·식품 제외)
2. 긍정적 화제성: 신제품·수상·팝업 (분쟁·주가·결산 제외)
3. 메일 서두에 "축하드립니다" 언급 가능 여부
# Output (JSON만): {{"is_suitable": boolean, "reason": "string"}}
# Input: {title_jp} ({title_ko})
'''
    try:
        await _wait_llm_rate_limit()
        loop = asyncio.get_event_loop()
        raw = await loop.run_in_executor(None, _call_openai_sync, prompt)
        async with _llm_lock:
            _record_llm_call()
        text = re.sub(r"```json\s*|\s*```", "", raw).strip()
        data = json.loads(text)
        return data.get("is_suitable", False), data.get("reason", "")
    except Exception as e:
        return False, f"API 오류: {str(e)}"


async def judge_korean_company(company: str, address: str, url: str, keywords: str) -> tuple:
    """한국 회사 여부 판단. (label: str, reason: str)"""
    if not OPENAI_API_KEY:
        return "불명", "API Key 없음"

    prompt = f'''# Role: 글로벌 뷰티 기업 분석 전문가
# Task: 아래 회사가 한국 기업 또는 한국계 기업(일본 지사 포함)인지 판단
# 판단 규칙:
- 아모레퍼시픽·LG생활건강·코스알엑스·이니스프리·설화수·아누아·토리든 등 한국 본사 브랜드의 일본 법인/지사 → '한국'
- 본사 주소가 일본이어도 브랜드/모회사가 한국이면 → '한국'
- URL이 .co.kr이거나 회사명에 한글/Korea 포함 → '한국'
- 불명확하면 → '불명'
# Output (JSON만): {{"label": "한국"|"비한국"|"불명", "reason": "string"}}
# Input:
- 회사명: {company or ""}
- 주소: {address or ""}
- URL: {url or ""}
- 키워드: {keywords or ""}
'''
    try:
        await _wait_llm_rate_limit()
        loop = asyncio.get_event_loop()
        raw = await loop.run_in_executor(None, _call_openai_sync, prompt)
        async with _llm_lock:
            _record_llm_call()
        text = re.sub(r"```json\s*|\s*```", "", raw).strip()
        data = json.loads(text)
        label = data.get("label", "불명")
        if label not in ("한국", "비한국", "불명"):
            label = "불명"
        return label, data.get("reason", "")
    except Exception as e:
        return "불명", f"API 오류: {str(e)}"


def _ensure_columns(row: dict) -> dict:
    """최종 컬럼 순서대로 값 채우기. 없으면 빈 문자열."""
    out = {}
    for col in FINAL_COLUMNS:
        out[col] = row.get(col, "")
        if not isinstance(out[col], str) and pd.notna(out[col]):
            out[col] = str(out[col])
        if pd.isna(out[col]):
            out[col] = ""
    return out


def _save_intermediate(results: list, final_path: str, current: int, total: int) -> None:
    """현재까지 처리된 결과를 CSV에 덮어쓰기 저장."""
    pd.DataFrame(results, columns=FINAL_COLUMNS).to_csv(
        final_path, index=False, encoding="utf-8-sig"
    )
    print(f"  → 중간 저장 완료 ({current}/{total}건)")


def send_email(today_str: str, csv_path: str, total: int, suitable_count: int) -> None:
    """Gmail SMTP 587로 final CSV 첨부 메일 발송."""
    sender = os.environ.get("SENDER_EMAIL")
    password = os.environ.get("SENDER_PASSWORD")
    recipient = os.environ.get("RECIPIENT_EMAIL")
    if not sender or not password or not recipient:
        print("메일 발송 스킵: SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_EMAIL 중 누락")
        return
    subject = f"[PR TIMES] 뷰티 뉴스 영업 리스트 {today_str}"
    body = f"총 {total}건 수집, 영업 적합 {suitable_count}건"

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(body, "plain", "utf-8"))

    if os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            part = MIMEApplication(f.read(), _subtype="csv")
            part.add_header("Content-Disposition", "attachment", filename=os.path.basename(csv_path))
            msg.attach(part)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
        print(f"메일 발송 완료: {recipient}")
    except Exception as e:
        print(f"메일 발송 실패: {e}")


async def run_analysis(input_path: str, today_str: str) -> None:
    """raw CSV 읽기 → 행별 LLM 처리 → final CSV 저장 → 메일 발송."""
    if not os.path.exists(input_path):
        print(f"에러: 파일 없음 — {input_path}")
        sys.exit(1)

    df = pd.read_csv(input_path, encoding="utf-8-sig", dtype=str)
    df = df.fillna("")
    total = len(df)
    print(f"총 {total}건 로드: {input_path}")
    print("-" * 50)

    results = []
    suitable_count = 0
    final_path = f"final_{today_str}.csv"

    for i in range(total):
        current = i + 1  # 1-based
        remaining = total - current

        row = df.iloc[i].to_dict()
        row = {str(k): ("" if (pd.isna(v) or v is None) else str(v)) for k, v in row.items()}

        title_jp = row.get("일어 기사 제목", "")
        comp_jp = row.get("회사명(원문)", "")

        # 진행 상황 출력
        title_preview = title_jp[:30] + "..." if len(title_jp) > 30 else title_jp
        print(f"[처리중 {current}/{total}] {title_preview} (남은 기사 {remaining}건)")

        title_ko = await translate_title_async(title_jp)
        comp_ko = await translate_company_async(comp_jp)

        suitability, reason = await judge_suitability(title_jp, title_ko)
        if suitability is True:
            suitable_count += 1

        address = row.get("본사 주소", "")
        url = row.get("공식 URL", "")
        keywords = row.get("키워드", "")
        kr_label, kr_reason = await judge_korean_company(comp_jp, address, url, keywords)

        out = dict(row)
        out["한국어 번역"] = title_ko
        out["회사명(한국어)"] = comp_ko
        out["영업 적합성"] = suitability
        out["판단 근거"] = reason or ""
        out["한국 회사 여부"] = kr_label
        out["한국 회사 판단 근거"] = kr_reason or ""

        for col in FINAL_COLUMNS:
            if col not in out:
                out[col] = ""
        results.append(_ensure_columns(out))

        # SAVE_INTERVAL건마다 또는 마지막 건에서 중간 저장
        if current % SAVE_INTERVAL == 0 or current == total:
            _save_intermediate(results, final_path, current, total)

    print("-" * 50)
    print(f"완료: {final_path} | 총 {total}건, 영업 적합 {suitable_count}건")

    send_email(today_str, final_path, total, suitable_count)


def main():
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    if len(sys.argv) >= 2:
        input_path = sys.argv[1].strip()
        m = re.match(r"raw_(\d{4}-\d{2}-\d{2})\.csv", os.path.basename(input_path))
        if m:
            today_str = m.group(1)
    else:
        input_path = f"raw_{today_str}.csv"

    asyncio.run(run_analysis(input_path, today_str))


if __name__ == "__main__":
    main()