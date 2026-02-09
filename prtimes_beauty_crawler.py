# -*- coding: utf-8 -*-
"""
PR TIMES Beauty 섹션 오늘자 뉴스 크롤러
- 목록 페이지에서 '오늘' 게재 기사만 필터링 (分前, 時間前)
- 각 기사 상세 페이지에서 제목/회사/프로필/연락처 추출
- 5건마다 CSV에 저장
"""

import asyncio
import datetime
import os
import re
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from playwright.async_api import async_playwright

# --- 설정 ---
# 오늘 날짜를 가져와서 파일명에 넣습니다 (예: prtimes_beauty_2024-05-22.csv)
today_str = datetime.datetime.now().strftime("%Y-%m-%d")
TARGET_URL = "https://prtimes.jp/beauty/"
OUTPUT_FILE = f"prtimes_beauty_{today_str}.csv"
SAVE_INTERVAL = 5

# 한국어 번역: googletrans 사용 (동기 라이브러리 → 스레드 풀에서 실행)
# 대안: Ollama 로컬 번역 시 아래 ask_ollama 사용으로 교체 가능
_executor = ThreadPoolExecutor(max_workers=2)


# 번역 비활성화 시 True (크롤링만 테스트)
SKIP_TRANSLATION = True


def _translate_ja_to_ko(text: str) -> str:
    """일본어 → 한국어 번역 (동기, 스레드에서 호출). 실패 시 NULL 반환."""
    if SKIP_TRANSLATION or not text or text.strip() == "" or text == "NULL":
        return "NULL"
    try:
        from googletrans import Translator
        t = Translator()
        result = t.translate(text, src="ja", dest="ko")
        return (result.text or "NULL").strip()
    except Exception:
        return "NULL"


def _company_name_ko_phonetic(ja_name: str) -> str:
    """일본어 회사명을 한국어 발음 표기 (예: 주식회사 XXX). 동기."""
    if SKIP_TRANSLATION or not ja_name or ja_name == "NULL":
        return "NULL"
    try:
        from googletrans import Translator
        t = Translator()
        # 발음용: 일본어를 한국어로 번역하면 한글 표기가 나오는 경우가 많음
        result = t.translate(ja_name, src="ja", dest="ko")
        return (result.text or "NULL").strip()
    except Exception:
        return "NULL"


async def translate_title_async(ja_title: str) -> str:
    """기사 제목 일본어 → 한국어 (이벤트 루프 블로킹 방지)."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _translate_ja_to_ko, ja_title)


async def translate_company_async(ja_company: str) -> str:
    """회사명 한국어 발음 표기 (비동기 래퍼)."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _company_name_ko_phonetic, ja_company)


# og:description에서 게재 일시 추출 (예: （2026年2月9日 11時00分）)
async def _extract_publish_time_from_og_description(page) -> str:
    """기사 상세 페이지의 meta og:description에서 （YYYY年M月D日 H時MM分） 형식 추출."""
    try:
        meta = await page.query_selector('meta[property="og:description"]')
        if not meta:
            return ""
        content = await meta.get_attribute("content") or ""
        match = re.search(r"[（(](\d{4}年\d{1,2}月\d{1,2}日\s*\d{1,2}時\d{1,2}分)[）)]", content)
        return match.group(1).strip() if match else ""
    except Exception:
        return ""


def _extract_email_and_website(body_text: str) -> tuple:
    """본문에서 이메일 주소와 문의용 웹사이트 URL 추출. (없으면 빈 문자열)"""
    email = ""
    website = ""
    if not body_text:
        return email, website
    # 이메일
    email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', body_text)
    if email_match:
        email = email_match.group(0)
    # 본문에 등장하는 http/https URL (공식 URL과 구분하기 위해 'prtimes' 제외한 일반 URL 1개)
    url_pattern = r'https?://[a-zA-Z0-9][-a-zA-Z0-9.]*(?:/[^\s<>"\']*)?'
    urls = re.findall(url_pattern, body_text)
    for u in urls:
        if "prtimes" not in u and "google" not in u and "facebook" not in u and "youtube" not in u and "x.com" not in u and "twitter" not in u:
            website = u
            break
    return email, website


async def extract_company_profile(page) -> dict:
    """
    기사 하단의 회사 프로필(dl > dt, dd)에서
    업종, 본사 주소, 전화번호, 대표자명, 상장, 자본금, 설립일, 공식 URL, SNS(X, Facebook, YouTube) 추출.
    """
    data = {
        "업종": "",
        "본사 주소": "",
        "전화번호": "",
        "대표자명": "",
        "상장 여부": "",
        "자본금": "",
        "설립일": "",
        "공식 URL": "",
        "SNS X": "",
        "SNS Facebook": "",
        "SNS YouTube": "",
    }
    # PR TIMES는 dl.__dl_93dhx_1 형태의 클래스를 사용할 수 있음 (변경 가능성 있으므로 둘 다 시도)
    selectors = ["dl.__dl_93dhx_1", "dl"]
    for sel in selectors:
        dl_elements = await page.query_selector_all(sel)
        for dl in dl_elements:
            dts = await dl.query_selector_all("dt")
            dds = await dl.query_selector_all("dd")
            for dt, dd in zip(dts, dds):
                try:
                    key = (await dt.inner_text()).strip()
                    link_el = await dd.query_selector("a")
                    if link_el:
                        val = (await link_el.get_attribute("href")) or ""
                    else:
                        val = (await dd.inner_text()).strip().replace("\n", " ")
                except Exception:
                    val = ""
                if "業種" in key:
                    data["업종"] = val
                elif "本社所在地" in key:
                    data["본사 주소"] = val
                elif "電話番号" in key:
                    data["전화번호"] = val
                elif "代表者名" in key:
                    data["대표자명"] = val
                elif "上場" in key:
                    data["상장 여부"] = val
                elif "資本金" in key:
                    data["자본금"] = val
                elif "設立" in key:
                    data["설립일"] = val
                elif "URL" in key or key.strip() == "URL":
                    data["공식 URL"] = val
                elif key.strip() == "X":
                    data["SNS X"] = val
                elif "Facebook" in key:
                    data["SNS Facebook"] = val
                elif "YouTube" in key:
                    data["SNS YouTube"] = val
        if any(data.values()):
            break
    return data


def _is_today_time(time_text: str) -> bool:
    """'~분 전', '~시간 전'이면 True. '일' 또는 특정 날짜(예: 2月8日)가 있으면 False."""
    if not time_text:
        return False
    t = time_text.strip()
    if "分前" in t or "時間前" in t:
        return True
    if "日" in t or "昨日" in t:
        return False
    # 2月8日 같은 패턴
    if re.search(r"\d+月\d+日", t):
        return False
    return False


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        print(f"시작: {TARGET_URL} 접속 중...")
        await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_load_state("load")
        await asyncio.sleep(2)  # 목록(상대시간) 렌더링 대기

        # 「ビューティーのプレスリリース一覧」영역만 크롤링 (h2.page-main__heading 아래 section.list-latest-articles)
        section = await page.query_selector("section.list-latest-articles")
        if not section:
            section = await page.query_selector("h2.page-main__heading")

        # 한 페이지에서 기사 목록 추출 (상대시간 分前/時間前 + 전체날짜 2026年2月8日 15時00分 모두 반환해, 아래에서 오늘만 필터)
        def extract_articles(root_el):
            return root_el.evaluate("""
                (sectionEl) => {
                    const root = sectionEl || document;
                    const result = [];
                    const articleLinks = root.querySelectorAll('a[href*="/main/html/rd/p/"]');
                    const seen = new Set();
                    const timeRe = /(\\d+分前|\\d+時間前|\\d{4}年\\d{1,2}月\\d{1,2}日\\s*\\d{1,2}時\\d{1,2}分)/;
                    for (const a of articleLinks) {
                        const href = a.getAttribute('href') || '';
                        const link = href.startsWith('http') ? href : 'https://prtimes.jp' + href;
                        if (seen.has(link)) continue;
                        const card = a.closest('article') || a.closest('[class*="item"]') || a.closest('li') || a.closest('div[class*="release"]') || a.parentElement?.parentElement?.parentElement;
                        if (!card) continue;
                        const cardText = card.innerText || '';
                        const timeMatch = cardText.match(timeRe);
                        const timeText = timeMatch ? timeMatch[1] : '';
                        const titleEl = card.querySelector('h3 a') || card.querySelector('a[href*="/main/html/rd/p/"]');
                        const title = titleEl ? titleEl.innerText.trim() : (a.innerText || '').trim();
                        const companyLinkEl = card.querySelector('a[href*="company_id"]');
                        const compJp = companyLinkEl ? companyLinkEl.innerText.trim() : '';
                        const compLink = companyLinkEl && companyLinkEl.href ? companyLinkEl.href : '';
                        if (title && link) {
                            seen.add(link);
                            result.push({ title_jp: title, link, time: timeText, comp_jp: compJp || 'NULL', comp_link: compLink });
                        }
                    }
                    return result;
                }
            """)

        # 당일 전체 수집: 「もっと見る」클릭 반복 후, 오늘자(分前/時間前)만 유지
        seen_links = set()
        articles = []
        max_clicks = 50  # 무한 방지
        click_count = 0

        while click_count < max_clicks:
            section = await page.query_selector("section.list-latest-articles")
            if not section:
                section = await page.query_selector("h2.page-main__heading")
            batch = await extract_articles(section) if section else await page.evaluate("""
                () => {
                    const root = document;
                    const result = [];
                    const articleLinks = root.querySelectorAll('a[href*="/main/html/rd/p/"]');
                    const seen = new Set();
                    const timeRe = /(\\d+分前|\\d+時間前|\\d{4}年\\d{1,2}月\\d{1,2}日\\s*\\d{1,2}時\\d{1,2}分)/;
                    for (const a of articleLinks) {
                        const href = a.getAttribute('href') || '';
                        const link = href.startsWith('http') ? href : 'https://prtimes.jp' + href;
                        if (seen.has(link)) continue;
                        const card = a.closest('article') || a.closest('[class*="item"]') || a.closest('li') || a.closest('div[class*="release"]') || a.parentElement?.parentElement?.parentElement;
                        if (!card) continue;
                        const cardText = card.innerText || '';
                        const timeMatch = cardText.match(timeRe);
                        const timeText = timeMatch ? timeMatch[1] : '';
                        const titleEl = card.querySelector('h3 a') || card.querySelector('a[href*="/main/html/rd/p/"]');
                        const title = titleEl ? titleEl.innerText.trim() : (a.innerText || '').trim();
                        const companyLinkEl = card.querySelector('a[href*="company_id"]');
                        const compJp = companyLinkEl ? companyLinkEl.innerText.trim() : '';
                        const compLink = companyLinkEl && companyLinkEl.href ? companyLinkEl.href : '';
                        if (title && link) { seen.add(link); result.push({ title_jp: title, link, time: timeText, comp_jp: compJp || 'NULL', comp_link: compLink }); }
                    }
                    return result;
                }
            """)

            added = 0
            for art in batch:
                if art["link"] in seen_links:
                    continue
                if not _is_today_time(art["time"]):
                    break  # 오늘이 아닌 기사(2026年2月8日 등) 나오면 수집 중단
                seen_links.add(art["link"])
                articles.append(art)
                added += 1

            more_btn = await page.query_selector('a[href*="pagenum"]')
            if not more_btn:
                more_btn = await page.query_selector('a:has-text("もっと見る")')
            if not more_btn:
                more_btn = await page.query_selector('button:has-text("もっと見る")')
            if not more_btn:
                break
            if added == 0:
                break  # 이번에 추가된 오늘자 없으면 종료
            await more_btn.scroll_into_view_if_needed()
            await more_btn.click()
            await asyncio.sleep(2)
            click_count += 1
            print(f"  … 더보기 클릭 ({click_count}회), 누적 {len(articles)}건")

        total = len(articles)
        print(f"오늘자 기사 {total}건 수집. 상세 수집 시작.\n")

        results = []
        for i, art in enumerate(articles, 1):
            remaining = total - i
            print(f"[{i}/{total}] 처리 중... (남은 기사 {remaining}건)")

            # 한국어 번역 (제목 번역, 회사명 발음)
            title_ko = await translate_title_async(art["title_jp"])
            comp_ko = await translate_company_async(art["comp_jp"])

            detail_page = await context.new_page()
            try:
                await detail_page.goto(art["link"], wait_until="domcontentloaded", timeout=20000)
            except Exception:
                await detail_page.close()
                results.append({
                    "일어 기사 제목": art["title_jp"],
                    "한국어 번역": title_ko,
                    "기사 링크": art["link"],
                    "게재 일시": art["time"],  # 상세 미진입 시 목록 시간 유지
                    "회사명(원문)": art["comp_jp"],
                    "회사명(한국어 발음)": comp_ko,
                    "회사 프로필 링크": art["comp_link"],
                    "업종": "", "본사 주소": "", "전화번호": "", "대표자명": "",
                    "상장 여부": "", "자본금": "", "설립일": "", "공식 URL": "",
                    "SNS X": "", "SNS Facebook": "", "SNS YouTube": "",
                    "이메일": "", "문의 웹사이트 URL": "",
                })
                continue

            body_text = await detail_page.inner_text("body")
            email, website = _extract_email_and_website(body_text)
            company_profile = await extract_company_profile(detail_page)
            # D열 게재 일시: 기사 페이지 og:description 내 （2026年2月9日 11時00分） 사용
            pub_time = await _extract_publish_time_from_og_description(detail_page)
            await detail_page.close()

            record = {
                "일어 기사 제목": art["title_jp"],
                "한국어 번역": title_ko,
                "기사 링크": art["link"],
                "게재 일시": pub_time if pub_time else art["time"],
                "회사명(원문)": art["comp_jp"],
                "회사명(한국어 발음)": comp_ko,
                "회사 프로필 링크": art["comp_link"],
                **company_profile,
                "이메일": email or "",
                "문의 웹사이트 URL": website or "",
            }
            results.append(record)

            # 5건마다 (또는 마지막에) CSV 저장 — 지금까지 미저장분만 추가
            if i % SAVE_INTERVAL == 0 or i == total:
                start_idx = (i - 1) // SAVE_INTERVAL * SAVE_INTERVAL  # 이번에 쓸 구간 시작
                chunk = results[start_idx:i]
                df = pd.DataFrame(chunk)
                write_header = not os.path.exists(OUTPUT_FILE)
                df.to_csv(OUTPUT_FILE, mode="a", index=False, header=write_header, encoding="utf-8-sig")
                print(f"--- [{i}/{total}] 완료, {total - i}건 남음, CSV 저장됨 ---")

        await browser.close()
        print(f"\n모든 크롤링 완료. 총 {total}건 → {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
