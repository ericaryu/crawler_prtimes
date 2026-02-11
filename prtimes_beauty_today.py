# -*- coding: utf-8 -*-
"""
PR TIMES Beauty 섹션 오늘자 뉴스 크롤러
- 목록 페이지에서 '오늘' 게재 기사만 필터링 (分前, 時間前)
- 각 기사 상세 페이지에서 제목/회사/種類(개요·키워드·위치정보·소재 다운로드)/프로필/연락처 추출
- 5건마다 CSV에 저장
"""

import asyncio
import datetime
import os
import re

import pandas as pd
from playwright.async_api import async_playwright

# --- 설정 ---
# 오늘 날짜를 가져와서 파일명에 넣습니다 (예: prtimes_beauty_2024-05-22.csv)
today_str = datetime.datetime.now().strftime("%Y-%m-%d")
TARGET_URL = "https://prtimes.jp/beauty/"
OUTPUT_FILE = f"raw_{today_str}.csv"
SAVE_INTERVAL = 5

# 과거 산출물 정리: prtimes_beauty_today.csv가 남아 있으면 메일에 같이 첨부될 수 있어 제거
LEGACY_TODAY_CSV = "prtimes_beauty_today.csv"
if os.path.exists(LEGACY_TODAY_CSV):
    try:
        os.remove(LEGACY_TODAY_CSV)
    except Exception:
        pass


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


async def extract_article_category(page) -> dict:
    """
    기사 상단 '種類' 섹션에서
    商品サービス(개요), ビジネスカテゴリ, キーワード, 位置情報, ダウンロード(소재파일) 추출.
    dl > dt/dd 또는 div.table_row 내 dt, dd 구조 대응.
    """
    data = {
        "개요": "",
        "비즈니스카테고리": "",
        "키워드": "",
        "위치정보": "",
        "관련링크": "",
        "첨부PDF명": "",
        "첨부PDF링크": "",
        "소재파일명": "",
        "소재파일링크": "",
    }
    try:
        # dt/dd 쌍에서 라벨별로 dd 내용 수집 (키워드 등은 복수 항목을 공백/쉼표로 합침)
        result = await page.evaluate("""
            () => {
                const out = { "商品・サービス": "", "ビジネスカテゴリ": "", "キーワード": "", "位置情報": "", "ダウンロード": "", "ダウンロードURL": "" };
                const dts = document.querySelectorAll("dt");
                for (const dt of dts) {
                    const key = (dt.innerText || "").trim();
                    let dd = dt.nextElementSibling;
                    if (!dd || dd.tagName !== "DD") dd = dt.parentElement?.querySelector("dd");
                    if (!dd) continue;
                    const text = (dd.innerText || "").trim().replace(/\\s+/g, " ");
                    const firstLink = dd.querySelector("a");
                    const href = firstLink ? (firstLink.getAttribute("href") || "") : "";
                    const fullUrl = href && href.startsWith("http") ? href : (href ? "https://prtimes.jp" + href : "");
                    if (key.indexOf("商品") !== -1 && key.indexOf("サービス") !== -1) out["商品・サービス"] = text;
                    else if (key.indexOf("ビジネスカテゴリ") !== -1) out["ビジネスカテゴリ"] = text;
                    else if (key.indexOf("キーワード") !== -1) out["キーワード"] = text;
                    else if (key.indexOf("位置情報") !== -1) out["位置情報"] = text;
                    else if (key.indexOf("ダウンロード") !== -1) { out["ダウンロード"] = text; out["ダウンロードURL"] = fullUrl; }
                }
                return out;
            }
        """)
        if result:
            data["개요"] = (result.get("商品・サービス") or "").strip()
            data["비즈니스카테고리"] = (result.get("ビジネスカテゴリ") or "").strip()
            data["키워드"] = (result.get("キーワード") or "").strip()
            data["위치정보"] = (result.get("位置情報") or "").strip()
            data["소재파일명"] = (result.get("ダウンロード") or "").strip()
            data["소재파일링크"] = (result.get("ダウンロードURL") or "").strip()
    except Exception:
        pass
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

            detail_page = await context.new_page()
            try:
                await detail_page.goto(art["link"], wait_until="domcontentloaded", timeout=20000)
            except Exception:
                await detail_page.close()
                results.append({
                    "일어 기사 제목": art["title_jp"],
                    "기사 링크": art["link"],
                    "게재 일시": art["time"],
                    "회사명(원문)": art["comp_jp"],
                    "회사 프로필 링크": art["comp_link"],
                    "개요": "", "비즈니스카테고리": "", "키워드": "", "위치정보": "", "관련링크": "",
                    "첨부PDF명": "", "첨부PDF링크": "", "소재파일명": "", "소재파일링크": "",
                    "업종": "", "본사 주소": "", "전화번호": "", "대표자명": "",
                    "상장 여부": "", "자본금": "", "설립일": "", "공식 URL": "",
                    "SNS X": "", "SNS Facebook": "", "SNS YouTube": "",
                    "이메일": "", "문의 웹사이트 URL": "",
                })
                continue

            body_text = await detail_page.inner_text("body")
            email, website = _extract_email_and_website(body_text)
            company_profile = await extract_company_profile(detail_page)
            category_section = await extract_article_category(detail_page)
            pub_time = await _extract_publish_time_from_og_description(detail_page)
            await detail_page.close()

            record = {
                "일어 기사 제목": art["title_jp"],
                "기사 링크": art["link"],
                "게재 일시": pub_time if pub_time else art["time"],
                "회사명(원문)": art["comp_jp"],
                "회사 프로필 링크": art["comp_link"],
                **category_section,
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
