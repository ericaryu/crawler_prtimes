# -*- coding: utf-8 -*-
"""
04_to_relate.py
- Google Sheets에서 조건에 맞는 행을 읽어 Relate에 Organization upsert + List entry upsert
- 실행: python 04_to_relate.py
"""

import json
import os
from urllib.parse import urlparse

import gspread
import requests
from google.oauth2.service_account import Credentials

# --- 설정 ---
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1G7dCOf4NjPwiWCiGkAfirrMmLuzPplU_tCrV8AQYArA")
SHEET_TAB = "Relate_PRtimes"
RELATE_LIST_ID = "ZBUABR"
RELATE_BASE_URL = "https://api.relate.so/v1"

# Organization에 저장할 커스텀 필드명
ORG_CUSTOM_FIELD_NAMES = ["이메일", "기사(원문)", "기사(한국어)", "기사(링크)", "회사명(한국어)"]
# List entry에 저장할 필드명 + data_type
LIST_FIELD_DEFS = [
    {"name": "기사(원문)",   "data_type": "text"},
    {"name": "기사(한국어)", "data_type": "text"},
    {"name": "기사(링크)",   "data_type": "url"},
]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ── Google Sheets ────────────────────────────────────────────
def get_gspread_client() -> gspread.Client:
    json_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not json_str:
        raise EnvironmentError("환경변수 GOOGLE_SERVICE_ACCOUNT_JSON 이 설정되지 않았습니다.")
    creds = Credentials.from_service_account_info(json.loads(json_str), scopes=SCOPES)
    return gspread.authorize(creds)


# ── Relate 공통 ───────────────────────────────────────────────
def rh(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def col(row: dict, key: str) -> str:
    return str(row.get(key, "") or "").strip()


def parse_domain(url: str) -> str | None:
    url = url.strip()
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    host = urlparse(url).hostname
    return host.removeprefix("www.") if host else None


# ── 초기화: 커스텀 필드 / List 필드 확보 ─────────────────────
def ensure_org_custom_fields(api_key: str) -> None:
    """Organization 커스텀 필드 없으면 API로 생성."""
    r = requests.get(f"{RELATE_BASE_URL}/custom_fields", headers=rh(api_key), timeout=15)
    r.raise_for_status()
    existing = {f["name"] for f in r.json()["data"] if f["model"] == "organization"}
    for name in ORG_CUSTOM_FIELD_NAMES:
        if name not in existing:
            r2 = requests.post(f"{RELATE_BASE_URL}/custom_fields", headers=rh(api_key),
                json={"name": name, "model": "organization", "data_type": "text"}, timeout=15)
            status = "생성" if r2.ok else f"실패({r2.status_code})"
            print(f"  [Org 커스텀필드 {status}] {name}")


def ensure_list_fields(api_key: str) -> None:
    """List에 필요한 필드 없으면 PATCH로 추가."""
    r = requests.get(f"{RELATE_BASE_URL}/lists/{RELATE_LIST_ID}", headers=rh(api_key), timeout=15)
    r.raise_for_status()
    existing_names = {f["name"] for f in r.json().get("fields", [])}
    missing = [f for f in LIST_FIELD_DEFS if f["name"] not in existing_names]
    if missing:
        r2 = requests.patch(f"{RELATE_BASE_URL}/lists/{RELATE_LIST_ID}", headers=rh(api_key),
            json={"fields": LIST_FIELD_DEFS}, timeout=15)
        status = "추가 완료" if r2.ok else f"실패({r2.status_code})"
        print(f"  [List 필드 {status}] {[f['name'] for f in missing]}")


# ── 기존 데이터 로드 ──────────────────────────────────────────
def build_existing_map(api_key: str) -> dict[str, tuple[str, str]]:
    """
    현재 List의 모든 entry를 순회해 {org_name: (org_id, entry_id)} 맵 구성.
    org_id → org name은 개별 GET으로 조회.
    """
    h = rh(api_key)
    entries: list[dict] = []
    after = 0
    while True:
        r = requests.get(f"{RELATE_BASE_URL}/lists/{RELATE_LIST_ID}/entries",
            headers=h, params={"first": 100, "after": after}, timeout=15)
        r.raise_for_status()
        data = r.json()
        entries.extend(data["data"])
        if not data["pagination"]["has_next_page"]:
            break
        after = data["pagination"]["end_cursor"]

    org_map: dict[str, tuple[str, str]] = {}
    for e in entries:
        org_id = e["entryable_id"]
        entry_id = e["id"]
        r2 = requests.get(f"{RELATE_BASE_URL}/organizations/{org_id}", headers=h, timeout=10)
        if r2.ok:
            org_map[r2.json()["name"]] = (org_id, entry_id)

    return org_map


# ── Organization upsert ───────────────────────────────────────
def upsert_organization(
    api_key: str,
    name: str,
    custom_fields: list,
    domain: str | None,
    existing_org_id: str | None,
) -> tuple[str, str]:
    """
    (org_id, action) 반환. action = 'created' | 'updated'.
    도메인 422 시 도메인 제외 후 재시도.
    """
    h = rh(api_key)
    payload: dict = {"custom_fields": custom_fields}
    if domain:
        payload["domains"] = [domain]

    def _post_with_fallback(pl: dict) -> requests.Response:
        r = requests.post(f"{RELATE_BASE_URL}/organizations",
            headers=h, json={**pl, "name": name}, timeout=30)
        if r.status_code == 422 and domain:
            pl2 = {k: v for k, v in pl.items() if k != "domains"}
            r = requests.post(f"{RELATE_BASE_URL}/organizations",
                headers=h, json={**pl2, "name": name}, timeout=30)
        return r

    def _patch_with_fallback(org_id: str, pl: dict) -> requests.Response:
        r = requests.patch(f"{RELATE_BASE_URL}/organizations/{org_id}",
            headers=h, json=pl, timeout=30)
        if r.status_code == 422 and domain:
            pl2 = {k: v for k, v in pl.items() if k != "domains"}
            r = requests.patch(f"{RELATE_BASE_URL}/organizations/{org_id}",
                headers=h, json=pl2, timeout=30)
        return r

    if existing_org_id:
        resp = _patch_with_fallback(existing_org_id, payload)
        resp.raise_for_status()
        return existing_org_id, "updated"
    else:
        resp = _post_with_fallback(payload)
        resp.raise_for_status()
        return resp.json()["id"], "created"


# ── List entry upsert ─────────────────────────────────────────
def upsert_list_entry(
    api_key: str,
    org_id: str,
    list_fields: list,
    existing_entry_id: str | None,
) -> str:
    """action = 'created' | 'updated' 반환."""
    h = rh(api_key)
    if existing_entry_id:
        r = requests.patch(
            f"{RELATE_BASE_URL}/lists/{RELATE_LIST_ID}/entries/{existing_entry_id}",
            headers=h, json={"list_fields": list_fields}, timeout=30)
        r.raise_for_status()
        return "updated"
    else:
        r = requests.post(
            f"{RELATE_BASE_URL}/lists/{RELATE_LIST_ID}/entries",
            headers=h,
            json={"entryable_id": org_id, "entryable_type": "Organization",
                  "list_fields": list_fields},
            timeout=30)
        r.raise_for_status()
        return "created"


# ── 메인 ──────────────────────────────────────────────────────
def main() -> None:
    api_key = os.environ.get("RELATE_API_KEY")
    if not api_key:
        raise EnvironmentError("환경변수 RELATE_API_KEY 가 설정되지 않았습니다.")

    # 초기화
    print("=== 초기화 ===")
    ensure_org_custom_fields(api_key)
    ensure_list_fields(api_key)

    # 기존 List entry 맵 구성 (org_name → (org_id, entry_id))
    print("기존 List entries 로딩 중...")
    existing_map = build_existing_map(api_key)
    print(f"  기존 등록: {len(existing_map)}건")

    # Sheets 로드
    client = get_gspread_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_TAB)
    all_values = ws.get_all_values()
    if len(all_values) < 2:
        print("시트에 데이터가 없습니다.")
        return

    headers = all_values[0]
    if "Relate_등록여부" not in headers or "Relate_오류메시지" not in headers:
        raise ValueError("Relate_등록여부 / Relate_오류메시지 컬럼 없음. 03_to_sheets.py 먼저 실행하세요.")

    status_idx = headers.index("Relate_등록여부")
    error_idx  = headers.index("Relate_오류메시지")

    # 필터링
    target_rows: list[tuple[int, dict]] = []
    skip_count = 0
    for i, raw_row in enumerate(all_values[1:], start=2):
        padded = raw_row + [""] * (len(headers) - len(raw_row))
        row = dict(zip(headers, padded))
        if col(row, "영업 적합성").lower() != "true":
            continue
        if col(row, "한국 회사 여부") != "비한국":
            continue
        email = col(row, "이메일")
        if not email or "wordpress" in email.lower():
            continue
        if col(row, "Relate_등록여부") != "":
            skip_count += 1
            continue
        target_rows.append((i, row))

    print()
    print(f"=== 처리 시작: {len(target_rows)}건 (스킵 {skip_count}건) ===")
    print()

    success_count = fail_count = 0

    for sheet_row_idx, row in target_rows:
        name = col(row, "회사명(원문)")
        if not name:
            msg = "회사명(원문) 없음"
            print(f"  [행 {sheet_row_idx}] FAIL: {msg}")
            ws.update_cell(sheet_row_idx, error_idx + 1, msg)
            ws.update_cell(sheet_row_idx, status_idx + 1, "failed")
            fail_count += 1
            continue

        domain = parse_domain(col(row, "공식 URL"))
        existing_org_id, existing_entry_id = (
            (existing_map[name][0], existing_map[name][1])
            if name in existing_map else (None, None)
        )

        org_custom_fields = [
            {"name": "이메일",         "value": col(row, "이메일")},
            {"name": "기사(원문)",     "value": col(row, "일어 기사 제목")},
            {"name": "기사(한국어)",   "value": col(row, "한국어 번역")},
            {"name": "기사(링크)",     "value": col(row, "기사 링크")},
            {"name": "회사명(한국어)", "value": col(row, "회사명(한국어)")},
        ]
        list_fields = [
            {"name": "기사(원문)",   "value": col(row, "일어 기사 제목")},
            {"name": "기사(한국어)", "value": col(row, "한국어 번역")},
            {"name": "기사(링크)",   "value": col(row, "기사 링크")},
        ]

        # 1. Organization upsert
        try:
            org_id, org_action = upsert_organization(
                api_key, name, org_custom_fields, domain, existing_org_id)
            print(f"  [행 {sheet_row_idx}] Org {org_action}: {name} ({org_id})")
        except requests.HTTPError as e:
            msg = f"Org 실패: {e.response.status_code} {e.response.text[:150]}"
            print(f"  [행 {sheet_row_idx}] FAIL — {msg}")
            ws.update_cell(sheet_row_idx, error_idx + 1, msg)
            ws.update_cell(sheet_row_idx, status_idx + 1, "failed")
            fail_count += 1
            continue
        except Exception as e:
            msg = f"Org 오류: {e}"
            print(f"  [행 {sheet_row_idx}] FAIL — {msg}")
            ws.update_cell(sheet_row_idx, error_idx + 1, msg)
            ws.update_cell(sheet_row_idx, status_idx + 1, "failed")
            fail_count += 1
            continue

        # 2. List entry upsert
        try:
            entry_action = upsert_list_entry(
                api_key, org_id, list_fields, existing_entry_id)
            print(f"  [행 {sheet_row_idx}] List entry {entry_action}")
        except requests.HTTPError as e:
            msg = f"List entry 실패: {e.response.status_code} {e.response.text[:150]}"
            print(f"  [행 {sheet_row_idx}] FAIL — {msg}")
            ws.update_cell(sheet_row_idx, error_idx + 1, msg)
            ws.update_cell(sheet_row_idx, status_idx + 1, "failed")
            fail_count += 1
            continue
        except Exception as e:
            msg = f"List entry 오류: {e}"
            print(f"  [행 {sheet_row_idx}] FAIL — {msg}")
            ws.update_cell(sheet_row_idx, error_idx + 1, msg)
            ws.update_cell(sheet_row_idx, status_idx + 1, "failed")
            fail_count += 1
            continue

        ws.update_cell(sheet_row_idx, status_idx + 1, "done")
        ws.update_cell(sheet_row_idx, error_idx + 1, "")
        success_count += 1

    print()
    print(f"=== 완료: 성공 {success_count}건 / 실패 {fail_count}건 / 스킵 {skip_count}건 ===")


if __name__ == "__main__":
    main()
