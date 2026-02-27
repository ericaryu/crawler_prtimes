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
RELATE_LIST_ID = "55Uq1B"
RELATE_BASE_URL = "https://api.relate.so/v1"

# Organization에 저장할 커스텀 필드명
ORG_CUSTOM_FIELD_NAMES = ["이메일", "기사(원문)", "기사(한국어)", "기사(링크)", "회사명(한국어)"]
# List entry에 저장할 필드명 + data_type
LIST_FIELD_DEFS = [
    {"name": "기사(원문)",   "data_type": "text"},
    {"name": "기사(한국어)", "data_type": "text"},
    {"name": "기사(링크)",   "data_type": "url"},
]

# Contact에 저장할 커스텀 필드에서 제외할 시트 컬럼
CONTACT_CUSTOM_FIELD_EXCLUDE = {"Relate_등록여부", "Relate_오류메시지"}

# Contact에 동기화할 "선택 필드" 정의 (기본: text)
# - 시트 컬럼명이 바뀌면 여기만 수정하면 됨
CONTACT_CUSTOM_FIELD_DEFS = [
    {"name": "회사명(원문)", "data_type": "text", "sheet_col": "회사명(원문)"},
    {"name": "회사명(한국어)", "data_type": "text", "sheet_col": "회사명(한국어)"},
    {"name": "게재 일시", "data_type": "text", "sheet_col": "게재 일시"},
    {"name": "기사(원문)", "data_type": "text", "sheet_col": "일어 기사 제목"},
    {"name": "기사(한국어)", "data_type": "text", "sheet_col": "한국어 번역"},
    {"name": "기사(링크)", "data_type": "url", "sheet_col": "기사 링크"},
    {"name": "판단 근거", "data_type": "textarea", "sheet_col": "판단 근거"},
    {"name": "이메일", "data_type": "text", "sheet_col": "이메일"},
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


def ensure_contact_custom_fields(api_key: str, field_defs: list[dict]) -> None:
    """Contact 커스텀 필드 없으면 API로 생성."""
    if not field_defs:
        return
    r = requests.get(f"{RELATE_BASE_URL}/custom_fields", headers=rh(api_key), timeout=15)
    r.raise_for_status()
    existing = {f["name"] for f in r.json()["data"] if f.get("model") == "contact"}
    for fd in field_defs:
        name = str(fd.get("name") or "").strip()
        data_type = str(fd.get("data_type") or "text").strip() or "text"
        if not name or name in existing:
            continue
        r2 = requests.post(
            f"{RELATE_BASE_URL}/custom_fields",
            headers=rh(api_key),
            json={"name": name, "model": "contact", "data_type": data_type},
            timeout=15,
        )
        status = "생성" if r2.ok else f"실패({r2.status_code})"
        print(f"  [Contact 커스텀필드 {status}] {name}")


def try_get_list_meta(api_key: str) -> dict | None:
    """List가 존재하면 메타 반환, 없으면 None."""
    try:
        r = requests.get(f"{RELATE_BASE_URL}/lists/{RELATE_LIST_ID}", headers=rh(api_key), timeout=15)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


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
def build_existing_list_entry_map(api_key: str) -> dict[str, str]:
    """현재 List의 모든 entry를 순회해 {entryable_id: entry_id} 맵 구성."""
    h = rh(api_key)
    entries: list[dict] = []
    after = 0
    while True:
        r = requests.get(
            f"{RELATE_BASE_URL}/lists/{RELATE_LIST_ID}/entries",
            headers=h,
            params={"first": 100, "after": after},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        entries.extend(data.get("data", []))
        if not data.get("pagination", {}).get("has_next_page"):
            break
        after = data["pagination"]["end_cursor"]

    out: dict[str, str] = {}
    for e in entries:
        entryable_id = str(e.get("entryable_id") or "").strip()
        entry_id = str(e.get("id") or "").strip()
        if entryable_id and entry_id:
            out[entryable_id] = entry_id
    return out


def build_existing_org_map_by_name(api_key: str) -> dict[str, str]:
    """전체 Organization을 순회해 {org_name: org_id} 맵 구성."""
    h = rh(api_key)
    orgs: list[dict] = []
    after = 0
    while True:
        r = requests.get(
            f"{RELATE_BASE_URL}/organizations",
            headers=h,
            params={"first": 100, "after": after},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        orgs.extend(data.get("data", []))
        if not data.get("pagination", {}).get("has_next_page"):
            break
        after = data["pagination"]["end_cursor"]

    out: dict[str, str] = {}
    for o in orgs:
        name = str(o.get("name") or "").strip()
        oid = str(o.get("id") or "").strip()
        if name and oid:
            out[name] = oid
    return out


def build_existing_contact_map_by_email(api_key: str) -> dict[str, str]:
    """전체 Contact을 순회해 {email(lower): contact_id} 맵 구성."""
    h = rh(api_key)
    contacts: list[dict] = []
    after = 0
    while True:
        r = requests.get(
            f"{RELATE_BASE_URL}/contacts",
            headers=h,
            params={"first": 100, "after": after},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        contacts.extend(data.get("data", []))
        if not data.get("pagination", {}).get("has_next_page"):
            break
        after = data["pagination"]["end_cursor"]

    out: dict[str, str] = {}
    for c in contacts:
        cid = str(c.get("id") or "").strip()
        emails = c.get("emails") or []
        if not cid or not isinstance(emails, list):
            continue
        for e in emails:
            em = str(e or "").strip().lower()
            if em and em not in out:
                out[em] = cid
    return out


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


# ── Contact upsert ────────────────────────────────────────────
def upsert_contact(
    api_key: str,
    org_id: str,
    email: str,
    custom_fields: list,
    existing_contact_id: str | None,
) -> tuple[str, str]:
    """
    (contact_id, action) 반환. action = 'created' | 'updated'.
    - 기존 contact면 PATCH로 업데이트
    - 신규면 POST로 생성
    """
    h = rh(api_key)
    email = (email or "").strip()
    if not email:
        raise ValueError("이메일이 비어있어 Contact upsert 불가")

    if existing_contact_id:
        payload = {"emails": [email], "custom_fields": custom_fields, "organization_id": org_id}
        r = requests.patch(
            f"{RELATE_BASE_URL}/contacts/{existing_contact_id}",
            headers=h,
            json=payload,
            timeout=30,
        )
        # API가 organization_id 업데이트를 허용하지 않는 경우를 대비해 재시도
        if r.status_code in (400, 401, 403, 422):
            payload2 = {"emails": [email], "custom_fields": custom_fields}
            r2 = requests.patch(
                f"{RELATE_BASE_URL}/contacts/{existing_contact_id}",
                headers=h,
                json=payload2,
                timeout=30,
            )
            r2.raise_for_status()
            return existing_contact_id, "updated"
        r.raise_for_status()
        return existing_contact_id, "updated"
    else:
        payload = {"organization_id": org_id, "emails": [email], "custom_fields": custom_fields}
        r = requests.post(
            f"{RELATE_BASE_URL}/contacts",
            headers=h,
            json=payload,
            timeout=30,
        )
        r.raise_for_status()
        return str(r.json().get("id") or "").strip(), "created"


# ── List entry upsert ─────────────────────────────────────────
def upsert_list_entry(
    api_key: str,
    entryable_id: str,
    entryable_type: str,
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
            json={"entryable_id": entryable_id, "entryable_type": entryable_type,
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
    list_meta = try_get_list_meta(api_key)
    if list_meta:
        try:
            ensure_list_fields(api_key)
        except Exception as e:
            print(f"  [경고] List 필드 확인/추가 실패: {e}")
        print(f"  List 확인: {list_meta.get('name')} (entry_type={list_meta.get('entry_type')}, process={list_meta.get('process')})")
    else:
        print(f"  [경고] Relate list가 없습니다. (RELATE_LIST_ID={RELATE_LIST_ID})")
        print("        list를 다시 만든 뒤 재실행하면 list entry까지 자동으로 등록됩니다.")

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

    # AF열(32번째 컬럼) 값 존재 개수 (Null/빈값 제외)
    af_index = 31  # 0-based
    af_non_null = 0
    for r in all_values[1:]:
        if len(r) > af_index and str(r[af_index] or "").strip() != "":
            af_non_null += 1
    print(f"스프레드시트 AF열(빈값 제외) 값 개수: {af_non_null}")

    # Contact 커스텀필드: 선택한 필드만 생성/보장
    print(f"Contact 커스텀필드 동기화 대상: {len(CONTACT_CUSTOM_FIELD_DEFS)}개 (선택 필드)")
    ensure_contact_custom_fields(api_key, CONTACT_CUSTOM_FIELD_DEFS)

    # 기존 Org/Contact 맵 구성 (list 의존성 제거)
    print("기존 Organizations 로딩 중...")
    existing_org_map = build_existing_org_map_by_name(api_key)
    print(f"  기존 Org: {len(existing_org_map)}건")
    print("기존 Contacts 로딩 중...")
    existing_contact_map = build_existing_contact_map_by_email(api_key)
    print(f"  기존 Contact(email): {len(existing_contact_map)}건")

    # 기존 List entry 맵 구성 (있을 때만)
    existing_entry_map: dict[str, str] = {}
    if list_meta:
        try:
            print("기존 List entries 로딩 중...")
            existing_entry_map = build_existing_list_entry_map(api_key)
            print(f"  기존 List entry: {len(existing_entry_map)}건")
        except Exception as e:
            print(f"  [경고] 기존 List entry 로딩 실패: {e}")

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
    org_created = org_updated = 0
    contact_created = contact_updated = 0
    list_created = list_updated = 0

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
        email = col(row, "이메일")
        existing_org_id = existing_org_map.get(name)
        existing_contact_id = existing_contact_map.get(email.strip().lower()) if email else None

        org_custom_fields = [
            {"name": "이메일",         "value": col(row, "이메일")},
            {"name": "기사(원문)",     "value": col(row, "일어 기사 제목")},
            {"name": "기사(한국어)",   "value": col(row, "한국어 번역")},
            {"name": "기사(링크)",     "value": col(row, "기사 링크")},
            {"name": "회사명(한국어)", "value": col(row, "회사명(한국어)")},
        ]

        # Contact에 선택 필드만 custom_fields로 저장(빈 값은 제외)
        contact_custom_fields: list[dict] = []
        for fd in CONTACT_CUSTOM_FIELD_DEFS:
            cf_name = str(fd.get("name") or "").strip()
            sheet_col = str(fd.get("sheet_col") or "").strip()
            if not cf_name or not sheet_col:
                continue
            v = col(row, sheet_col)
            if v == "":
                continue
            contact_custom_fields.append({"name": cf_name, "value": v})

        # 1. Organization upsert
        try:
            org_id, org_action = upsert_organization(
                api_key, name, org_custom_fields, domain, existing_org_id)
            print(f"  [행 {sheet_row_idx}] Org {org_action}: {name} ({org_id})")
            existing_org_map[name] = org_id
            if org_action == "created":
                org_created += 1
            else:
                org_updated += 1
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

        # 2. Contact upsert (기존이면 업데이트, 없으면 생성)
        if not email:
            msg = "Contact 실패: 이메일 없음"
            print(f"  [행 {sheet_row_idx}] FAIL — {msg}")
            ws.update_cell(sheet_row_idx, error_idx + 1, msg)
            ws.update_cell(sheet_row_idx, status_idx + 1, "failed")
            fail_count += 1
            continue

        try:
            contact_id, contact_action = upsert_contact(
                api_key, org_id, email, contact_custom_fields, existing_contact_id
            )
            print(f"  [행 {sheet_row_idx}] Contact {contact_action}: {email} ({contact_id})")
            existing_contact_map[email.strip().lower()] = contact_id
            if contact_action == "created":
                contact_created += 1
            else:
                contact_updated += 1
        except requests.HTTPError as e:
            msg = f"Contact 실패: {e.response.status_code} {e.response.text[:150]}"
            print(f"  [행 {sheet_row_idx}] FAIL — {msg}")
            ws.update_cell(sheet_row_idx, error_idx + 1, msg)
            ws.update_cell(sheet_row_idx, status_idx + 1, "failed")
            fail_count += 1
            continue
        except Exception as e:
            msg = f"Contact 오류: {e}"
            print(f"  [행 {sheet_row_idx}] FAIL — {msg}")
            ws.update_cell(sheet_row_idx, error_idx + 1, msg)
            ws.update_cell(sheet_row_idx, status_idx + 1, "failed")
            fail_count += 1
            continue

        # 3. List entry upsert (list가 있을 때만)
        if list_meta:
            entryable_type = str(list_meta.get("entry_type") or "").strip() or "Organization"
            entryable_id = org_id if entryable_type == "Organization" else contact_id
            existing_entry_id = existing_entry_map.get(entryable_id)

            # 과거에 list_fields에 Organization name을 넣던 로직은 제거하고,
            # list에 정의된 기사 필드만 업데이트
            list_fields = [
                {"name": "기사(원문)",   "value": col(row, "일어 기사 제목")},
                {"name": "기사(한국어)", "value": col(row, "한국어 번역")},
                {"name": "기사(링크)",   "value": col(row, "기사 링크")},
            ]

            try:
                entry_action = upsert_list_entry(
                    api_key, entryable_id, entryable_type, list_fields, existing_entry_id
                )
                print(f"  [행 {sheet_row_idx}] List entry {entry_action} ({entryable_type})")
                if entry_action == "created":
                    list_created += 1
                else:
                    list_updated += 1
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
    print(f"=== 등록 요약 ===")
    print(f"  Org: created {org_created}, updated {org_updated}")
    print(f"  Contact: created {contact_created}, updated {contact_updated}")
    print(f"  List entry: created {list_created}, updated {list_updated}")


if __name__ == "__main__":
    main()
