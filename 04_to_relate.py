# -*- coding: utf-8 -*-
"""
04_to_relate.py
- Google Sheets에서 조건에 맞는 행을 읽어 Relate List에 Organization 등록
- 실행: python 04_to_relate.py
"""

import json
import os
import sys
from urllib.parse import urlparse

import gspread
import requests
from google.oauth2.service_account import Credentials

# --- 설정 ---
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1G7dCOf4NjPwiWCiGkAfirrMmLuzPplU_tCrV8AQYArA")
SHEET_TAB = "Relate_PRtimes"
RELATE_LIST_ID = "ZBUABR"
RELATE_BASE_URL = "https://api.relate.so/v1"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# --- Google Sheets 클라이언트 ---
def get_gspread_client() -> gspread.Client:
    json_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not json_str:
        raise EnvironmentError("환경변수 GOOGLE_SERVICE_ACCOUNT_JSON 이 설정되지 않았습니다.")
    info = json.loads(json_str)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


# --- Relate API 헬퍼 ---
def relate_headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def create_organization(api_key: str, name: str, domain: str | None) -> str:
    """Organization 생성 후 id 반환.
    - 도메인 중복/유효하지 않은 422는 도메인 없이 재시도.
    - 응답 구조: {"id": "...", "name": "...", ...} (flat)
    """
    def _post(payload: dict) -> requests.Response:
        return requests.post(
            f"{RELATE_BASE_URL}/organizations",
            headers=relate_headers(api_key),
            json=payload,
            timeout=30,
        )

    payload: dict = {"name": name}
    if domain:
        payload["domains"] = [domain]

    resp = _post(payload)

    # 도메인 관련 422이면 도메인 없이 재시도
    if resp.status_code == 422 and domain:
        resp = _post({"name": name})

    resp.raise_for_status()
    return resp.json()["id"]


def create_list_entry(api_key: str, org_id: str, list_fields: dict) -> None:
    """List Entry 생성."""
    payload = {
        "entryable_id": org_id,
        "entryable_type": "Organization",
        "list_fields": list_fields,
    }
    resp = requests.post(
        f"{RELATE_BASE_URL}/lists/{RELATE_LIST_ID}/entries",
        headers=relate_headers(api_key),
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()


# --- 유틸 ---
def parse_domain(url: str) -> str | None:
    """URL에서 호스트명만 추출 (없으면 None)."""
    url = url.strip()
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    host = urlparse(url).hostname
    if not host:
        return None
    # www. 제거
    return host.removeprefix("www.")


def col(row: dict, key: str) -> str:
    """행에서 값을 꺼내 문자열로 반환 (없거나 None이면 빈 문자열)."""
    return str(row.get(key, "") or "").strip()


# --- 메인 ---
def main() -> None:
    api_key = os.environ.get("RELATE_API_KEY")
    if not api_key:
        raise EnvironmentError("환경변수 RELATE_API_KEY 가 설정되지 않았습니다.")

    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SHEET_TAB)

    all_values = ws.get_all_values()
    if len(all_values) < 2:
        print("시트에 데이터가 없습니다.")
        return

    headers = all_values[0]

    # Relate_등록여부 / Relate_오류메시지 컬럼 인덱스 확인 (없으면 오류)
    if "Relate_등록여부" not in headers:
        raise ValueError("시트에 'Relate_등록여부' 컬럼이 없습니다. 03_to_sheets.py를 먼저 실행하세요.")
    if "Relate_오류메시지" not in headers:
        raise ValueError("시트에 'Relate_오류메시지' 컬럼이 없습니다. 03_to_sheets.py를 먼저 실행하세요.")

    status_col_idx = headers.index("Relate_등록여부")   # 0-based
    error_col_idx  = headers.index("Relate_오류메시지")  # 0-based

    # gspread는 1-based row 인덱스 (1행 = 헤더)
    success_count = 0
    fail_count = 0
    skip_count = 0

    # 데이터 행 목록 구성
    data_rows = []
    for sheet_row_idx, raw_row in enumerate(all_values[1:], start=2):
        padded = raw_row + [""] * (len(headers) - len(raw_row))
        data_rows.append((sheet_row_idx, dict(zip(headers, padded))))

    # 필터 적용 후 대상 건수 출력
    target_rows = []
    for sheet_row_idx, row in data_rows:
        if col(row, "영업 적합성").lower() != "true":
            continue
        if col(row, "한국 회사 여부") != "비한국":
            continue
        email = col(row, "이메일")
        if not email or "wordpress" in email.lower():
            continue
        if col(row, "Relate_등록여부") != "":
            continue
        target_rows.append((sheet_row_idx, row))

    print(f"필터 통과: {len(target_rows)}건 → Relate 등록 시작")

    for sheet_row_idx, row in target_rows:
        # --- 1. Organization 생성 ---
        name = col(row, "회사명(한국어)") or col(row, "회사명(원문)")
        if not name:
            error_msg = "회사명이 없어 등록 불가"
            ws.update_cell(sheet_row_idx, error_col_idx + 1, error_msg)
            ws.update_cell(sheet_row_idx, status_col_idx + 1, "failed")
            fail_count += 1
            continue

        domain = parse_domain(col(row, "공식 URL"))

        try:
            org_id = create_organization(api_key, name, domain)
        except requests.HTTPError as e:
            error_msg = f"Organization 생성 실패: {e.response.status_code} {e.response.text[:200]}"
            ws.update_cell(sheet_row_idx, error_col_idx + 1, error_msg)
            ws.update_cell(sheet_row_idx, status_col_idx + 1, "failed")
            fail_count += 1
            continue
        except Exception as e:
            error_msg = f"Organization 생성 오류: {e}"
            ws.update_cell(sheet_row_idx, error_col_idx + 1, error_msg)
            ws.update_cell(sheet_row_idx, status_col_idx + 1, "failed")
            fail_count += 1
            continue

        # --- 2. List Entry 생성 ---
        list_fields = {
            "기사제목":  col(row, "일어 기사 제목"),
            "한국어번역": col(row, "한국어 번역"),
            "기사링크":  col(row, "기사 링크"),
            "수집일":    col(row, "게재 일시"),
            "영업근거":  col(row, "판단 근거"),
        }

        try:
            create_list_entry(api_key, org_id, list_fields)
        except requests.HTTPError as e:
            error_msg = f"List Entry 생성 실패: {e.response.status_code} {e.response.text[:200]}"
            ws.update_cell(sheet_row_idx, error_col_idx + 1, error_msg)
            ws.update_cell(sheet_row_idx, status_col_idx + 1, "failed")
            fail_count += 1
            continue
        except Exception as e:
            error_msg = f"List Entry 생성 오류: {e}"
            ws.update_cell(sheet_row_idx, error_col_idx + 1, error_msg)
            ws.update_cell(sheet_row_idx, status_col_idx + 1, "failed")
            fail_count += 1
            continue

        # --- 3. 성공 기록 ---
        ws.update_cell(sheet_row_idx, status_col_idx + 1, "done")
        ws.update_cell(sheet_row_idx, error_col_idx + 1, "")
        success_count += 1

    # --- 5. 완료 출력 ---
    print(f"완료: 성공 {success_count}건, 실패 {fail_count}건")


if __name__ == "__main__":
    main()
