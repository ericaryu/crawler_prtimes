# -*- coding: utf-8 -*-
"""
03_to_sheets.py
- final_{today}.csv를 읽어 Google Sheets에 적재
- 실행: python 03_to_sheets.py final_2026-02-26.csv
"""

import json
import os
import sys

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

# --- 설정 ---
SPREADSHEET_ID = "1G7dCOf4NjPwiWCiGkAfirrMmLuzPplU_tCrV8AQYArA"
SHEET_TAB = "Relate_PRtimes"
LINK_COL = "기사 링크"
EXTRA_COLS = ["Relate_등록여부", "Relate_오류메시지"]
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_client() -> gspread.Client:
    json_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not json_str:
        raise EnvironmentError("환경변수 GOOGLE_SERVICE_ACCOUNT_JSON 이 설정되지 않았습니다.")
    info = json.loads(json_str)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def main(csv_path: str) -> None:
    # 1. final CSV 읽기
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    client = get_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SHEET_TAB)

    existing_values = ws.get_all_values()
    is_empty = len(existing_values) == 0

    # 2. 시트가 비어있으면 헤더 자동 삽입
    if is_empty:
        headers = list(df.columns)
        for col in EXTRA_COLS:
            if col not in headers:
                headers.append(col)
        ws.append_row(headers, value_input_option="USER_ENTERED")
        existing_links: set[str] = set()
        sheet_headers = headers
        header_row_values = headers
    else:
        header_row_values = existing_values[0]
        sheet_headers = header_row_values
        # 3. 기존 시트의 '기사 링크' 컬럼 값으로 중복 체크
        if LINK_COL not in sheet_headers:
            raise ValueError(f"시트에 '{LINK_COL}' 컬럼이 없습니다.")
        link_idx = sheet_headers.index(LINK_COL)
        existing_links = {
            row[link_idx]
            for row in existing_values[1:]
            if len(row) > link_idx and row[link_idx]
        }

    # 5. 'Relate_등록여부', 'Relate_오류메시지' 컬럼이 헤더에 없으면 추가
    cols_added = False
    for col in EXTRA_COLS:
        if col not in sheet_headers:
            sheet_headers.append(col)
            cols_added = True

    if cols_added and not is_empty:
        # 헤더 행 업데이트 (A1 기준으로 헤더만 덮어쓰기)
        ws.update("A1", [sheet_headers], value_input_option="USER_ENTERED")

    # 4. 중복 아닌 신규 행만 필터링
    if LINK_COL not in df.columns:
        raise ValueError(f"CSV에 '{LINK_COL}' 컬럼이 없습니다.")

    new_rows_df = df[~df[LINK_COL].astype(str).isin(existing_links)]

    if new_rows_df.empty:
        print("신규 데이터 없음")
        return

    # sheet_headers 순서에 맞춰 행 구성 (없는 컬럼은 빈 문자열)
    rows_to_append = []
    for _, row in new_rows_df.iterrows():
        sheet_row = []
        for col in sheet_headers:
            if col in df.columns:
                val = row[col]
                sheet_row.append("" if pd.isna(val) else str(val))
            else:
                sheet_row.append("")
        rows_to_append.append(sheet_row)

    # 시작행 계산: 현재 시트 행 수 + 1
    current_row_count = len(ws.get_all_values())
    start_row = current_row_count + 1
    end_row = start_row + len(rows_to_append) - 1

    ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")

    # 6. 완료 출력
    print(f"시작행: {start_row}, 종료행: {end_row}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("사용법: python 03_to_sheets.py final_YYYY-MM-DD.csv", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1])
