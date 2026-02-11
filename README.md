# PR TIMES Beauty 크롤러

오늘자 PR TIMES Beauty 섹션 뉴스를 수집해 CSV로 저장하는 비동기 크롤러입니다.  

## 설치

```bash
pip install -r requirements.txt
playwright install chromium
```

## 실행

```bash
python prtimes_beauty_today.py
```

결과는 실행한 날짜 기준 `raw_YYYY-MM-DD.csv` (예: `raw_2026-02-09.csv`)에 UTF-8 BOM으로 저장됩니다.

## CSV 컬럼

raw CSV(`raw_YYYY-MM-DD.csv`) 헤더/열 순서는 아래와 같습니다.

1) **기사**: 일어 기사 제목, 기사 링크, 게재 일시  
2) **회사**: 회사명(원문), 회사 프로필 링크  
3) **種類(카테고리)**: 개요(商品・サービス), 비즈니스카테고리, 키워드, 위치정보, 관련링크, 첨부PDF명, 첨부PDF링크, 소재파일명, 소재파일링크  
4) **회사 프로필**: 업종, 본사 주소, 전화번호, 대표자명, 상장 여부, 자본금, 설립일, 공식 URL, SNS X, SNS Facebook, SNS YouTube  
5) **연락처**: 이메일, 문의 웹사이트 URL

전체 컬럼(순서 그대로):

```text
일어 기사 제목, 기사 링크, 게재 일시, 회사명(원문), 회사 프로필 링크,
개요, 비즈니스카테고리, 키워드, 위치정보, 관련링크,
첨부PDF명, 첨부PDF링크, 소재파일명, 소재파일링크,
업종, 본사 주소, 전화번호, 대표자명, 상장 여부, 자본금, 설립일, 공식 URL,
SNS X, SNS Facebook, SNS YouTube,
이메일, 문의 웹사이트 URL
```
