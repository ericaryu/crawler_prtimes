# PR TIMES Beauty 크롤러

오늘자 PR TIMES Beauty 섹션 뉴스를 수집해 CSV로 저장하는 비동기 크롤러입니다.  
로컬 키워드 1차 필터와 Gemini LLM으로 **영업 적합성**을 판단하며, API 1분 15회 제한에 맞춰 자동 쓰로틀링합니다.

## 설치

```bash
pip install -r requirements.txt
playwright install chromium
```

## 실행

```bash
# 영업 적합성 판단 사용 시 환경변수 설정 (선택)
export Gemini_Git_API_Key="your-gemini-api-key"
python prtimes_beauty_crawler.py
```

결과는 실행한 날짜 기준 `prtimes_beauty_YYYY-MM-DD.csv` (예: `prtimes_beauty_2026-02-09.csv`)에 UTF-8 BOM으로 저장됩니다.

## CSV 컬럼

- **기사**: 일어 기사 제목, 한국어 번역, **영업 적합성**, **판단 근거**, 기사 링크, 게재 일시  
- **회사**: 회사명(원문), 회사명(한국어 발음), 회사 프로필 링크  
- **프로필**: 업종, 본사 주소, 전화번호, 대표자명, 상장 여부, 자본금, 설립일, 공식 URL, SNS X, SNS Facebook, SNS YouTube  
- **연락처**: 이메일, 문의 웹사이트 URL  

## 영업 적합성 판단

1. **1차 필터 (로컬)**: 부정 키워드(리콜, 회수, 정정, 사과, 부적합, 중단, 오기, 결산, 인사, 주가) 포함 시 LLM 호출 없이 부적합 처리.  
2. **2차 필터 (Gemini)**: `gemini-1.5-flash`로 뷰티 산업 관련성·긍정적 화제성·영업 활용도를 판단해 `is_suitable` / `reason`을 JSON으로 반환.  
3. **쓰로틀링**: 1분 15회 제한을 지키기 위해 최근 15회 호출 시각을 기록하고, 60초가 지나기 전에는 자동 대기 후 호출합니다.

API 키가 없으면 `영업 적합성`은 비우고 `판단 근거`에 "API Key 없음" 등이 들어갑니다.

## 번역

한국어 번역/발음은 `googletrans`로 수행합니다.  
Ollama 등 로컬 번역을 쓰려면 스크립트 내 `translate_title_async`, `translate_company_async`를 해당 API 호출로 교체하면 됩니다.
