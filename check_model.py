async def judge_news_suitability(title_jp: str, title_ko: str) -> tuple:
    """영업 적합성 판단 및 모델 리스트 디버깅"""
    # 1. 로컬 필터링 (일본어 제목 기준)
    is_passed, local_reason = local_first_filter(title_jp) 
    if not is_passed:
        return False, local_reason

    if not GEMINI_API_KEY:
        return None, "API Key 없음"

    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        
        # [중요] 사용 가능한 모델 리스트를 로그에 출력합니다.
        # 처음 한 번만 실행되거나 에러 시 출력되도록 설정
        print("--- 현재 API 키로 사용 가능한 모델 목록 ---")
        for m in genai.list_models():
            if 'generateContent' in m.supported_generation_methods:
                print(f"사용 가능 모델명: {m.name}")
        
        # 모델 선언 (가장 표준적인 이름을 시도합니다)
        model = genai.GenerativeModel("models/gemini-1.5-flash")
        
        prompt = f"다음 뉴스 제목이 영업에 적합한지 JSON으로 판단해줘: {title_jp} ({title_ko})"
        
        await _wait_gemini_rate_limit()
        response = await model.generate_content_async(prompt)
        await _record_gemini_call()
        
        result_text = (response.text or "").replace("```json", "").replace("```", "").strip()
        result_json = json.loads(result_text)
        return result_json.get("is_suitable", False), result_json.get("reason", "")
        
    except Exception as e:
        print(f"--- Gemini API 상세 에러: {str(e)} ---")
        return False, f"API 오류: {str(e)}"