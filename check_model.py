\"\"\"OpenAI 연결 테스트용 스크립트.

과거 Gemini 모델 확인용 스크립트가 남아있어 GitHub Actions에서 혼선을 줄 수 있어,
OpenAI 기반으로 교체했습니다.
\"\"\"

import os


def smoke_test_openai(model: str = \"gpt-4o-mini\") -> str:
    api_key = os.environ.get(\"OPENAI_API_KEY\")
    if not api_key:
        raise RuntimeError(\"OPENAI_API_KEY 환경변수가 필요합니다.\")

    from openai import OpenAI

    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[{\"role\": \"user\", \"content\": \"Reply with exactly: ok\"}],
    )
    return (resp.choices[0].message.content or \"\").strip()


if __name__ == \"__main__\":
    print(smoke_test_openai())