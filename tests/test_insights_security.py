from scripts.insights import _llm_call


def test_openai_compat_rejects_non_http_scheme(monkeypatch):
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setenv("LLM_PROVIDER", "openai_compat")
    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    monkeypatch.setenv("LLM_BASE_URL", "file:///etc/passwd")
    out = _llm_call("test")
    assert "Invalid LLM_BASE_URL" in out


def test_hf_inference_rejects_non_http_scheme(monkeypatch):
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setenv("LLM_PROVIDER", "hf_inference")
    monkeypatch.setenv("HF_TOKEN", "dummy")
    monkeypatch.setenv("HF_MODEL", "dummy-model")
    monkeypatch.setenv("HF_INFERENCE_URL", "ftp://example.com/model")
    out = _llm_call("test")
    assert "Invalid HF_INFERENCE_URL" in out
