import scripts.insights as insights


def test_openai_compat_rejects_non_http_scheme(monkeypatch):
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setenv("LLM_PROVIDER", "openai_compat")
    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    monkeypatch.setenv("LLM_BASE_URL", "file:///etc/passwd")
    out = insights._llm_call("test")
    assert "Invalid LLM_BASE_URL" in out


def test_hf_inference_rejects_non_http_scheme(monkeypatch):
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setenv("LLM_PROVIDER", "hf_inference")
    monkeypatch.setenv("HF_TOKEN", "dummy")
    monkeypatch.setenv("HF_MODEL", "dummy-model")
    monkeypatch.setenv("HF_INFERENCE_URL", "ftp://example.com/model")
    out = insights._llm_call("test")
    assert "Invalid HF_INFERENCE_URL" in out


def test_openai_compat_rejects_private_host_by_default(monkeypatch):
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setenv("LLM_PROVIDER", "openai_compat")
    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    monkeypatch.setenv("LLM_BASE_URL", "http://127.0.0.1:8080/v1")

    out = insights._llm_call("test")

    assert "private/local hosts are blocked" in out


def test_hf_inference_rejects_private_host_by_default(monkeypatch):
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setenv("LLM_PROVIDER", "hf_inference")
    monkeypatch.setenv("HF_TOKEN", "dummy")
    monkeypatch.setenv("HF_MODEL", "dummy-model")
    monkeypatch.setenv("HF_INFERENCE_URL", "http://10.0.0.5/model")

    out = insights._llm_call("test")

    assert "private/local hosts are blocked" in out


def test_hf_inference_can_allow_private_host_with_override(monkeypatch):
    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"generated_text": "ok"}

    def _fake_post(url, headers=None, json=None, timeout=0):
        return _FakeResponse()

    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setenv("LLM_PROVIDER", "hf_inference")
    monkeypatch.setenv("HF_TOKEN", "dummy")
    monkeypatch.setenv("HF_MODEL", "dummy-model")
    monkeypatch.setenv("HF_INFERENCE_URL", "http://127.0.0.1:8080/model")
    monkeypatch.setenv("ALLOW_PRIVATE_LLM_HOSTS", "true")
    monkeypatch.setattr(insights.requests, "post", _fake_post)

    out = insights._llm_call("test")

    assert out == "ok"
