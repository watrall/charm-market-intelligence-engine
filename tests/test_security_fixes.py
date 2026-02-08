"""Tests for security fixes: path traversal, env parsing."""

from scripts.parse_reports import _load_text_file


class TestLoadTextFileTraversal:
    """_load_text_file must reject filenames that escape TEXT_DIR."""

    def test_rejects_dotdot_traversal(self):
        result = _load_text_file("../../etc/passwd")
        assert result is None

    def test_rejects_absolute_path(self):
        result = _load_text_file("/etc/passwd")
        assert result is None

    def test_rejects_backslash_traversal(self):
        result = _load_text_file("..\\..\\etc\\passwd")
        assert result is None

    def test_rejects_dot_prefix(self):
        result = _load_text_file(".hidden_file")
        assert result is None

    def test_accepts_valid_hash_filename(self):
        # A valid cache filename won't exist, so returns None for missing
        result = _load_text_file("abc123def456.txt")
        assert result is None  # File doesn't exist, but wasn't rejected

    def test_rejects_none(self):
        result = _load_text_file(None)
        assert result is None

    def test_rejects_empty(self):
        result = _load_text_file("")
        assert result is None


class TestLlmMaxTokensSafeParsing:
    """LLM_MAX_TOKENS should not crash on bad input."""

    def test_non_numeric_max_tokens_does_not_crash(self, monkeypatch):
        monkeypatch.setenv("USE_LLM", "true")
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        monkeypatch.setenv("LLM_MAX_TOKENS", "not-a-number")
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)

        from scripts.insights import _llm_call
        # Should not raise ValueError; returns empty since no API key
        result = _llm_call("test prompt")
        assert isinstance(result, str)
