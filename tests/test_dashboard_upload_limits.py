import sys
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard import app


class FakeUpload:
    def __init__(self, name: str, size: int):
        self.name = name
        self.size = size
        self._buf = BytesIO(b"a" * size)

    def getbuffer(self):
        return self._buf.getbuffer()


def test_save_uploads_enforces_max_size(tmp_path):
    uploads = [
        FakeUpload("too-big.pdf", app.MAX_UPLOAD_BYTES + 1),
        FakeUpload("ok.pdf", 1024),
    ]

    saved, skipped = app.save_uploads(uploads, tmp_path, max_bytes=app.MAX_UPLOAD_BYTES)

    assert saved == 1
    assert skipped and skipped[0][0] == "too-big.pdf"
    assert (tmp_path / "ok.pdf").exists()
