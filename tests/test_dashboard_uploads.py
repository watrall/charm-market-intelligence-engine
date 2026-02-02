from dashboard.app import sanitize_upload_name


def test_sanitize_upload_name_strips_paths():
    assert sanitize_upload_name("../../etc/passwd") == "passwd"


def test_sanitize_upload_name_normalizes_characters():
    assert sanitize_upload_name("weird name@#.pdf") == "weird_name_.pdf"
