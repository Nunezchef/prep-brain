from services.argparse_simple import split_command_line


def test_split_command_line_respects_quotes():
    parts = split_command_line('/setname 12 "Braised Ribs"')
    assert parts == ["/setname", "12", "Braised Ribs"]


def test_split_command_line_unbalanced_quotes_fallback():
    parts = split_command_line('/setname 12 "Braised Ribs')
    assert parts[0] == "/setname"
    assert "12" in parts
