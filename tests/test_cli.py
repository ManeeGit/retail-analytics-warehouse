from retail_warehouse.cli import main


def test_empty_status_is_clear_error(tmp_path, capsys):
    assert main(["--root", str(tmp_path), "status"]) == 1
    assert "No published release" in capsys.readouterr().err


def test_fixture_cli(tmp_path, capsys):
    assert main(["fixture", "--output", str(tmp_path)]) == 0
    assert (tmp_path / "initial.csv").is_file()
    assert "initial.csv" in capsys.readouterr().out
