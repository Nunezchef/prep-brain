from pathlib import Path

from prep_brain import config as pb_config


def test_resolve_path_absolute_from_relative(tmp_path, monkeypatch):
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text("memory:\n  db_path: data/custom.db\n", encoding="utf-8")
    monkeypatch.setenv("PREP_BRAIN_CONFIG", str(cfg_path))
    pb_config.reload_config()

    db_path = pb_config.get_db_path()
    assert db_path.is_absolute()
    assert str(db_path).endswith("data/custom.db")


def test_env_override_db_path(monkeypatch, tmp_path):
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text("memory:\n  db_path: data/from_yaml.db\n", encoding="utf-8")
    monkeypatch.setenv("PREP_BRAIN_CONFIG", str(cfg_path))
    monkeypatch.setenv("PREP_BRAIN_DB_PATH", str(tmp_path / "override.db"))
    pb_config.reload_config()

    assert pb_config.get_db_path() == (tmp_path / "override.db").resolve()
