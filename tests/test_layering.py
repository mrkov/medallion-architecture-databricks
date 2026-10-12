import re


def dependencies(source: str) -> set[str]:
    return set(re.findall(r'spark\.read\.table\(table_name\("([a-z]+)"', source))


def test_silver_reads_only_bronze(repository_root):
    source = (repository_root / "src/pipelines/silver.py").read_text()
    assert dependencies(source) == {"bronze"}


def test_gold_reads_only_silver(repository_root):
    source = (repository_root / "src/pipelines/gold.py").read_text()
    assert dependencies(source) == {"silver"}


def test_platinum_reads_only_gold(repository_root):
    source = (repository_root / "src/pipelines/platinum.py").read_text()
    assert dependencies(source) == {"gold"}


def test_bronze_is_append_only(repository_root):
    source = (repository_root / "src/pipelines/bronze.py").read_text()
    assert "read_landing" in source
    assert not re.search(r"\b(delete|merge|overwrite|update)\b", source, flags=re.IGNORECASE)


def test_unity_catalog_file_lineage_uses_supported_metadata(repository_root):
    source = (repository_root / "src/pipelines/pipeline_common.py").read_text()
    assert "_metadata.file_path" in source
    assert "input_file_name" not in source

