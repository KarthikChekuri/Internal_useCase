"""
tests/utils/test_strategy_loader.py — Tests for app/utils/strategy_loader.py

TDD: These tests are written BEFORE the production code. Each test maps
directly to a Given/When/Then scenario from the strategy-system spec.

Valid PII field names (from master_data table):
  Fullname, FirstName, LastName, DOB, SSN, DriversLicense,
  Address1, Address2, Address3, ZipCode, City, State, Country
"""
import os
import tempfile
import textwrap
import pytest

# ---------------------------------------------------------------------------
# Helper — write a temp YAML file and return its path
# ---------------------------------------------------------------------------

def _write_yaml(content: str) -> str:
    """Write content to a temp file and return the path."""
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8"
    )
    f.write(textwrap.dedent(content))
    f.close()
    return f.name


# ---------------------------------------------------------------------------
# Scenario: Load default strategy set — valid YAML loading
# ---------------------------------------------------------------------------

class TestLoadStrategiesValidYaml:
    """load_strategies() correctly reads a valid YAML file and returns Strategy objects."""

    def test_load_returns_list_of_strategies(self, tmp_path):
        """WHEN a valid strategies YAML is provided THEN a list of Strategy objects is returned."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies:
              - name: fullname_ssn
                description: Search by full name and SSN
                fields:
                  - Fullname
                  - SSN
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies, Strategy
        result = load_strategies(str(yaml_file))
        assert isinstance(result, list)
        assert len(result) == 1

    def test_load_returns_strategy_objects(self, tmp_path):
        """WHEN valid YAML is loaded THEN each item is a Strategy instance."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies:
              - name: fullname_ssn
                description: Search by full name and SSN
                fields:
                  - Fullname
                  - SSN
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies, Strategy
        result = load_strategies(str(yaml_file))
        assert isinstance(result[0], Strategy)

    def test_strategy_has_correct_name(self, tmp_path):
        """WHEN a strategy is loaded THEN it has the correct name attribute."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies:
              - name: fullname_ssn
                description: Search by full name and SSN
                fields:
                  - Fullname
                  - SSN
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        result = load_strategies(str(yaml_file))
        assert result[0].name == "fullname_ssn"

    def test_strategy_has_correct_description(self, tmp_path):
        """WHEN a strategy is loaded THEN it has the correct description attribute."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies:
              - name: fullname_ssn
                description: Search by full name and SSN
                fields:
                  - Fullname
                  - SSN
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        result = load_strategies(str(yaml_file))
        assert result[0].description == "Search by full name and SSN"

    def test_strategy_has_correct_fields(self, tmp_path):
        """WHEN a strategy is loaded THEN it has the correct fields list."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies:
              - name: fullname_ssn
                description: Search by full name and SSN
                fields:
                  - Fullname
                  - SSN
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        result = load_strategies(str(yaml_file))
        assert result[0].fields == ["Fullname", "SSN"]

    def test_load_multiple_strategies(self, tmp_path):
        """WHEN YAML contains multiple strategies THEN all are loaded."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies:
              - name: fullname_ssn
                description: Search by full name and SSN
                fields:
                  - Fullname
                  - SSN
              - name: lastname_dob
                description: Search by last name and DOB
                fields:
                  - LastName
                  - DOB
              - name: unique_identifiers
                description: Search by unique identifiers
                fields:
                  - SSN
                  - DriversLicense
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        result = load_strategies(str(yaml_file))
        assert len(result) == 3
        assert result[0].name == "fullname_ssn"
        assert result[1].name == "lastname_dob"
        assert result[2].name == "unique_identifiers"


# ---------------------------------------------------------------------------
# Scenario: Default strategy set contents
# ---------------------------------------------------------------------------

class TestDefaultStrategiesYaml:
    """The default strategies.yaml in project root must contain the three required strategies."""

    def test_default_yaml_loads_three_strategies(self):
        """WHEN the default strategies.yaml is loaded THEN it contains exactly 3 strategies."""
        project_root = os.path.join(os.path.dirname(__file__), "..", "..")
        yaml_path = os.path.join(project_root, "strategies.yaml")
        from app.utils.strategy_loader import load_strategies
        result = load_strategies(yaml_path)
        assert len(result) == 3

    def test_default_yaml_has_fullname_ssn(self):
        """WHEN default strategies.yaml is loaded THEN fullname_ssn strategy exists with Fullname, SSN."""
        project_root = os.path.join(os.path.dirname(__file__), "..", "..")
        yaml_path = os.path.join(project_root, "strategies.yaml")
        from app.utils.strategy_loader import load_strategies
        strategies = {s.name: s for s in load_strategies(yaml_path)}
        assert "fullname_ssn" in strategies
        assert strategies["fullname_ssn"].fields == ["Fullname", "SSN"]

    def test_default_yaml_has_lastname_dob(self):
        """WHEN default strategies.yaml is loaded THEN lastname_dob strategy exists with LastName, DOB."""
        project_root = os.path.join(os.path.dirname(__file__), "..", "..")
        yaml_path = os.path.join(project_root, "strategies.yaml")
        from app.utils.strategy_loader import load_strategies
        strategies = {s.name: s for s in load_strategies(yaml_path)}
        assert "lastname_dob" in strategies
        assert strategies["lastname_dob"].fields == ["LastName", "DOB"]

    def test_default_yaml_has_unique_identifiers(self):
        """WHEN default strategies.yaml is loaded THEN unique_identifiers strategy exists with SSN, DriversLicense."""
        project_root = os.path.join(os.path.dirname(__file__), "..", "..")
        yaml_path = os.path.join(project_root, "strategies.yaml")
        from app.utils.strategy_loader import load_strategies
        strategies = {s.name: s for s in load_strategies(yaml_path)}
        assert "unique_identifiers" in strategies
        assert strategies["unique_identifiers"].fields == ["SSN", "DriversLicense"]


# ---------------------------------------------------------------------------
# Scenario: Strategy with all valid fields
# ---------------------------------------------------------------------------

class TestAllValidFields:
    """load_strategies() accepts all 13 valid PII field names."""

    def test_all_valid_field_names_accepted(self, tmp_path):
        """WHEN a strategy references all 13 valid field names THEN no error is raised."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies:
              - name: all_fields
                description: Uses every valid PII field
                fields:
                  - Fullname
                  - FirstName
                  - LastName
                  - DOB
                  - SSN
                  - DriversLicense
                  - Address1
                  - Address2
                  - Address3
                  - ZipCode
                  - City
                  - State
                  - Country
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        result = load_strategies(str(yaml_file))
        assert len(result) == 1
        assert len(result[0].fields) == 13


# ---------------------------------------------------------------------------
# Scenario: Strategy references invalid field name
# ---------------------------------------------------------------------------

class TestInvalidFieldName:
    """load_strategies() raises a validation error for unknown field names."""

    def test_invalid_field_raises_value_error(self, tmp_path):
        """WHEN a strategy contains an invalid field name THEN a ValueError is raised."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies:
              - name: bad_strategy
                description: Has invalid field
                fields:
                  - Fullname
                  - InvalidField
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        with pytest.raises(ValueError):
            load_strategies(str(yaml_file))

    def test_invalid_field_error_mentions_invalid_field_name(self, tmp_path):
        """WHEN an invalid field is detected THEN the error message names the invalid field."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies:
              - name: bad_strategy
                description: Has invalid field
                fields:
                  - Fullname
                  - InvalidField
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        with pytest.raises(ValueError, match="InvalidField"):
            load_strategies(str(yaml_file))

    def test_invalid_field_error_mentions_valid_fields(self, tmp_path):
        """WHEN an invalid field is detected THEN the error message lists valid field names."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies:
              - name: bad_strategy
                description: Has invalid field
                fields:
                  - Fullname
                  - BadFieldXYZ
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        with pytest.raises(ValueError, match="Fullname"):
            # Error should mention at least one valid field to guide the user
            load_strategies(str(yaml_file))


# ---------------------------------------------------------------------------
# Scenario: Missing file error
# ---------------------------------------------------------------------------

class TestMissingFile:
    """load_strategies() raises a clear error when the file is missing."""

    def test_missing_file_raises_file_not_found_error(self, tmp_path):
        """WHEN strategies.yaml does not exist THEN FileNotFoundError is raised."""
        missing_path = str(tmp_path / "nonexistent.yaml")
        from app.utils.strategy_loader import load_strategies
        with pytest.raises(FileNotFoundError):
            load_strategies(missing_path)

    def test_missing_file_error_mentions_file_path(self, tmp_path):
        """WHEN strategies.yaml is missing THEN error message contains the file path."""
        missing_path = str(tmp_path / "nonexistent.yaml")
        from app.utils.strategy_loader import load_strategies
        with pytest.raises(FileNotFoundError, match="nonexistent.yaml"):
            load_strategies(missing_path)


# ---------------------------------------------------------------------------
# Scenario: Invalid YAML error
# ---------------------------------------------------------------------------

class TestInvalidYaml:
    """load_strategies() raises a clear error when YAML is malformed."""

    def test_invalid_yaml_raises_value_error(self, tmp_path):
        """WHEN strategies.yaml contains invalid YAML THEN a ValueError is raised."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            "strategies: [unclosed bracket\n  - bad yaml::::\n",
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        with pytest.raises(ValueError):
            load_strategies(str(yaml_file))

    def test_invalid_yaml_error_mentions_file_path(self, tmp_path):
        """WHEN invalid YAML is detected THEN the error message mentions the file path."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            "strategies: [unclosed bracket\n  - bad yaml::::\n",
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        with pytest.raises(ValueError, match="strategies.yaml"):
            load_strategies(str(yaml_file))


# ---------------------------------------------------------------------------
# Scenario: Empty strategies list error
# ---------------------------------------------------------------------------

class TestEmptyStrategiesList:
    """load_strategies() raises an error when the strategies list is empty."""

    def test_empty_strategies_list_raises_value_error(self, tmp_path):
        """WHEN strategies.yaml has an empty strategies list THEN a ValueError is raised."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies: []
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        with pytest.raises(ValueError, match="empty"):
            load_strategies(str(yaml_file))

    def test_missing_strategies_key_raises_value_error(self, tmp_path):
        """WHEN strategies.yaml lacks the 'strategies' key THEN a ValueError is raised."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            configs:
              - name: something
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        with pytest.raises(ValueError):
            load_strategies(str(yaml_file))


# ---------------------------------------------------------------------------
# Scenario: Custom strategy override
# ---------------------------------------------------------------------------

class TestCustomStrategyOverride:
    """load_strategies() supports custom strategy files with any valid content."""

    def test_single_custom_strategy_is_returned(self, tmp_path):
        """WHEN user provides a single custom strategy THEN only that strategy is used."""
        yaml_file = tmp_path / "strategies.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            strategies:
              - name: ssn_only
                description: Search by SSN only
                fields:
                  - SSN
            """),
            encoding="utf-8",
        )
        from app.utils.strategy_loader import load_strategies
        result = load_strategies(str(yaml_file))
        assert len(result) == 1
        assert result[0].name == "ssn_only"
        assert result[0].fields == ["SSN"]
