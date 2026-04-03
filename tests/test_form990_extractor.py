from pathlib import Path

from nonprofit_platform.parsers.form990.extractor import Form990Extractor


FIXTURE = Path(__file__).parent / "fixtures" / "form990" / "sample_990.xml"


def test_extracts_core_form_990_fields() -> None:
    xml_bytes = FIXTURE.read_bytes()
    extractor = Form990Extractor()

    extracted = extractor.extract(xml_bytes, object_id="2024_0001")

    assert extracted.ein == "123456789"
    assert extracted.organization_name == "Sample Helping Hands"
    assert extracted.city == "Springfield"
    assert extracted.state == "IL"
    assert extracted.form_type == "990"
    assert extracted.tax_year == 2024
    assert extracted.total_revenue == 1500000.0
    assert extracted.total_expenses == 1325000.0
    assert extracted.total_assets == 4100000.0
    assert extracted.total_liabilities == 725000.0
    assert extracted.net_assets == 3375000.0
    assert extracted.employee_count == 24
    assert extracted.volunteer_count == 180
    assert len(extracted.program_service_accomplishments) == 2
    assert extracted.officers[0]["name"] == "Jane Doe"
