from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from nonprofit_platform.parsers.form990.xml_utils import (
    as_float,
    as_int,
    find_child_text,
    find_first_text,
    findall_text,
    local_name,
)


UTC = timezone.utc


@dataclass(slots=True)
class ExtractedForm990:
    object_id: str
    return_id: str | None
    ein: str | None
    tax_year: int | None
    filing_year: int | None
    tax_period: str | None
    form_type: str | None
    organization_name: str | None
    address_line_1: str | None
    address_line_2: str | None
    city: str | None
    state: str | None
    zip_code: str | None
    country: str | None
    organization_type: str | None
    deductibility_status: str | None
    public_charity_status: str | None
    total_revenue: float | None
    total_expenses: float | None
    total_assets: float | None
    total_liabilities: float | None
    net_assets: float | None
    mission_text: str | None
    employee_count: int | None
    volunteer_count: int | None
    contributions_revenue: float | None
    program_service_revenue: float | None
    investment_income: float | None
    program_service_accomplishments: list[str]
    officers: list[dict[str, Any]]
    extracted_sections: dict[str, Any]
    narrative_sections: dict[str, Any]
    parser_version: str
    extracted_at: datetime

    def to_record(self) -> dict[str, Any]:
        return self.__dict__.copy()


class Form990Extractor:
    parser_version = "v1"

    def extract(self, xml_bytes: bytes, object_id: str, return_id: str | None = None) -> ExtractedForm990:
        root = ET.fromstring(xml_bytes)
        filing_year = self._extract_filing_year(root)
        officers = self._extract_officers(root)
        accomplishments = self._extract_program_accomplishments(root)
        mission = find_first_text(
            root,
            "ActivityOrMissionDesc",
            "MissionDesc",
            "PrimaryExemptPurposeTxt",
        )
        narratives = {
            "mission": mission,
            "program_accomplishments": accomplishments,
            "desc_sections": findall_text(
                root,
                "Desc",
                "Description",
                "ProgramServiceAccomplishmentsTxt",
            ),
        }
        extracted_sections = {
            "return_header": {
                "tax_period_begin": find_first_text(root, "TaxPeriodBeginDt"),
                "tax_period_end": find_first_text(root, "TaxPeriodEndDt"),
            },
            "financials": {
                "total_revenue": self._extract_amount(root, "CYTotalRevenueAmt", "TotalRevenueCurrentYearAmt"),
                "total_expenses": self._extract_amount(root, "CYTotalExpensesAmt", "TotalExpensesCurrentYearAmt"),
                "total_assets": self._extract_amount(root, "TotalAssetsEOYAmt", "EOYAmt"),
                "total_liabilities": self._extract_amount(root, "TotalLiabilitiesEOYAmt"),
                "net_assets": self._extract_amount(root, "NetAssetsOrFundBalancesEOYAmt"),
            },
        }

        organization_type = find_first_text(root, "Organization501c3Ind", "Organization501cInd")
        if organization_type:
            organization_type = "501(c)(3)" if organization_type.lower() in {"x", "true"} else organization_type

        return ExtractedForm990(
            object_id=object_id,
            return_id=return_id or find_first_text(root, "ReturnTs", "ReturnTypeCd"),
            ein=find_first_text(root, "EIN", "FilerEIN"),
            tax_year=as_int(find_first_text(root, "TaxYr", "TaxYear")),
            filing_year=filing_year,
            tax_period=find_first_text(root, "TaxPeriodEndDt", "TaxPeriodBeginDt"),
            form_type=find_first_text(root, "ReturnTypeCd"),
            organization_name=find_first_text(root, "BusinessNameLine1Txt", "BusinessNameLine1"),
            address_line_1=find_first_text(root, "AddressLine1Txt"),
            address_line_2=find_first_text(root, "AddressLine2Txt"),
            city=find_first_text(root, "CityNm"),
            state=find_first_text(root, "StateAbbreviationCd"),
            zip_code=find_first_text(root, "ZIPCd", "ForeignPostalCd"),
            country=find_first_text(root, "CountryCd", "CountryForeignCd"),
            organization_type=organization_type,
            deductibility_status=find_first_text(root, "DeductibilityStatusDesc", "DeductibilityInd"),
            public_charity_status=find_first_text(root, "PublicCharityStatusTxt", "PublicSupportedOrganizationInd"),
            total_revenue=extracted_sections["financials"]["total_revenue"],
            total_expenses=extracted_sections["financials"]["total_expenses"],
            total_assets=extracted_sections["financials"]["total_assets"],
            total_liabilities=extracted_sections["financials"]["total_liabilities"],
            net_assets=extracted_sections["financials"]["net_assets"],
            mission_text=mission,
            employee_count=as_int(find_first_text(root, "TotalEmployeeCnt", "EmployeeCnt")),
            volunteer_count=as_int(find_first_text(root, "TotalVolunteersCnt", "VolunteerCnt")),
            contributions_revenue=self._extract_amount(root, "CYContributionsGrantsAmt", "ContributionsGrantsAmt"),
            program_service_revenue=self._extract_amount(root, "CYProgramServiceRevenueAmt", "ProgramServiceRevenueAmt"),
            investment_income=self._extract_amount(root, "CYInvestmentIncomeAmt", "InvestmentIncomeAmt"),
            program_service_accomplishments=accomplishments,
            officers=officers,
            extracted_sections=extracted_sections,
            narrative_sections=narratives,
            parser_version=self.parser_version,
            extracted_at=datetime.now(tz=UTC),
        )

    def _extract_amount(self, root: ET.Element, *tags: str) -> float | None:
        return as_float(find_first_text(root, *tags))

    def _extract_program_accomplishments(self, root: ET.Element) -> list[str]:
        values = findall_text(
            root,
            "ProgramServiceAccomplishmentsTxt",
            "Desc",
            "Description",
        )
        unique_values: list[str] = []
        seen: set[str] = set()
        for value in values:
            normalized = value.strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                unique_values.append(normalized)
        return unique_values[:15]

    def _extract_officers(self, root: ET.Element) -> list[dict[str, Any]]:
        officers: list[dict[str, Any]] = []
        for element in root.iter():
            if local_name(element.tag) not in {"Form990PartVIISectionAGrp", "OfficerDirectorTrusteeKeyEmployeeGrp"}:
                continue
            entry = {
                "name": find_child_text(element, "PersonNm"),
                "title": find_child_text(element, "TitleTxt"),
                "average_hours": as_float(find_child_text(element, "AverageHoursPerWeekRt")),
                "reportable_compensation": as_float(find_child_text(element, "ReportableCompFromOrgAmt")),
            }
            if any(value is not None for value in entry.values()):
                officers.append(entry)
        return officers[:25]

    def _extract_filing_year(self, root: ET.Element) -> int | None:
        period_end = find_first_text(root, "TaxPeriodEndDt")
        if period_end and len(period_end) >= 4:
            return as_int(period_end[:4])
        submitted = find_first_text(root, "ReturnTs")
        if submitted and len(submitted) >= 4:
            return as_int(submitted[:4])
        return None
