from typing import Optional
from pydantic import BaseModel, field_validator, ConfigDict

class BusinessSchema(BaseModel):
    model_config = ConfigDict(extra='ignore', from_attributes=True)

    id: str
    name: str
    status: Optional[str] = None
    accountnumber: Optional[str] = None
    date_registration: Optional[str] = None
    mailing_address: Optional[str] = None
    woman_owned_organization: Optional[bool] = None
    veteran_owned_organization: Optional[bool] = None
    minority_owned_organization: Optional[bool] = None
    org_owned_by_person_s_with: Optional[bool] = None
    organization_is_lgbtqi_owned: Optional[bool] = None
    create_dt: Optional[str] = None
    business_type: Optional[str] = None
    billingstreet: Optional[str] = None
    billingcity: Optional[str] = None
    billingcountry: Optional[str] = None
    billingpostalcode: Optional[str] = None
    billingstate: Optional[str] = None
    business_email_address: Optional[str] = None
    citizenship: Optional[str] = None
    country_formation: Optional[str] = None
    formation_place: Optional[str] = None
    state_or_territory_formation: Optional[str] = None
    reason_for_administrative: Optional[str] = None
    dissolution_date: Optional[str] = None
    category_survey_email_address: Optional[str] = None
    naics_code: Optional[str] = None
    total_authorized_shares: Optional[str] = None
    billing_unit: Optional[str] = None
    annual_report_due_date: Optional[str] = None
    began_transacting_in_ct: Optional[str] = None
    business_name_in_state_country: Optional[str] = None
    sub_status: Optional[str] = None
    mailing_international_address: Optional[str] = None
    date_of_organization_meeting: Optional[str] = None
    geo_location: Optional[str] = None
    naics_sub_code: Optional[str] = None
    office_jurisdiction_address: Optional[str] = None
    office_jurisdiction_2: Optional[str] = None
    mailing_jurisdiction_address: Optional[str] = None
    mailing_jurisdiction_2: Optional[str] = None
    mailing_jurisdiction: Optional[str] = None
    mailing_jurisdiction_1: Optional[str] = None
    mailing_jurisdiction_4: Optional[str] = None
    mailing_jurisdiction_country: Optional[str] = None
    office_jurisdiction: Optional[str] = None
    office_jurisdiction_1: Optional[str] = None
    office_jurisdiction_4: Optional[str] = None
    office_in_jurisdiction_country: Optional[str] = None
    record_address: Optional[str] = None
    records_address_street: Optional[str] = None
    records_address_unit: Optional[str] = None
    records_address_city: Optional[str] = None
    records_address_state: Optional[str] = None
    records_address_zip_code: Optional[str] = None
    records_address_country: Optional[str] = None
    mailing_jurisdiction_3: Optional[str] = None
    office_jurisdiction_3: Optional[str] = None
    mail_jurisdiction: Optional[str] = None

    @field_validator('business_type', mode='before')
    @classmethod
    def normalize_business_type(cls, v):
        if v is None:
            return "UNKNOWN"
        return str(v).upper()