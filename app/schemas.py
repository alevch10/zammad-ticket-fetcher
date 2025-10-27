from pydantic import (
    BaseModel,
    field_validator,
    model_validator,
)  # Updated: field_validator for v2, model_validator for cross-field
from typing import Optional, List, Dict, Any
from datetime import datetime
import re


# Schema for Ticket Search Response from Zammad API
# All fields optional as per request, to handle partial/incomplete API responses gracefully
class TicketRecord(BaseModel):
    id: Optional[int] = None
    group_id: Optional[int] = None
    priority_id: Optional[int] = None
    state_id: Optional[int] = None  # Used as 'state' in CSV
    organization_id: Optional[int] = None
    number: Optional[str] = None
    title: Optional[str] = None  # Ignore specific title as per task
    owner_id: Optional[int] = None
    customer_id: Optional[int] = None
    note: Optional[str] = None
    first_response_at: Optional[str] = None
    first_response_escalation_at: Optional[str] = None
    first_response_in_min: Optional[int] = None
    first_response_diff_in_min: Optional[int] = None
    close_at: Optional[str] = None
    close_escalation_at: Optional[str] = None
    close_in_min: Optional[int] = None
    close_diff_in_min: Optional[int] = None
    update_escalation_at: Optional[str] = None
    update_in_min: Optional[int] = None
    update_diff_in_min: Optional[int] = None
    last_close_at: Optional[str] = None
    last_contact_at: Optional[str] = None
    last_contact_agent_at: Optional[str] = None
    last_contact_customer_at: Optional[str] = None
    last_owner_update_at: Optional[str] = None
    create_article_type_id: Optional[int] = None
    create_article_sender_id: Optional[int] = None
    article_count: Optional[int] = None  # Used in CSV
    escalation_at: Optional[str] = None
    pending_time: Optional[int] = None
    type: Optional[str] = None
    time_unit: Optional[str] = None
    preferences: Optional[Dict[str, Any]] = None
    updated_by_id: Optional[int] = None
    created_by_id: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    checklist_id: Optional[int] = None
    referencing_checklist_ids: Optional[List[int]] = None
    article_ids: Optional[List[int]] = None
    ticket_time_accounting_ids: Optional[List[int]] = None


class TicketSearchResponse(BaseModel):
    root: List[TicketRecord]


# Schema for Ticket Articles Response (list of articles)
class TicketArticle(BaseModel):
    id: Optional[int] = None
    ticket_id: Optional[int] = None
    type_id: Optional[int] = None
    sender_id: Optional[int] = None
    detected_language: Optional[str] = None
    from_field: Optional[str] = None  # 'from' is keyword, renamed
    to: Optional[str] = None
    cc: Optional[str] = None
    subject: Optional[str] = None
    reply_to: Optional[str] = None
    message_id: Optional[str] = None
    message_id_md5: Optional[str] = None
    in_reply_to: Optional[str] = None
    content_type: Optional[str] = None
    body: Optional[str] = None  # Used in CSV
    internal: Optional[bool] = None
    preferences: Optional[Dict[str, Any]] = None
    updated_by_id: Optional[int] = None
    created_by_id: Optional[int] = None
    origin_by_id: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    attachments: Optional[List[Dict[str, Any]]] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    type: Optional[str] = None
    sender: Optional[str] = None
    time_unit: Optional[str] = None


# For list response: use RootModel in Pydantic v2
from pydantic import RootModel


class TicketArticlesResponse(RootModel):
    root: List[TicketArticle]
    # Usage: TicketArticlesResponse.model_validate(data) where data is list


# For query params: validation with pattern and parse
class TicketQuery(BaseModel):
    start_date: str
    end_date: str

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        """
        Validator for YYYY-MM-DD format (pre-validation).
        Raises ValueError on invalid; integrates with OpenAPI for auto-docs/examples.
        """
        if not isinstance(v, str) or not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            raise ValueError("Date must be in YYYY-MM-DD format")
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Invalid date: use YYYY-MM-DD")
        return v

    @model_validator(mode="after")
    def validate_date_range(self) -> "TicketQuery":
        """
        Cross-field validator for date range (post-validation).
        Ensures start_date <= end_date using model_validator (v2 style for multi-field checks).
        """
        if (
            self.start_date > self.end_date
        ):  # Lexicographical compare works for YYYY-MM-DD
            raise ValueError("start_date must be before or equal to end_date")
        return self


# Comment: OpenAPI will show examples like ?start_date=2025-10-09&end_date=2025-10-10 with validation errors in UI.
# Migration note: Switched to @field_validator (v2) for per-field, @model_validator for cross-field to avoid deprecation.
