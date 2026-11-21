"""The stored company and the rule for merging an incoming event into it.

Delivery is at-least-once and not ordered, so the rule is: for each field, the latest non-blank value by source
`updatedAt` wins, whatever order the events arrive in. Applying the same event twice, or the same events in a
different order, ends in the same state.

Each field carries its own timestamp. With one timestamp per company, an older event that fills a blank field
would claim the newest timestamp, and a middle-aged event arriving afterwards would be wrongly ignored.
"""

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Final, Literal

import numpy as np

from recsys.parsing import CompanyEvent

Status = Literal["active", "struck_off"]
_DATA_FIELDS: Final = ("name", "description", "industry", "state")


@dataclass(frozen=True, slots=True)
class Company:
    registration_number: str
    name: str | None = None
    description: str | None = None
    industry: str | None = None
    state: str | None = None
    status: Status = "active"
    field_updated_at: dict[str, datetime] = field(default_factory=dict)  # source timestamp of each field above
    status_updated_at: datetime | None = None  # latest event of any kind, which is what decides `status`
    embedded_text: str | None = None  # the exact text `embedding` was computed from
    embedding: np.ndarray | None = None  # only set when it still has to be written to the database


def merge(existing: Company | None, event: CompanyEvent) -> Company:
    """Return the company after applying `event`. Never lets an older value override a newer one."""
    company = existing or Company(event.registration_number)

    if event.kind == "upserted":
        versions = dict(company.field_updated_at)
        for name in _DATA_FIELDS:
            incoming = getattr(event, name)
            if incoming is None:  # blank means "not provided", never "erase"
                continue
            if _wins(incoming, event.updated_at, getattr(company, name), versions.get(name)):
                company = replace(company, **{name: incoming})
                versions[name] = event.updated_at
        company = replace(company, field_updated_at=versions)

    if company.status_updated_at is None or event.updated_at > company.status_updated_at:
        status: Status = "struck_off" if event.kind == "struck_off" else "active"
        company = replace(company, status=status, status_updated_at=event.updated_at)

    return company


def _wins(incoming: str, incoming_at: datetime, current: str | None, current_at: datetime | None) -> bool:
    if current is None or current_at is None or incoming_at > current_at:
        return True
    # Same timestamp, different values: pick one deterministically so arrival order cannot matter.
    return incoming_at == current_at and incoming > current


def embedding_text(company: Company) -> str | None:
    """What gets embedded: the business description, or name and industry when the registry sent none."""
    if company.description:
        return company.description
    if company.name:
        return f"{company.name}. {company.industry}" if company.industry else company.name
    return None
