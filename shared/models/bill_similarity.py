from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class SimilarityClass(str, Enum):
    """Bill-to-bill relationship strength from text comparison."""
    MODEL_OR_DERIVATIVE = "model_or_derivative"  # near-duplicate: model bill / copy
    RELATED = "related"                          # substantial shared language
    UNRELATED = "unrelated"


class BillSimilarity(BaseModel):
    """A scored similarity relationship between two bills.

    Bills are referenced by ``external_id`` (the stable key used in the ``bills``
    table). ``score`` is a 0.0-1.0 text-overlap score; ``classification`` buckets it.
    """
    id: Optional[UUID] = None
    source_bill_id: str = Field(..., description="external_id of the first bill")
    target_bill_id: str = Field(..., description="external_id of the second bill")
    score: float = Field(..., ge=0.0, le=1.0)
    classification: str = SimilarityClass.UNRELATED.value
    method: str = "jaccard+3gram"
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
        use_enum_values = True
