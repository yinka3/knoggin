"""HTTP input for curated episode narrative edits."""

from typing import Annotated

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator


class EditEpisode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str = Field(min_length=1, max_length=20000)
    new_developments: list[Annotated[str, Field(min_length=1, max_length=20000)]] = Field(max_length=100)
    updates: list[Annotated[str, Field(min_length=1, max_length=20000)]] = Field(max_length=100)
    unresolved: list[Annotated[str, Field(min_length=1, max_length=20000)]] = Field(max_length=100)
    expected_updated_at: AwareDatetime

    @field_validator("summary")
    @classmethod
    def nonblank_summary(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("Summary must not be blank")
        return value

    @field_validator("new_developments", "updates", "unresolved")
    @classmethod
    def nonblank_items(cls, values):
        result = [item.strip() for item in values]
        if any(not item for item in result):
            raise ValueError("Narrative items must not be blank")
        return result

    @model_validator(mode="after")
    def bounded_narrative(self):
        if sum(map(len, (self.summary, *self.new_developments, *self.updates, *self.unresolved))) > 20000:
            raise ValueError("Episode narrative is too long")
        return self
