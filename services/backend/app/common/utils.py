from typing import Any, List, Sequence, Type

from pydantic import BaseModel


def schema_transformer(
    self, model_type: Type[BaseModel], models: Sequence[Any]
) -> List[Any]:
    r = []
    for model in models:
        r.append(model_type.model_validate(model))

    return r


async def aschema_transformer(
    self, model_type: Type[BaseModel], models: Sequence[Any]
) -> List[Any]:
    r = []
    for model in models:
        r.append(model_type.model_validate(model))

    return r