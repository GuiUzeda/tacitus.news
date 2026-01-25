from typing import Any, List, Sequence, Type

from pydantic import BaseModel


def schema_transformer(models: Sequence[Any], model_type: Type[BaseModel]) -> List[Any]:
    r = []
    for model in models:
        r.append(model_type.model_validate(model))

    return r


async def aschema_transformer(
    models: Sequence[Any], model_type: Type[BaseModel]
) -> List[Any]:
    r = []
    for model in models:
        r.append(model_type.model_validate(model))

    return r
