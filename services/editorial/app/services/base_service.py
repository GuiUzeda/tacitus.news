from typing import Any, Dict, List, Type

from app.services.sqlalchemy_filter import SQLAlchemyFilter
from sqlalchemy import asc, desc, select
from sqlalchemy.orm import Session


class BaseService:
    def __init__(self, session: Session):
        self.session = session

    def _search(
        self,
        model_class: Type,
        filters: Dict[str, Any],
        limit: int = 20,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        stmt = select(model_class)
        order_by = filters.pop("order_by", None)

        if filters:
            f = SQLAlchemyFilter(model_class, stmt)
            stmt = f.apply(filters)

        # Apply ordering
        if order_by:
            if order_by.startswith("-"):
                field_name = order_by[1:]
                if hasattr(model_class, field_name):
                    stmt = stmt.order_by(desc(getattr(model_class, field_name)))
            else:
                field_name = order_by
                if hasattr(model_class, field_name):
                    stmt = stmt.order_by(asc(getattr(model_class, field_name)))
        else:
            # Default ordering
            if hasattr(model_class, "updated_at"):
                stmt = stmt.order_by(desc(model_class.updated_at))
            elif hasattr(model_class, "created_at"):
                stmt = stmt.order_by(desc(model_class.created_at))

        stmt = stmt.limit(limit).offset(offset)
        rows = self.session.scalars(stmt).all()
        return [self._serialize(row) for row in rows]

    def _serialize(self, row: Any) -> Dict[str, Any]:
        data = {}
        # Columns to exclude from serialization (e.g. large embeddings)
        exclude_columns = {"embedding", "embedding_centroid"}

        for column in row.__table__.columns:
            if column.name in exclude_columns:
                continue

            val = getattr(row, column.name)
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            elif hasattr(val, "value") and hasattr(val, "__str__"):  # Enums
                val = val.value
            data[column.name] = val
        return data
