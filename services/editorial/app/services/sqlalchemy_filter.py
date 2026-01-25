from typing import Any, Dict, Type

from sqlalchemy import Select, inspect
from sqlalchemy.orm import DeclarativeBase


class SQLAlchemyFilter:
    """
    A utility class to apply filters to a SQLAlchemy select statement
    based on a dictionary of criteria, similar to Django/fastapi-filter.

    Supported operators:
    - default (no suffix): Equality (==)
    - __gt: Greater than (>)
    - __gte: Greater than or equal (>=)
    - __lt: Less than (<)
    - __lte: Less than or equal (<=)
    - __in: In list (IN)
    - __like: Case-sensitive like (LIKE)
    - __ilike: Case-insensitive like (ILIKE)
    - __neq: Not equal (!=)

    Supports relationship spanning:
    - article__title__ilike: Joins article and filters by title
    """

    def __init__(self, model: Type[DeclarativeBase], query: Select):
        self.model = model
        self.query = query

    def apply(self, filters: Dict[str, Any]) -> Select:
        if not filters:
            return self.query

        for key, value in filters.items():
            if value is None:
                continue

            self.query = self._apply_filter(self.query, self.model, key, value)

        return self.query

    def _apply_filter(
        self, query: Select, model: Type[DeclarativeBase], key: str, value: Any
    ) -> Select:
        parts = key.split("__")
        field_name = parts[0]

        mapper = inspect(model)

        # Check if it's a relationship
        if field_name in mapper.relationships:
            relation = mapper.relationships[field_name]
            related_model = relation.mapper.class_

            # Join the relationship
            # We use isouter=True if we want to allow nulls, but for filtering usually we want Inner Join if we filter on it.
            # However, if the user filters "article__title", they imply the article must exist.
            # Using join() (inner join) is standard for filtering.

            # Check if already joined to avoid multiple joins?
            # SQLAlchemy's select.join() is smart enough usually, but let's be safe.
            # Actually, without aliasing, checking presence in query._join_entities might be hard.
            # For now, we trust SQLAlchemy to handle redundant joins or we just join.
            query = query.join(getattr(model, field_name))

            # Recurse
            remaining_key = "__".join(parts[1:])
            return self._apply_filter(query, related_model, remaining_key, value)

        else:
            # It's a direct column field (or invalid)
            # We need to parse the operator from the *remaining* key
            # But wait, parts contains all splits.

            column_name, op = self._parse_key_parts(parts)

            if not hasattr(model, column_name):
                return query

            column = getattr(model, column_name)
            return self._apply_operator(query, column, op, value)

    def _parse_key_parts(self, parts: list[str]) -> tuple[str, str]:
        """
        Interprets the last part as operator if valid, otherwise defaults to eq.
        Returns (field_name, operator)
        """
        if len(parts) > 1 and parts[-1] in [
            "gt",
            "gte",
            "lt",
            "lte",
            "in",
            "like",
            "ilike",
            "neq",
        ]:
            return "__".join(parts[:-1]), parts[-1]
        return "__".join(parts), "eq"

    def _apply_operator(
        self, query: Select, column: Any, op: str, value: Any
    ) -> Select:
        if op == "eq":
            return query.where(column == value)
        elif op == "neq":
            return query.where(column != value)
        elif op == "gt":
            return query.where(column > value)
        elif op == "gte":
            return query.where(column >= value)
        elif op == "lt":
            return query.where(column < value)
        elif op == "lte":
            return query.where(column <= value)
        elif op == "in":
            if isinstance(value, list):
                return query.where(column.in_(value))
        elif op == "like":
            return query.where(column.like(f"%{value}%"))
        elif op == "ilike":
            return query.where(column.ilike(f"%{value}%"))

        return query
