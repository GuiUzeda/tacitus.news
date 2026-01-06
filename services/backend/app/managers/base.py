from app.common.mixins import DBSessionMixin
from contextlib import contextmanager
from typing import (
    Any,
    List,
    Optional,
    Sequence,
    Type,
)
from uuid import UUID
from app.exc import raise_with_log

from fastapi_pagination import Page
from fastapi_pagination.bases import AbstractParams
from fastapi_pagination.ext.sqlalchemy import paginate
from fastapi_pagination.types import (
    AdditionalData,
    AsyncItemsTransformer,
    SyncItemsTransformer,
)
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import delete, func, select, update

from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import Executable, Select, Update
from sqlalchemy.orm import DeclarativeBase

class BaseManager(DBSessionMixin):
    """Base data manager class responsible for operations over database."""

    @contextmanager
    def db_transaction(self):
        try:
            yield
            self.db_session.commit()
        except IntegrityError as e:
            logger.error(f"IntegrityError: {e.orig}")
            self.db_session.rollback()
            raise raise_with_log(
                403,
                "What you are trying to do is not permited by the DB rules. Common DB constrains: duplicated value, null value, delete/update fk in use",
            )
        except Exception:
            self.db_session.rollback()
            raise

    def filter_deletes(self, model: DeclarativeBase, stmt):
        pass
        return stmt

    def add_one(self, model: DeclarativeBase) -> UUID :
        # model.data_hora = datetime.utcnow()  # type: ignore
        with self.db_transaction():
            self.db_session.add(model)
        return model.id  # type: ignore

    def add_all(self, models) -> List[UUID ]:
        with self.db_transaction():
            self.db_session.add_all(models)
        return [model.id for model in models]

    def get_one(self, select_stmt: Executable) -> Optional[Any]:
        return self.db_session.scalar(select_stmt)

    def get_all_scalar(self, select_stmt: Executable) -> List[Any]:
        return self.db_session.scalars(select_stmt).unique().all()

    def get_all(self, select_stmt: Executable) -> List[Any]:
        return self.db_session.execute(select_stmt).all()

    def get_paginated(
        self,
        select_stmt: Select,
        additional_data: AdditionalData = None,
        transformer: Optional[SyncItemsTransformer] = None,
        params: AbstractParams | None = None,
        unique: bool = True,
    ) -> Page[Any]:

        return paginate(
            self.db_session,
            select_stmt,
            params=params,
            additional_data=additional_data,
            transformer=transformer,
            unique=unique,
        )

    async def aget_paginated(
        self,
        select_stmt: Select,
        additional_data: AdditionalData = None,
        transformer: Optional[AsyncItemsTransformer] = None,
    ) -> Page[Any]:
        return paginate(
            self.db_session,
            select_stmt,
            additional_data=additional_data,
            transformer=transformer,  # type: ignore
        )

    def get_from_tvf(self, model: Type[DeclarativeBase], *args: Any) -> List[Type[DeclarativeBase]]:
        """Query from table valued function.

        This is a wrapper function that can be used to retrieve data from
        table valued functions.

        Examples:
            from app.models.base import SQLModel

            class MyModel(SQLModel):
                __tablename__ = "function"
                __table_args__ = {"schema": "schema"}

                x: Mapped[int] = mapped_column("x", primary_key=True)
                y: Mapped[str] = mapped_column("y")
                z: Mapped[float] = mapped_column("z")

            # equivalent to "SELECT x, y, z FROM schema.function(1, "AAA")"
            BaseDataManager(session).get_from_tvf(MyModel, 1, "AAA")
        """

        fn = getattr(getattr(func, model.schema()), model.table_name())
        stmt = select(fn(*args).table_valued(*model.fields()))
        return self.get_all(select(model).from_statement(stmt))

    def update(
        self,
        model: Type[DeclarativeBase],
        data: dict,
        model_id: Optional[int | UUID] = None,
        stmt: Optional[Update] = None,
        validate_row_count: Optional[bool] = True,
    ) -> None:
        if model_id is None and stmt is None:
            raise Exception("Both model_id and stmt can not be None ")
        """Partially update specific fields of a record in the database."""

        s = stmt if stmt is not None else update(model).where(model.id == model_id)
        with self.db_transaction():
            result = self.db_session.execute(
                s.values(data),
                execution_options={"synchronize_session": "fetch"},
            )
            if validate_row_count:
                if not result.rowcount or result.rowcount == 0:
                    raise Exception("No rows affected")

    def delete(
        self, model: Type[DeclarativeBase], model_id: UUID | int = 0, stmt=None
    ) -> None:
        if stmt is None:
            stmt = delete(model).where(model.id == model_id)
        with self.db_transaction():
            self.db_session.execute(
                stmt, execution_options={"synchronize_session": "fetch"}
            )

    def execute(self, select_stmt: Executable) -> Any:
        """Partially update specific fields of a record in the database."""
        return self.db_session.execute(select_stmt)

    def transformer(
        self, model_type: Type[BaseModel], models: Sequence[Any]
    ) -> List[Any]:
        r = []
        for model in models:
            # print(model)
            r.append(model_type.model_validate(model))

        return r

    def _check_nome_exists(self, stmt, msg="Já cadastrado"):
        model = self.get_one(stmt)
        if model:
            raise raise_with_log(403, msg)
