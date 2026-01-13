import os
from logging.config import fileConfig

from alembic import context

from sqlalchemy import engine_from_config, pool, text

from news_events_lib.models import AuthorModel, ArticleModel, ArticleContentModel, FeedModel, NewspaperModel, BaseModel
from services.editorial.core.models import ArticlesQueueModel, EventsQueueModel








# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata


def extract_value_from_connection_string(prop_name, connection_string):
    return dict(
        [
            pair
            for pair in (d.split("=") for d in connection_string.split(";"))
            if len(pair) == 2
        ]
    )[prop_name]


target_metadata = BaseModel.metadata  # type: ignore


section = config.config_ini_section

database_url = os.environ.get("PG_DSN") or "postgresql://postgre:postgre@localhost:5432/postgre"


config.set_section_option(section, "POSTGRE_URL",database_url)



def include_object(object, name, type_, reflected, compare_to):
    # Exclude the 'cron.job' and 'cron.job_run_details' tables from autogenerate
    if (
        type_ == "table"
        and name in ["job", "job_run_details"]
        and object.schema == "cron"
    ):
        return False
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:  # type: ignore
        # Automatically kill blocking connections to allow migration locks
        try:
            connection.execute(text("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = current_database()
                AND pid <> pg_backend_pid()
            """))
            connection.commit()
        except Exception as e:
            print(f"⚠️ Failed to kill blocking connections: {e}")

        # Prevent migrations from hanging indefinitely by setting a lock timeout (e.g., 5 seconds)
        # connection.execute(text("SET lock_timeout = '15s'"))

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            include_schemas=True,
            include_object=include_object,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
