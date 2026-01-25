import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Type

# Import Models
from news_events_lib.models import (
    ArticleModel,
    AuthorModel,
    FeedModel,
    MergeProposalModel,
    NewsEventModel,
    NewspaperModel,
    article_author_association,
)
from sqlalchemy import insert, select, text
from sqlalchemy.orm import Session


class BackupService:
    def __init__(self, session: Session, dump_dir: str = "dump_data"):
        self.session = session
        self.dump_dir = Path(dump_dir)
        self.dump_dir.mkdir(exist_ok=True)

    def _json_serializer(self, obj: Any) -> str:
        """Custom serializer for UUID, Datetime."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return str(obj)

    def dump_table(self, model: Type, filename: str) -> int:
        """Dumps a single table to JSON."""
        # Check if it's an Association Table (not a class)
        if hasattr(model, "c"):
            stmt = select(model)
            rows = self.session.execute(stmt).all()
            data = [dict(row._mapping) for row in rows]
        else:
            # ORM Model
            objs = self.session.scalars(select(model)).all()
            data = []
            for obj in objs:
                # Copy raw dict, remove SQLAlchemy internal state
                record = obj.__dict__.copy()
                record.pop("_sa_instance_state", None)
                data.append(record)

        filepath = self.dump_dir / f"{filename}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                data, f, default=self._json_serializer, ensure_ascii=False, indent=2
            )

        return len(data)

    def dump_all(self) -> str:
        """Dumps all core tables in dependency order."""
        try:
            c1 = self.dump_table(NewspaperModel, "newspapers")
            c2 = self.dump_table(FeedModel, "feeds")
            c3 = self.dump_table(AuthorModel, "authors")
            c4 = self.dump_table(NewsEventModel, "news_events")
            c5 = self.dump_table(ArticleModel, "articles")
            c6 = self.dump_table(MergeProposalModel, "merge_proposals")
            c7 = self.dump_table(article_author_association, "article_authors")

            return f"✅ Dump successful! Total records: {c1 + c2 + c3 + c4 + c5 + c6 + c7} saved to '{self.dump_dir}'"
        except Exception as e:
            return f"❌ Dump failed: {str(e)}"

    def _load_json(self, filename: str) -> List[Dict]:
        filepath = self.dump_dir / f"{filename}.json"
        if not filepath.exists():
            return []
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def clean_db(self):
        """Truncates all tables (Cascade)."""
        # Reverse order of dependency
        tables = [
            "merge_proposals",
            "article_authors",
            "articles",
            "news_events",
            "authors",
            "feeds",
            "newspapers",
        ]
        for table in tables:
            self.session.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
        self.session.commit()

    def restore_table(self, model: Any, filename: str):
        records = self._load_json(filename)
        if not records:
            return 0

        # Chunking for memory safety
        CHUNK_SIZE = 1000
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i : i + CHUNK_SIZE]
            self.session.execute(insert(model), chunk)

        self.session.commit()
        return len(records)

    def restore_events(self) -> int:
        """Special logic for self-referential events (merged_into_id)."""
        records = self._load_json("news_events")
        if not records:
            return 0

        # Step 1: Insert without merged_into_id
        deferred_merges = []
        for rec in records:
            merged_into = rec.get("merged_into_id")
            if merged_into:
                deferred_merges.append({"id": rec["id"], "merged_into_id": merged_into})
                rec["merged_into_id"] = None

        # Insert
        CHUNK_SIZE = 1000
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i : i + CHUNK_SIZE]
            self.session.execute(insert(NewsEventModel), chunk)
        self.session.commit()

        # Step 2: Re-link Merges
        if deferred_merges:
            for item in deferred_merges:
                self.session.execute(
                    text("UPDATE news_events SET merged_into_id = :mid WHERE id = :id"),
                    {"mid": item["merged_into_id"], "id": item["id"]},
                )
            self.session.commit()

        return len(records)

    def restore_all(self) -> str:
        """Restores all tables from JSON dumps."""
        try:
            self.clean_db()

            c1 = self.restore_table(NewspaperModel, "newspapers")
            c2 = self.restore_table(FeedModel, "feeds")
            c3 = self.restore_table(AuthorModel, "authors")
            c4 = self.restore_events()  # Handling news_events
            c5 = self.restore_table(ArticleModel, "articles")
            c6 = self.restore_table(article_author_association, "article_authors")
            c7 = self.restore_table(MergeProposalModel, "merge_proposals")

            return f"✅ Restore successful! Total records: {c1 + c2 + c3 + c4 + c5 + c6 + c7}"
        except Exception as e:
            self.session.rollback()
            return f"❌ Restore failed: {str(e)}"
