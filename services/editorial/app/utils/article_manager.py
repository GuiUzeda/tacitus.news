import uuid
from datetime import datetime, timezone

from loguru import logger
from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import and_, select
from sqlalchemy.orm import Session


class ArticleManager:
    @staticmethod
    def create_merge_proposal(
        session: Session,
        article: ArticleModel,
        event: NewsEventModel,
        score: float,
        ambiguous: bool = False,
        reason: str = "",
        commit: bool = False,
        status=JobStatus.PENDING,
    ) -> MergeProposalModel:
        exists = session.execute(
            select(MergeProposalModel).where(
                and_(
                    MergeProposalModel.source_article_id == article.id,
                    MergeProposalModel.target_event_id == event.id,
                )
            )
        ).scalar()
        if exists:
            logger.warning(
                f"⚠️ Proposal already exists: {article.title[:20]} -> {event.title[:20]}"
            )
            return exists

        final_reason = reason
        if ambiguous:
            final_reason += " (AMBIGUOUS)"

        prop = MergeProposalModel(
            id=uuid.uuid4(),
            source_article_id=article.id,
            target_event_id=event.id,
            distance_score=float(score),
            status=status,
            reasoning=final_reason,
        )
        session.add(prop)
        logger.info(
            f"⚠️ Proposal created: {article.title[:20]} -> {event.title[:20]} ({final_reason})"
        )
        if commit:
            session.commit()
        return prop

    @staticmethod
    def create_article_queue(
        session: Session,
        article_id: uuid.UUID,
        queue_name: ArticlesQueueName,
        reason: str = "",
        commit: bool = False,
    ) -> ArticlesQueueModel:
        now = datetime.now(timezone.utc)
        event_queue_entry = ArticlesQueueModel(
            article_id=article_id,
            queue_name=queue_name,
            status=JobStatus.PENDING,
            created_at=now,
            updated_at=now,
            msg=reason,
        )
        session.add(event_queue_entry)
        if commit:
            session.commit()
        return event_queue_entry
