from datetime import datetime, timedelta, timezone
import stat
from types import NoneType
from typing import List, Optional
import uuid
from loguru import logger
from news_events_lib.models import (
    EventStatus,
    JobStatus,
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    EventsQueueModel,
    EventsQueueName,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import and_, delete, desc, func, or_, select, update
from sqlalchemy.orm import Session, aliased
from wasabi import msg

from services.editorial.utils.event_aggregator import EventAggregator


class ArticleManager():
    @staticmethod
    def create_merge_proposal(
        session: Session,
        article: ArticleModel,
        event: NewsEventModel,
        score: float,
        ambiguous: bool = False,
        reason: str = "",
        commit: bool = False,
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

        reason = f"RRF Match. Vector Dist {score:.3f}"
        if ambiguous:
            reason += " (AMBIGUOUS)"

        prop = MergeProposalModel(
            id=uuid.uuid4(),
            source_article_id=article.id,
            target_event_id=event.id,
            distance_score=float(score),
            status=JobStatus.PENDING,
            reasoning=reason,
        )
        session.add(prop)
        logger.info(
            f"⚠️ Proposal created: {article.title[:20]} -> {event.title[:20]} ({reason})"
        )
        if commit:
            session.commit()
        return prop