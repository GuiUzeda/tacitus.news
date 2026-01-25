from typing import Optional

from news_events_lib.models import JobStatus, MergeProposalModel
from sqlalchemy import select
from sqlalchemy.orm import joinedload


class ReviewService:
    def __init__(self, session, cluster_service=None):
        self.session = session
        self._cluster_service = cluster_service

    @property
    def cluster_service(self):
        if self._cluster_service is None:
            from app.workers.cluster.domain import NewsCluster

            self._cluster_service = NewsCluster()
        return self._cluster_service

    def get_pending_counts(self) -> dict:
        """Returns counts of pending merges."""
        from sqlalchemy import func

        a_count = self.session.scalar(
            select(func.count(MergeProposalModel.id)).where(
                MergeProposalModel.status == JobStatus.PENDING,
                MergeProposalModel.source_article_id.is_not(None),
            )
        )
        e_count = self.session.scalar(
            select(func.count(MergeProposalModel.id)).where(
                MergeProposalModel.status == JobStatus.PENDING,
                MergeProposalModel.source_event_id.is_not(None),
            )
        )
        return {"article_merges": a_count, "event_merges": e_count}

    def get_next_merge_proposal(
        self, type: str = "article"
    ) -> Optional[MergeProposalModel]:
        """Fetches the next pending proposal. type='article' or 'event'"""
        stmt = select(MergeProposalModel).where(
            MergeProposalModel.status == JobStatus.PENDING
        )

        if type == "article":
            stmt = stmt.where(
                MergeProposalModel.source_article_id.is_not(None)
            ).options(
                joinedload(MergeProposalModel.source_article),
                joinedload(MergeProposalModel.target_event),
            )
        else:
            stmt = stmt.where(MergeProposalModel.source_event_id.is_not(None)).options(
                joinedload(MergeProposalModel.source_event),
                joinedload(MergeProposalModel.target_event),
            )

        return self.session.scalars(
            stmt.order_by(MergeProposalModel.distance_score).limit(1)
        ).first()

    def process_merge_proposal(self, proposal_id: str, action: str) -> str:
        """
        Action: 'merge', 'reject', 'new_event' (only for article)
        """
        proposal = self.session.get(MergeProposalModel, proposal_id)
        if not proposal or proposal.status != JobStatus.PENDING:
            return "❌ Proposal not found or already processed."

        if action == "merge":
            if proposal.source_article_id:
                self.cluster_service.execute_merge_action(
                    self.session, proposal.source_article, proposal.target_event
                )
            elif proposal.source_event_id:
                self.cluster_service.execute_event_merge(
                    self.session, proposal.source_event, proposal.target_event
                )
            self.session.commit()
            return "✅ Merged successfully."

        elif action == "reject":
            proposal.status = JobStatus.REJECTED
            self.session.commit()
            return "✅ Proposal Rejected."

        elif action == "new_event" and proposal.source_article_id:
            new_event = self.cluster_service.execute_new_event_action(
                self.session,
                proposal.source_article,
                reason="Manual Review: Rejected Merge",
            )
            proposal.status = (
                JobStatus.REJECTED
            )  # Reject the merge since we made a new event
            self.session.add(new_event)
            self.session.commit()
            return f"✅ New Event Created: {new_event.title}"

        return "❌ Invalid action."
