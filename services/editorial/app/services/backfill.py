from news_events_lib.models import ArticleModel, NewsEventModel
from sqlalchemy import or_, select

# Attempt to import aggregator, might need careful pathing
try:
    from domain.aggregator import EventAggregator
except ImportError:
    EventAggregator = None


class BackfillService:
    def __init__(self, session):
        self.session = session
        self._nlp = None

    def _get_nlp(self):
        if self._nlp is None:
            import spacy

            try:
                self._nlp = spacy.load("pt_core_news_lg")
            except OSError:
                self._nlp = spacy.load("pt_core_news_sm")
        return self._nlp

    def backfill_stance_batch(self, limit: int = 50) -> str:
        """Recalculates stance distribution for events."""
        if EventAggregator is None:
            return "❌ EventAggregator not found. Cannot backfill stance."

        stmt = (
            select(NewsEventModel)
            .where(NewsEventModel.is_active.is_(True))
            .limit(limit)
        )
        events = self.session.scalars(stmt).all()

        if not events:
            return "✅ No active events for stance backfill."

        for event in events:
            # Load articles with newspapers
            articles = self.session.scalars(
                select(ArticleModel)
                .join(ArticleModel.newspaper)
                .where(ArticleModel.event_id == event.id)
            ).all()

            event.stance = 0.0
            event.stance_distribution = {}

            for art in articles:
                if art.newspaper and art.stance is not None:
                    EventAggregator.aggregate_stance(
                        event, art.newspaper.bias, art.stance
                    )

        self.session.commit()
        return f"✅ Backfilled stance for {len(events)} events."

    def backfill_interests_batch(self, limit: int = 20) -> str:
        """Updates entities/interests using spaCy."""
        nlp = self._get_nlp()

        # 1. Update Articles
        stmt = (
            select(ArticleModel)
            .where(or_(ArticleModel.interests.is_(None), ArticleModel.interests == {}))
            .limit(limit)
        )

        articles = self.session.scalars(stmt).all()
        if not articles:
            return "✅ No articles needing interest backfill."

        for art in articles:
            text = f"{art.title} {art.summary or ''}"
            doc = nlp(text[:100000])

            entities = {"person": set(), "place": set(), "org": set(), "topic": set()}
            for ent in doc.ents:
                if len(ent.text) < 3:
                    continue
                if ent.label_ == "PER":
                    entities["person"].add(ent.text)
                elif ent.label_ in ["LOC", "GPE"]:
                    entities["place"].add(ent.text)
                elif ent.label_ == "ORG":
                    entities["org"].add(ent.text)
                elif ent.label_ == "MISC":
                    entities["topic"].add(ent.text)

            art.interests = {k: sorted(list(v)) for k, v in entities.items() if v}

            flat_ents = []
            for v in art.interests.values():
                flat_ents.extend(v)
            art.entities = list(set(flat_ents))

        self.session.commit()
        return f"✅ Backfilled interests for {len(articles)} articles."

    def backfill_missing_topics_batch(self, limit: int = 50) -> str:
        """
        Backfills 'main_topic_counts' for events where it is missing.
        Runs a small batch to avoid timeouts.
        """
        # Find events with missing topic counts
        stmt = (
            select(NewsEventModel)
            .where(
                or_(
                    NewsEventModel.main_topic_counts.is_(None),
                    NewsEventModel.main_topic_counts == {},
                )
            )
            .limit(limit)
        )

        events = self.session.scalars(stmt).all()
        if not events:
            return "✅ No events found needing Topic Backfill."

        count = 0
        for event in events:
            # 1. Fetch articles
            # Note: Ideally we load this with a join, but lazy load is fine for batch=50
            articles = self.session.scalars(
                select(ArticleModel).where(ArticleModel.event_id == event.id)
            ).all()

            if not articles:
                continue

            # 2. Calculate
            topic_counts = {}
            for art in articles:
                if art.main_topics:
                    for t in art.main_topics:
                        topic_counts[t] = topic_counts.get(t, 0) + 1

            # 3. Update
            event.main_topic_counts = topic_counts
            self.session.add(event)
            count += 1

        self.session.commit()
        return f"✅ Backfilled Topics for {count} events."

    def backfill_bias_distribution_batch(self, limit: int = 50) -> str:
        """
        Backfills 'bias_distribution' for events where it is missing.
        """
        stmt = (
            select(NewsEventModel)
            .where(
                or_(
                    NewsEventModel.bias_distribution.is_(None),
                    NewsEventModel.bias_distribution == {},
                )
            )
            .limit(limit)
        )

        events = self.session.scalars(stmt).all()
        if not events:
            return "✅ No events found needing Bias Backfill."

        count = 0
        for event in events:
            # Join Newspaper to get bias
            articles = self.session.scalars(
                select(ArticleModel)
                .join(ArticleModel.newspaper)
                .where(ArticleModel.event_id == event.id)
            ).all()

            if not articles:
                continue

            dist = {}
            for art in articles:
                if art.newspaper and art.newspaper.bias:
                    bias = art.newspaper.bias
                    if bias not in dist:
                        dist[bias] = set()
                    dist[bias].add(str(art.newspaper.id))

            # Convert sets to lists for JSON
            event.bias_distribution = {k: list(v) for k, v in dist.items()}
            self.session.add(event)
            count += 1

        self.session.commit()
        return f"✅ Backfilled Bias Distribution for {count} events."
