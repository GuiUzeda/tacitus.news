import time
from datetime import datetime

import pandas as pd
import streamlit as st

# Imports
from app.cli.cli_common import SessionLocal
from app.services.analytics_service import AnalyticsService
from app.services.article_service import ArticleService
from app.services.audit_service import AuditService
from app.services.event_service import EventService
from app.services.health_service import HealthService
from app.services.queue_service import QueueService
from app.services.rate_limit_service import RateLimitService
from news_events_lib.models import ArticlesQueueName, EventsQueueName, JobStatus

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Tacitus Editorial",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- CUSTOM STYLING (CSS) ---
st.markdown(
    """
<style>
    /* Global Font & Spacing */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 2rem;
    }

    /* Metrics Styling */
    div[data-testid="stMetric"] {
        background-color: #262730;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #41424C;
        text-align: center;
    }
    div[data-testid="stMetricLabel"] {
        font-size: 0.9rem;
        color: #9DA3A8;
        justify-content: center;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
        color: #FFFFFF;
    }

    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background-color: #1A1C24;
    }

    /* Custom Alert Banner */
    .heartbeat-ok {
        padding: 12px 20px;
        background-color: #0E3A28;
        color: #66FF99;
        border-left: 5px solid #00CC66;
        border-radius: 4px;
        font-family: monospace;
        margin-bottom: 20px;
    }
    .heartbeat-warn {
        padding: 12px 20px;
        background-color: #3A2A0E;
        color: #FFCC66;
        border-left: 5px solid #FF9900;
        border-radius: 4px;
        font-family: monospace;
        margin-bottom: 20px;
    }
    .heartbeat-crit {
        padding: 12px 20px;
        background-color: #3A0E0E;
        color: #FF6666;
        border-left: 5px solid #FF3333;
        border-radius: 4px;
        font-family: monospace;
        margin-bottom: 20px;
    }
</style>
""",
    unsafe_allow_html=True,
)

# --- SIDEBAR & NAVIGATION ---
with st.sidebar:
    st.markdown("# 📰 Tacitus")
    st.markdown("---")
    page = st.radio(
        "📍 Navigation",
        ["Dashboard", "Live Desk", "Queue Inspector", "System Health"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()
    st.caption("v2.12.1 • Editorial Core")


# --- HELPERS ---
def get_session():
    return SessionLocal()


@st.cache_resource
def get_cluster_domain():
    """Cached loading of the heavy Cluster Domain (Torch models)."""
    from app.workers.cluster.domain import NewsCluster

    return NewsCluster()


# --- FRAGMENTS ---


@st.fragment
def render_queues_card(audit_service, queue_service):
    with st.container(border=True):
        q_head_c1, q_head_c2, q_head_c3 = st.columns([3, 1, 1])
        q_head_c1.subheader("🏭 Queues")
        if q_head_c2.button("🔄", key="q_ref_dash", help="Refresh Queue Stats"):
            st.rerun()
        if q_head_c3.button(
            "🧹", key="q_stale_dash", help="Reset Stale Processing Jobs"
        ):
            res = queue_service.requeue_stale_items(minutes_stale=10)
            st.toast(f"Reset {sum(res.values())} stale jobs", icon="🧹")
            time.sleep(1)
            st.rerun()

        q_stats = audit_service.get_queue_overview("all")
        q_data = []
        for cat, items in q_stats.items():
            for item in items:
                q_data.append(
                    {
                        "Category": cat.capitalize(),
                        "Queue": str(item.get("queue", cat))
                        .replace("QueueName.", "")
                        .split(".")[-1],
                        "Status": item.get("status"),
                        "Count": item.get("count"),
                    }
                )
        if q_data:
            df_q = pd.DataFrame(q_data)
            df_pivot = df_q.pivot_table(
                index=["Category", "Queue"],
                columns="Status",
                values="Count",
                fill_value=0,
            )
            st.dataframe(df_pivot, use_container_width=True)
        else:
            st.success("Queues empty.")


# --- PAGES ---

if page == "Dashboard":
    c_title, c_ref = st.columns([6, 1])
    c_title.title("Control Room")

    with get_session() as session:
        audit = AuditService(session)
        analytics = AnalyticsService(session)
        rate_svc = RateLimitService(session)
        queue_svc = QueueService(session)

        # --- HEARTBEAT ---
        ingest = analytics.get_last_ingestion_time()
        hb_class = "heartbeat-ok"
        if ingest["status"] == "warning":
            hb_class = "heartbeat-warn"
        elif ingest["status"] == "critical":
            hb_class = "heartbeat-crit"

        st.markdown(
            f"""
        <div class="{hb_class}">
            💓 SYSTEM HEARTBEAT | Last Ingestion: {ingest['minutes_ago']} mins ago ({ingest['last_ingest']})
        </div>
        """,
            unsafe_allow_html=True,
        )

        # --- KEY METRICS ---
        q_stats_overview = audit.get_queue_overview("all")
        blind_spots = analytics.get_blind_spot_stats()

        failed_count = 0
        for cat in q_stats_overview.values():
            for item in cat:
                if item.get("status") == "FAILED":
                    failed_count += item["count"]

        from news_events_lib.models import EventStatus, NewsEventModel

        active = session.query(NewsEventModel).filter_by(is_active=True).count()
        published = (
            session.query(NewsEventModel)
            .filter_by(status=EventStatus.PUBLISHED)
            .count()
        )

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Active Events", active)
        c2.metric("Published", published)
        c3.metric("System Failures", failed_count)
        c4.metric("Active Blind Spots", blind_spots["total_blind_spots"])

        st.markdown("###")

        # --- ROW 1: VOLUME & QUEUES ---
        col_r1_1, col_r1_2 = st.columns(2)

        with col_r1_1:
            with st.container(border=True):
                st.subheader("📈 Daily Volume (14d)")
                vol_data = analytics.get_daily_volumes(14)
                if vol_data:
                    df_vol = pd.DataFrame(vol_data)
                    df_pivot = df_vol.pivot(
                        index="date", columns="type", values="count"
                    ).fillna(0)
                    st.line_chart(df_pivot)
                else:
                    st.info("No volume data yet.")

        with col_r1_2:
            render_queues_card(audit, queue_svc)

        st.markdown("###")

        # --- ROW 2: INGESTION GRID ---
        with st.container(border=True):
            st.subheader("📰 Ingestion Grid (Last 36h)")
            grid_data = analytics.get_ingestion_grid(6)
            if grid_data:
                df_grid = pd.DataFrame(grid_data)
                df_pivot = df_grid.pivot(
                    index="source", columns="slot", values="count"
                ).fillna(0)
                st.dataframe(df_pivot, use_container_width=True)
            else:
                st.info("No recent ingestion data.")

        st.markdown("###")

        # --- ROW 3: AI VITALITY ---
        with st.container(border=True):
            st.subheader("⚡ AI Vitality (Rate Limits)")
            limits = rate_svc.get_status()
            if limits:
                df_limits = pd.DataFrame(limits)
                df_limits = df_limits[["model", "metric", "percent", "used", "limit"]]
                df_limits = df_limits.sort_values("percent", ascending=False)
                st.dataframe(
                    df_limits,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "percent": st.column_config.ProgressColumn(
                            "Usage %", min_value=0, max_value=100, format="%.1f%%"
                        )
                    },
                )
            else:
                st.info("No rate limit data.")

elif page == "Live Desk":
    st.title("Live Desk")

    with get_session() as session:
        event_svc = EventService(session)
        article_svc = ArticleService(session)

        # --- FILTERS ---
        with st.sidebar:
            st.markdown("### 🔍 Filters")
            search_query = st.text_input(
                "Hybrid Search", placeholder="Topic, entity..."
            )

            st.markdown("---")
            show_blind_spots = st.checkbox("Show Blind Spots Only")
            min_score = st.slider("Min Hot Score", 0, 100, 0)
            topic_filter = st.text_input("Topic Filter", placeholder="e.g. Politics")

        # --- DATA LOADING ---
        events = []
        is_search = False

        if search_query:
            is_search = True
            st.info(f"Results for '{search_query}' (Hybrid Search)")
            cluster = get_cluster_domain()
            results = cluster.search_news_events_hybrid(
                session,
                query_text=search_query,
                query_vector=None,
                target_date=datetime.now(),
                limit=20,
            )
            events = []
            for e, score, _ in results:
                if show_blind_spots and not e.is_blind_spot:
                    continue
                events.append(
                    {
                        "id": str(e.id),
                        "hot_score": e.hot_score,
                        "search_score": score,
                        "article_count": e.article_count,
                        "title": e.title,
                        "created_at": (
                            e.created_at.isoformat() if e.created_at else None
                        ),
                        "is_blind_spot": e.is_blind_spot,
                        "blind_spot_side": e.blind_spot_side,
                    }
                )
        else:
            if "page_number" not in st.session_state:
                st.session_state.page_number = 1
            limit = 15
            offset = (st.session_state.page_number - 1) * limit

            # Pagination Controls
            c1, c2, c3 = st.columns([1, 6, 1])
            with c1:
                if st.button("⬅️ Prev") and st.session_state.page_number > 1:
                    st.session_state.page_number -= 1
                    st.rerun()
            with c3:
                if st.button("Next ➡️"):
                    st.session_state.page_number += 1
                    st.rerun()

            events = event_svc.get_top_events(
                limit=limit,
                offset=offset,
                min_score=min_score if min_score > 0 else None,
                topic=topic_filter if topic_filter else None,
                only_blind_spots=show_blind_spots,
            )

        df_events = pd.DataFrame(events)

        if df_events.empty:
            st.warning("No events found.")
        else:
            # Main Table
            cols_to_show = ["id", "hot_score", "article_count", "title", "created_at"]
            if is_search:
                cols_to_show.insert(1, "search_score")
            if show_blind_spots:
                cols_to_show.append("blind_spot_side")

            display_df = df_events[cols_to_show]

            # Capture selection into session state
            event_selection = st.dataframe(
                display_df,
                use_container_width=True,
                on_select="rerun",
                selection_mode="single-row",
                hide_index=True,
                column_config={
                    "hot_score": st.column_config.ProgressColumn(
                        "Hot Score", min_value=0, max_value=100, format="%.1f"
                    ),
                    "created_at": st.column_config.DatetimeColumn(
                        "Created", format="D MMM, HH:mm"
                    ),
                    "title": st.column_config.TextColumn("Event Title", width="large"),
                },
            )

            # Update state from component
            if event_selection and event_selection.get("selection", {}).get("rows"):
                selected_idx = event_selection["selection"]["rows"][0]
                new_event_id = display_df.iloc[selected_idx]["id"]
                if st.session_state.get("selected_event_id") != new_event_id:
                    st.session_state.selected_event_id = new_event_id
                    st.session_state.pop(
                        "selected_art_id", None
                    )  # Clear article if event changes

            selected_id = st.session_state.get("selected_event_id")

            if selected_id:
                st.divider()

                # --- EVENT DETAILS ---
                details = event_svc.get_event_details(selected_id)

                if details:
                    # Title & Tags
                    st.markdown(f"## {details['title']}")
                    if details.get("subtitle"):
                        st.markdown(f"*{details['subtitle']}*")

                    # Tags
                    tags = []
                    if details.get("queue_tag"):
                        tags.append(f"⚙️ {details['queue_tag']}")
                    if details.get("proposal_tag"):
                        tags.append(f"🔄 {details['proposal_tag']}")
                    if details.get("last_summarized"):
                        tags.append(f"🕒 Sum: {details['last_summarized'][:16]}")
                    if tags:
                        st.markdown(" ".join([f"`{t}`" for t in tags]))

                    # Score Cards
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Hot Score", f"{details['hot_score']:.1f}")
                    c2.metric("Editorial Score", details["editorial_score"])
                    c3.metric("AI Impact", details["ai_impact_score"])
                    c4.metric("Articles", len(details.get("articles", [])))

                    # Actions Row
                    ac1, ac2, ac3, ac4, ac5 = st.columns(5)
                    if ac1.button(
                        "🔥 Boost (+10)", key="boost", use_container_width=True
                    ):
                        msg = event_svc.boost_event(selected_id, 10)
                        st.toast(msg, icon="🔥")
                        time.sleep(1)
                        st.rerun()

                    if ac2.button(
                        "❄️ Demote (-10)", key="demote", use_container_width=True
                    ):
                        msg = event_svc.boost_event(selected_id, -10)
                        st.toast(msg, icon="❄️")
                        time.sleep(1)
                        st.rerun()

                    if ac3.button(
                        "💀 Kill (Archive)", key="kill", use_container_width=True
                    ):
                        msg = event_svc.archive_event(selected_id)
                        st.toast(msg, icon="💀")
                        time.sleep(1)
                        st.rerun()

                    if ac4.button(
                        "♻️ Re-Enhance",
                        key="reenhance",
                        help="Regenerate Summary & Score (Event Level)",
                        use_container_width=True,
                    ):
                        msg = event_svc.requeue_event_for_enhancement(selected_id)
                        st.toast(msg, icon="♻️")
                        time.sleep(1)
                        st.rerun()

                    if ac5.button(
                        "📊 Recalculate",
                        key="recalc",
                        help="Recalculate aggregate data (Stance, Bias, etc.)",
                        use_container_width=True,
                    ):
                        msg = event_svc.recalculate_event_metrics(selected_id)
                        st.toast(msg, icon="📊")
                        time.sleep(1)
                        st.rerun()

                    # Summary & Insights
                    st.markdown("### 📝 Analysis")
                    with st.container(border=True):
                        st.markdown(details.get("summary", "No summary."))
                        st.caption(f"Insights: {details['insights']}")

                    # --- EVENT COVERAGE CHARTS ---
                    st.markdown("### 📊 Event Coverage Analysis")

                    # Use pre-calculated distributions if available
                    stance_dist = details.get("stance_dist")
                    clickbait_dist = details.get("clickbait_dist")

                    ec1, ec2, ec3 = st.columns(3)

                    with ec1:
                        st.caption("Stance Distribution (Left -1 to Right +1)")
                        if stance_dist:
                            flat_data = []
                            for bias, buckets in stance_dist.items():
                                for bucket, count in buckets.items():
                                    flat_data.append(
                                        {
                                            "Bias": bias,
                                            "Stance Bucket": bucket,
                                            "Count": count,
                                        }
                                    )
                            if flat_data:
                                df_s = pd.DataFrame(flat_data)
                                st.bar_chart(
                                    df_s,
                                    x="Bias",
                                    y="Count",
                                    color="Stance Bucket",
                                    stack=True,
                                )
                            else:
                                st.info("No stance data.")
                        else:
                            st.info("No pre-calc stance data.")

                    with ec2:
                        st.caption("Bias Distribution (Volume)")
                        articles = details.get("articles", [])
                        if articles:
                            df_arts = pd.DataFrame(articles)
                            df_arts["bias"] = df_arts["bias"].fillna("Unknown")
                            bias_counts = df_arts["bias"].value_counts().reset_index()
                            bias_counts.columns = ["bias", "count"]
                            st.bar_chart(bias_counts, x="bias", y="count")

                    with ec3:
                        st.caption("Avg Clickbait Score by Bias")
                        if clickbait_dist:
                            df_cb = pd.DataFrame(
                                list(clickbait_dist.items()), columns=["Bias", "Score"]
                            )
                            st.bar_chart(df_cb, x="Bias", y="Score")
                        else:
                            st.info("No clickbait data.")

                    # --- ARTICLES DRILL DOWN ---
                    st.markdown("### 🔗 Articles")
                    articles = details.get("articles", [])
                    if articles:
                        df_arts = pd.DataFrame(articles)

                        art_selection = st.dataframe(
                            df_arts[
                                ["title", "source", "published_date", "stance", "id"]
                            ],
                            use_container_width=True,
                            on_select="rerun",
                            selection_mode="single-row",
                            hide_index=True,
                            column_config={
                                "published_date": st.column_config.DatetimeColumn(
                                    "Date", format="D MMM, HH:mm"
                                ),
                                "stance": st.column_config.NumberColumn(
                                    "Stance", format="%.2f"
                                ),
                            },
                        )

                        # Update state for article selection
                        if art_selection and art_selection.get("selection", {}).get(
                            "rows"
                        ):
                            art_idx = art_selection["selection"]["rows"][0]
                            st.session_state.selected_art_id = df_arts.iloc[art_idx][
                                "id"
                            ]

                        selected_art_id = st.session_state.get("selected_art_id")

                        if selected_art_id:
                            art_details = article_svc.get_article_details(
                                selected_art_id
                            )

                            if art_details:
                                st.markdown("---")
                                with st.container(border=True):
                                    # Header with Re-Analyze Button
                                    ah1, ah2 = st.columns([5, 1])
                                    ah1.markdown(f"#### 📄 {art_details['title']}")
                                    if ah2.button(
                                        "🔬 Re-Analyze",
                                        key=f"re_art_{selected_art_id}",
                                        help="Re-queue for Enrichment",
                                    ):
                                        msg = article_svc.requeue_article_for_analysis(
                                            selected_art_id
                                        )
                                        st.toast(msg, icon="🔬")
                                        time.sleep(1)
                                        st.rerun()

                                    st.caption(
                                        f"Source: {art_details['source']} | Date: {art_details['published_date']}"
                                    )
                                    st.markdown(
                                        f"**URL:** [{art_details['url']}]({art_details['url']})"
                                    )

                                    tab1, tab2 = st.tabs(["Content", "Analysis"])
                                    with tab1:
                                        st.markdown(
                                            art_details["content"]
                                            or "*No content extracted.*"
                                        )
                                    with tab2:
                                        c_art1, c_art2 = st.columns(2)
                                        with c_art1:
                                            st.write(
                                                "**Stance:**", art_details.get("stance")
                                            )
                                            st.info(
                                                art_details.get(
                                                    "stance_reasoning", "No reasoning."
                                                )
                                            )
                                        with c_art2:
                                            st.write(
                                                "**Clickbait Score:**",
                                                art_details.get("clickbait_score"),
                                            )
                                            st.warning(
                                                art_details.get(
                                                    "clickbait_reasoning",
                                                    "No reasoning.",
                                                )
                                            )

                                        st.divider()
                                        st.write(
                                            "**Entities:**", art_details.get("entities")
                                        )
                                        st.write(
                                            "**Key Points:**",
                                            art_details.get("analysis"),
                                        )

elif page == "Queue Inspector":
    st.title("Queue Inspector")

    with get_session() as session:
        audit = AuditService(session)
        analytics = AnalyticsService(session)
        article_svc = ArticleService(session)
        event_svc = EventService(session)

        # --- TOP CONTROLS ---
        col_q1, col_q2, col_q3 = st.columns([2, 2, 1])
        with col_q1:
            # Using key to ensure state persistence across reruns
            queue_type = st.selectbox(
                "Select Queue", ["articles", "events", "proposals"], key="qi_queue_type"
            )

        # Queue Name Sub-Selector
        selected_queue_name = None
        with col_q2:
            if queue_type == "articles":
                q_names = ["All"] + [q.value for q in ArticlesQueueName]
                q_sel = st.selectbox("Sub-Queue", q_names, key="qi_sub_queue_articles")
                if q_sel != "All":
                    selected_queue_name = q_sel
            elif queue_type == "events":
                q_names = ["All"] + [q.value for q in EventsQueueName]
                q_sel = st.selectbox("Sub-Queue", q_names, key="qi_sub_queue_events")
                if q_sel != "All":
                    selected_queue_name = q_sel
            else:
                st.write("")  # Spacer

        with col_q3:
            st.write("")  # Spacer
            if st.button("🔄 Refresh"):
                st.rerun()

        # --- SUMMARY METRICS ---
        st.markdown("### 📊 Status Overview")
        q_stats = audit.get_queue_overview(queue_type)
        # Flatten stats
        counts = {}
        for item in q_stats.get(queue_type, []):
            if selected_queue_name and item.get("queue") != selected_queue_name:
                continue
            status = item["status"]
            counts[status] = counts.get(status, 0) + item["count"]

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Pending", counts.get("PENDING", 0))
        m2.metric("Processing", counts.get("PROCESSING", 0))
        m3.metric(
            "Completed/Approved", counts.get("COMPLETED", 0) + counts.get("APPROVED", 0)
        )
        m4.metric(
            "Failed/Rejected", counts.get("FAILED", 0) + counts.get("REJECTED", 0)
        )

        # --- VISUAL: QUEUE ACTIVITY 24H ---
        st.divider()
        st.markdown("### 📉 Queue Activity (24h)")
        vol_hist = analytics.get_queue_volume_history(
            queue_type, 24, queue_name=selected_queue_name
        )
        if vol_hist:
            df_hist = pd.DataFrame(vol_hist)
            st.line_chart(df_hist, x="hour", y="count", color="status")
        else:
            st.info("No processing activity in last 24h.")

        # --- INSPECTOR TABLE ---
        st.divider()
        st.markdown("### 🔍 Item Inspector")

        with st.expander("Filters", expanded=True):
            f1, f2, f3 = st.columns(3)
            with f1:
                search_id = st.text_input(
                    "Search ID (Article/Event UUID)", key="qi_search_id"
                )
            with f2:
                status_filter = st.multiselect(
                    "Status", [s.value for s in JobStatus], key="qi_status_filter"
                )
            with f3:
                limit = st.number_input("Page Size", 20, 500, 50, key="qi_limit")

        # --- PAGINATION LOGIC ---
        if "q_page" not in st.session_state:
            st.session_state.q_page = 1

        cp1, cp2, cp3 = st.columns([1, 6, 1])
        with cp1:
            if st.button("⬅️ Prev", key="qp") and st.session_state.q_page > 1:
                st.session_state.q_page -= 1
                st.rerun()
        with cp3:
            if st.button("Next ➡️", key="qn"):
                st.session_state.q_page += 1
                st.rerun()
        with cp2:
            st.write(f"Page {st.session_state.q_page}")

        offset = (st.session_state.q_page - 1) * limit

        # Build Filter Dict
        filters = {}
        if status_filter:
            filters["status__in"] = status_filter

        if selected_queue_name:
            filters["queue_name"] = selected_queue_name

        if search_id:
            if queue_type == "articles":
                filters["article_id"] = search_id
            elif queue_type == "events":
                filters["event_id"] = search_id

        # Fetch Data
        items = []
        if queue_type == "articles":
            items = article_svc.list_queue(filters=filters, limit=limit, offset=offset)
        elif queue_type == "events":
            items = event_svc.list_queue(filters=filters, limit=limit, offset=offset)
        elif queue_type == "proposals":
            items = event_svc.list_proposals(
                filters=filters, limit=limit, offset=offset
            )

        if items:
            df_items = pd.DataFrame(items)
            st.dataframe(df_items, use_container_width=True)
        else:
            st.warning("No items found.")

elif page == "System Health":
    st.title("System Health")

    with get_session() as session:
        health = HealthService(session)

        st.subheader("Blocking Analysis (403/429)")
        blocks = health.check_blocks()
        if "No obvious blocking" in blocks:
            st.success(blocks)
        else:
            st.error(blocks)

        # --- DB STATS ---
        st.subheader("🗄️ Database Stats")
        db_stats = health.get_db_stats()
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Articles", db_stats["articles"])
        d2.metric("Events", db_stats["events"])
        d3.metric("Newspapers", db_stats["newspapers"])
        d4.metric("Pending Jobs", db_stats["queue_articles"])

        st.divider()

        # --- SOURCE HEALTH ---
        st.subheader("📡 Source Status")
        sources = health.get_source_health()
        if sources:
            df_sources = pd.DataFrame(sources)
            st.dataframe(
                df_sources,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "status": st.column_config.TextColumn("Status"),
                    "minutes_ago": st.column_config.NumberColumn("Mins Ago"),
                    "count_24h": st.column_config.ProgressColumn(
                        "Vol (24h)",
                        min_value=0,
                        max_value=max([s["count_24h"] for s in sources] or [100]),
                    ),
                },
            )

        st.subheader("Maintenance")
        if st.button("Clear Completed Jobs"):
            from app.services.maintenance import MaintenanceService

            m_svc = MaintenanceService(session)
            res = m_svc.clear_completed_queues()
            st.success(res)
