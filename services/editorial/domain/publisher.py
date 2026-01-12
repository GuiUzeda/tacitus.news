import math
from datetime import datetime, timezone
from typing import Tuple, List, Dict
from loguru import logger
from sqlalchemy.orm import Session
from news_events_lib.models import NewsEventModel, EventStatus, JobStatus
from config import Settings

class NewsPublisherDomain:
    def __init__(self):
        self.settings = Settings()
        
        # Pesos para o Algoritmo
        self.WEIGHT_EDITORIAL = 2.0  # Autoridade da Fonte
        self.WEIGHT_VOLUME = 4.0     # Quantidade de Artigos
        self.WEIGHT_RECENCY = 40.0   # Bônus de Notícia Fresca
        
        # Limiares
        self.HERO_THRESHOLD = 80.0       # Score para ser Manchete
        self.BLIND_SPOT_VOL_MIN = 30.0   # Score mínimo para considerar um "Blind Spot" relevante

    def publish_event_job(self, session: Session, job) -> bool:
        """
        Processa um job da fila de Publicação.
        Retorna True se processado com sucesso, False se ignorado.
        """
        event = job.event
        
        # --- RACE CONDITION CHECK A (Early Exit) ---
        # Se o evento foi fundido enquanto estava na fila, ele estará inativo.
        if not event or not event.is_active:
            logger.warning(f"👻 Publisher ignorou evento morto/fundido: {job.event_id}")
            job.status = JobStatus.COMPLETED 
            job.msg = "Event merged/deleted before publish"
            return False

        # 1. Calcular Score e Insights (A Lógica Ground News)
        hot_score, insights, metadata = self.calculate_spectrum_score(event)
        
        # 2. Quality Gates (Filtros)
        # Se for "Breaking" ou "Blind Spot", passa direto pelo filtro de volume
        is_special = "BREAKING" in insights or "BLIND_SPOT" in insights
        
        if not is_special and event.article_count < 2:
            # Notícia fraca e sem relevância especial. Ignora silenciosamente.
            # Não falhamos o job, apenas deixamos ele lá para tentar de novo se crescer.
            job.status = JobStatus.WAITING
            job.msg = "Low Volume: Waiting for more articles"
            return False

        # 3. Executar Publicação
        self._execute_publish(session, event, job, hot_score, insights, metadata)
        return True

    def calculate_spectrum_score(self, event: NewsEventModel) -> Tuple[float, List[str], Dict]:
        """
        Retorna: (Score Numérico, Lista de Tags, Metadados de Viés)
        """
        score = 0.0
        insights = []
        
        # A. Editorial Authority (O peso dos Jornais)
        # Herdado do Cluster (Soma dos Ranks + Tiers)
        score += (event.editorial_score * self.WEIGHT_EDITORIAL)
        
        # B. Volume (Logarítmico)
        # Evita que 1000 artigos distorçam o gráfico. 10 artigos = ~23 pts. 
        score += (math.log1p(event.article_count) * self.WEIGHT_VOLUME)

        # C. Recency (Usa a data real da primeira notícia)
        ref_date = event.first_article_date or event.created_at
        if ref_date.tzinfo is None: ref_date = ref_date.replace(tzinfo=timezone.utc)
        
        age_hours = (datetime.now(timezone.utc) - ref_date).total_seconds() / 3600.0
        
        if age_hours < 4.0:
            score += self.WEIGHT_RECENCY
            insights.append("BREAKING") # Tag para o Frontend piscar
        elif age_hours > 48.0:
            score -= (age_hours * 0.5) # Penalidade de Arquivo

        # D. SPECTRUM ANALYSIS (Ground News Logic)
        bias_counts = event.article_counts_by_bias or {}
        # Filtra apenas o espectro político
        sides = {k: v for k, v in bias_counts.items() if k in ['left', 'center', 'right'] and v > 0}
        total_sides = len(sides)
        
        # Mapa de tradução para Insights
        missing_sides = {'left', 'center', 'right'} - sides.keys()

        if total_sides == 3:
            # CENÁRIO 1: Consenso / Debate Nacional
            score *= 1.8 
            insights.append("FULL_SPECTRUM")
            
        elif total_sides == 2:
            # CENÁRIO 2: Contraponto (Saudável)
            score *= 1.2
            
        elif total_sides == 1:
            # CENÁRIO 3: Bolha Unilateral
            dominant_side = list(sides.keys())[0]
            
            # Precisamos distinguir "Fofoca de Nicho" vs "Ponto Cego Importante"
            # Se o score base for alto (>30), significa que Jornais Grandes estão falando.
            if score > self.BLIND_SPOT_VOL_MIN:
                score *= 1.5 # Boost para forçar a aparição no feed geral
                
                # Gera a tag inversa: Se só a Esquerda fala, é um Blind Spot para a Direita?
                # Ou simplificamos: "Coverage: Only Left"
                insights.append(f"ONLY_{dominant_side.upper()}")
                
                # Tag específica de Blind Spot (Alerta de Viés)
                insights.append("BLIND_SPOT")
            else:
                # É apenas ruído de nicho. Penaliza.
                score *= 0.6 
                insights.append("NICHE")

        return round(score, 2), insights, {"bias_counts": bias_counts}

    def _execute_publish(self, session: Session, event: NewsEventModel, job, score, insights, metadata):
        # --- RACE CONDITION CHECK B (Late Check) ---
        # Recarrega o evento para garantir que ninguém o fundiu durante o cálculo
        session.refresh(event)
        if not event.is_active:
            return

        event.status = EventStatus.PUBLISHED
        event.hot_score = score
        
        # Salvar insights no banco (campo summary ou novo campo metadata)
        # Aqui, vamos injetar no campo 'interest_counts' ou criar um campo específico no futuro.
        # Por enquanto, assumimos que o frontend vai recalcular ou usamos um campo JSON existente.
        if event.summary and isinstance(event.summary, dict):
            summary_update = dict(event.summary)
            summary_update["insights"] = insights
            event.summary = summary_update
        
        if not event.published_at:
             event.published_at = datetime.now(timezone.utc)

        job.status = JobStatus.COMPLETED
        job.msg = f"Published (Score: {score:.1f}) | Tags: {insights}"
        
        logger.success(f"🚀 {event.title[:30]}... | Score: {score} | {insights}")