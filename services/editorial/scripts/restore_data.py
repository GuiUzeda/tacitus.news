import sys
import os
import json
import uuid
from datetime import datetime
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text, insert
from sqlalchemy.orm import sessionmaker

from config import Settings
from news_events_lib.models import (
    NewspaperModel, FeedModel, AuthorModel, 
    NewsEventModel, ArticleModel, ArticleContentModel, 
    MergeProposalModel, article_author_association
)

settings = Settings()
engine = create_engine(str(settings.pg_dsn))
SessionLocal = sessionmaker(bind=engine)

DUMP_DIR = Path("dump_data")

def load_json(filename):
    filepath = DUMP_DIR / f"{filename}.json"
    if not filepath.exists():
        print(f"⚠️  Arquivo {filepath} não encontrado. Pulando.")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def clean_db(session):
    print("🧹 Limpando banco de dados...")
    # Ordem de remoção (reversa à criação para respeitar FKs)
    tables = [
        "merge_proposals", "article_contents", "article_authors", 
        "articles", "news_events", "authors", "feeds", "newspapers"
    ]
    for table in tables:
        session.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
    session.commit()

def restore_table(session, model, filename, data_corrector=None):
    records = load_json(filename)
    if not records: return

    print(f"📥 Restaurando {filename} ({len(records)} registros)...")
    
    # Inserção em massa (Bulk Insert) é mais rápida e segura para IDs manuais
    # Precisamos tratar os dados antes de inserir (converter strings ISO para datetime)
    processed_records = []
    for rec in records:
        if data_corrector:
            rec = data_corrector(rec)
        processed_records.append(rec)

    # Chunking para não estourar memória em bancos grandes
    CHUNK_SIZE = 1000
    for i in range(0, len(processed_records), CHUNK_SIZE):
        chunk = processed_records[i:i+CHUNK_SIZE]
        session.execute(insert(model), chunk)
    
    session.commit()

def restore_events(session):
    """
    Eventos têm auto-referência (merged_into_id).
    Estratégia:
    1. Inserir todos os eventos com merged_into_id = NULL
    2. Atualizar merged_into_id num segundo passo.
    """
    records = load_json("news_events")
    if not records: return

    print(f"📥 Restaurando news_events ({len(records)} registros)...")

    # Passo 1: Inserir sem merged_into_id
    deferred_merges = []
    
    for rec in records:
        merged_into = rec.get("merged_into_id")
        if merged_into:
            deferred_merges.append({"id": rec["id"], "merged_into_id": merged_into})
            rec["merged_into_id"] = None # Remove temporariamente
        
        # Correção de datas e vetores se necessário (SQLAlchemy costuma lidar bem, mas UUID precisa ser string)
        # O driver asyncpg/psycopg2 lida com strings UUID automaticamente.
    
    session.execute(insert(NewsEventModel), records)
    session.commit()

    # Passo 2: Restaurar as fusões
    if deferred_merges:
        print(f"🔗 Reconectando {len(deferred_merges)} eventos fundidos...")
        for item in deferred_merges:
            session.execute(
                text("UPDATE news_events SET merged_into_id = :mid WHERE id = :id"),
                {"mid": item["merged_into_id"], "id": item["id"]}
            )
        session.commit()

def main():
    session = SessionLocal()
    try:
        # 1. Limpeza
        clean_db(session)

        # 2. Restaurar Tabelas Independentes
        restore_table(session, NewspaperModel, "newspapers")
        restore_table(session, FeedModel, "feeds")
        restore_table(session, AuthorModel, "authors")

        # 3. Restaurar Eventos (Lógica Especial)
        restore_events(session)

        # 4. Restaurar Dependentes
        restore_table(session, ArticleModel, "articles")
        restore_table(session, ArticleContentModel, "article_contents")
        
        # 5. Restaurar Muitos-para-Muitos e Propostas
        restore_table(session, article_author_association, "article_authors")
        restore_table(session, MergeProposalModel, "merge_proposals")

        print("\n✅ Restauração completa!")
        
    except Exception as e:
        print(f"\n❌ Erro na restauração: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()