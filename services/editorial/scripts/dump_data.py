import sys
import os
import json
import uuid
from datetime import datetime
from pathlib import Path

# Adiciona o diretório raiz ao path para encontrar os módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from config import Settings
from news_events_lib.models import (
    NewspaperModel, FeedModel, AuthorModel, 
    NewsEventModel, ArticleModel, ArticleContentModel, 
    MergeProposalModel, article_author_association
)

# Configuração
settings = Settings()
engine = create_engine(str(settings.pg_dsn))
SessionLocal = sessionmaker(bind=engine)

DUMP_DIR = Path("dump_data")
DUMP_DIR.mkdir(exist_ok=True)

def json_serializer(obj):
    """Serializador customizado para UUID, Datetime e Bytes"""
    if isinstance(obj, (datetime)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    return str(obj)

def dump_table(session, model, filename):
    print(f"📦 Dumping {filename}...")
    
    # Se for uma Tabela de Associação (não é uma classe)
    if hasattr(model, 'c'): 
        stmt = select(model)
        rows = session.execute(stmt).all()
        # Converte RowProxy para dict
        data = [dict(row._mapping) for row in rows]
    else:
        # Se for um Modelo ORM
        objs = session.scalars(select(model)).all()
        data = []
        for obj in objs:
            # Pega os dados brutos, remove o estado interno do SQLAlchemy
            record = obj.__dict__.copy()
            record.pop('_sa_instance_state', None)
            data.append(record)
            
    filepath = DUMP_DIR / f"{filename}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, default=json_serializer, ensure_ascii=False, indent=2)
    
    print(f"✅ Saved {len(data)} records to {filepath}")

def main():
    session = SessionLocal()
    try:
        # Ordem não importa no Dump, mas vamos manter organizado
        dump_table(session, NewspaperModel, "newspapers")
        dump_table(session, FeedModel, "feeds")
        dump_table(session, AuthorModel, "authors")
        dump_table(session, NewsEventModel, "news_events")
        dump_table(session, ArticleModel, "articles")
        dump_table(session, ArticleContentModel, "article_contents")
        dump_table(session, MergeProposalModel, "merge_proposals")
        
        # Tabela de Associação (Muitos-para-Muitos)
        dump_table(session, article_author_association, "article_authors")
        
        print("\n🎉 Dump concluído com sucesso na pasta 'dump_data/'")
    finally:
        session.close()

if __name__ == "__main__":
    main()