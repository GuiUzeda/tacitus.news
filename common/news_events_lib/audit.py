from sqlalchemy import event, inspect
from sqlalchemy.orm import Session
from .models import BaseModel, AuditLogModel

# Ignore technical tables or log tables themselves
IGNORED_TABLES = {'audit_logs', 'articles_queue', 'events_queue', 'alembic_version'}

@event.listens_for(Session, 'after_flush')
def receive_after_flush(session, flush_context):
    """
    Automatic Audit Logging for every commit.
    """
    # We use a separate list to avoid modifying the session while iterating
    audit_entries = []

    # 1. Handle NEW records
    for obj in session.new:
        if obj.__tablename__ in IGNORED_TABLES: continue
        
        changes = {}
        state = inspect(obj)
        for attr in state.attrs:
            # For inserts, we just record the initial value
            if attr.history.has_changes():
                changes[attr.key] = [None, attr.value]
        
        audit_entries.append(AuditLogModel(
            target_table=obj.__tablename__,
            target_id=obj.id,
            action="INSERT",
            changes=changes
        ))

    # 2. Handle UPDATED records
    for obj in session.dirty:
        if obj.__tablename__ in IGNORED_TABLES: continue
        
        changes = {}
        state = inspect(obj)
        for attr in state.attrs:
            # history.added contains the NEW value
            # history.deleted contains the OLD value
            history = attr.history
            if history.has_changes():
                old_val = history.deleted[0] if history.deleted else None
                new_val = history.added[0] if history.added else None
                changes[attr.key] = [old_val, new_val]

        if changes:
            audit_entries.append(AuditLogModel(
                target_table=obj.__tablename__,
                target_id=obj.id,
                action="UPDATE",
                changes=changes
            ))

    # 3. Handle DELETED records
    for obj in session.deleted:
        if obj.__tablename__ in IGNORED_TABLES: continue
        
        audit_entries.append(AuditLogModel(
            target_table=obj.__tablename__,
            target_id=obj.id,
            action="DELETE",
            changes={"_dump": obj_to_dict(obj)} # Snapshot of what was lost
        ))

    # Save all audit logs
    # Note: We append to the flush context, so they get committed in the SAME transaction
    for entry in audit_entries:
        session.add(entry)

def obj_to_dict(obj):
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}