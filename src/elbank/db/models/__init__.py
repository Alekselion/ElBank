"""ORM models.

Import every model module here so ``Base.metadata`` is fully populated
for Alembic autogenerate (``alembic/env.py`` imports this package).
"""

from elbank.db.base import Base

__all__ = ["Base"]
