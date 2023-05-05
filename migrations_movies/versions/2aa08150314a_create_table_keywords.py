"""

create table keywords

Revision ID: 2aa08150314a
Creation date: 2023-05-05 12:05:27.408007

"""
from alembic import op, context


# revision identifiers, used by Alembic.
revision = '2aa08150314a'
down_revision = '03a94830a8ac'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    create table keywords(
    keyword_id serial primary key ,
    name text not null
    )
    """)


def downgrade() -> None:
    op.execute("""
        DROP TABLE IF EXISTS keywords CASCADE;
    """)
