"""

create table pcompanies

Revision ID: 0f524bf2cd00
Creation date: 2023-05-05 15:21:31.774414

"""
from alembic import op, context


# revision identifiers, used by Alembic.
revision = '0f524bf2cd00'
down_revision = '5bb12bcf274e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    create table pcompanies(
    pcompany_id serial primary key,
    name text not null
    )
    """)


def downgrade() -> None:
    op.execute(f"""
        DROP TABLE IF EXISTS pcompanies CASCADE;
    """)