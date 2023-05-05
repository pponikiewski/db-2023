"""

create table movie_pcompanies

Revision ID: 0b76d74faf72
Creation date: 2023-05-05 15:46:22.442263

"""
from alembic import op, context


# revision identifiers, used by Alembic.
revision = '0b76d74faf72'
down_revision = '0f524bf2cd00'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE movie_pcompanies(
    movie_id int references movies(movie_id) on delete cascade,
    pcompany_id int references pcompanies(pcompany_id) on delete cascade
    );
    """)


def downgrade() -> None:
    op.execute("""
    DROP TABLE IF EXISTS movie_pcompanies CASCADE;
    """)