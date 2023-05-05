"""

create table movie_keywords

Revision ID: 5bb12bcf274e
Creation date: 2023-05-05 12:12:58.123874

"""
from alembic import op, context


# revision identifiers, used by Alembic.
revision = '5bb12bcf274e'
down_revision = '2aa08150314a'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE movie_keywords(
    movie_id int references movies(movie_id) on delete cascade,
    keyword_id int references keywords(keyword_id) on delete cascade
);
    """)


def downgrade() -> None:
    op.execute("""
    DROP TABLE IF EXISTS movie_keywords CASCADE;
    """)



