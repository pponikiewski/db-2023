import asyncio
from asyncio import run, sleep

from db_class import DbService
from src.movies.analysis_tools import *


async def main():
    db = DbService()
    await db.initialize()  # tu łączymy się z bazą danych

    keywords = get_movie_keywords('data/tmdb_5000_movies.csv')
    tasks = []

    for i, keyword in enumerate(keywords):
        tasks.append(asyncio.create_task(db.upsert_movie_keyword(keyword.keyword_id, keyword.movie_id)))
        if i % 100 == 0:
            print(f'import in {i / len(keywords) * 100:.1f}% done')
            await asyncio.gather(*tasks)
            tasks = []

    await asyncio.gather(*tasks)
    await sleep(1)
    print('all done')


if __name__ == '__main__':
    run(main())
