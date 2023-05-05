import asyncio
from asyncio import run, sleep

from db_class import DbService
from src.movies.analysis_tools import *


async def main():
    db = DbService()
    await db.initialize()  # tu łączymy się z bazą danych

    movie_pcompanies = get_movie_pcompanies('data/tmdb_5000_movies.csv')
    print(len(movie_pcompanies))

    tasks = []

    for i, pcompany in enumerate(movie_pcompanies):
        tasks.append(asyncio.create_task(db.upsert_movie_pcompany(pcompany)))
        if i % 100 == 0:
            await asyncio.gather(*tasks)
            print(f'import in {i / len(movie_pcompanies) * 100:.1f}% done')
            tasks = []
    await asyncio.gather(*tasks)


    await sleep(1)
    print('all done')

if __name__ == '__main__':
    run(main())
