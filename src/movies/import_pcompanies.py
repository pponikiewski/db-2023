import asyncio
from asyncio import run, sleep, create_task

from db_class import DbService
from src.movies.analysis_tools import *

async def main():
    db = DbService()
    await db.initialize()

    pcompanies = get_pcompanies()
    print(len(pcompanies))
    tasks = []

    for i, pcompany in enumerate(pcompanies):
        tasks.append(create_task(db.upsert_pcompany(pcompany)))
        if i % 100 == 0:
            print(f'import in {i / len(pcompanies) * 100:.1f}% done')
            await asyncio.gather(*tasks)
            tasks = []

    await asyncio.gather(*tasks)
    await sleep(1)
    print('all done')


if __name__ == '__main__':
    run(main())
