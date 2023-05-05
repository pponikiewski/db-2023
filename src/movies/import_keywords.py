import asyncio
from asyncio import run, sleep, create_task

from db_class import DbService
from src.movies.analysis_tools import *


async def main():
    db = DbService()
    await db.initialize()  # tu łączymy się z bazą danych

    keywords = get_keywords()
    print(len(keywords))
    tasks = []

    for i, keyword in enumerate(keywords):
        tasks.append(create_task(db.upsert_keyword(keyword)))
        if i % 100 == 0:
            print(f'import in {i / len(keywords) * 100:.1f}% done')
            await asyncio.gather(*tasks)
            tasks = []

    await asyncio.gather(*tasks)
    await sleep(1)




if __name__ == '__main__':
    run(main())
