import asyncio
import aiohttp
import datetime
from more_itertools import chunked
from models import Session, DeclarativeBase, PG_DSN, SwapiPeople, init_db

MAX_CHUNK = 10


async def extract_names(session, list_data):
    list_names = []
    for data in list_data:
        response = await session.get(data)
        response_json = await response.json()
        list_names.append(response_json[list(response_json.keys())[0]])
    return ','.join(list_names)


async def insert_people(session, people_list):
    person_data = [SwapiPeople(
                birth_year=person['birth_year'],
                eye_color=person['eye_color'],
                films=await extract_names(session, person['films']),
                gender=person['gender'],
                hair_color=person['hair_color'],
                height=person['height'],
                homeworld=person['homeworld'],
                mass=person['mass'],
                name=person['name'],
                skin_color=person['skin_color'],
                species=await extract_names(session, person['species']),
                starships=await extract_names(session, person['starships']),
                vehicles=await extract_names(session, person['vehicles'])) for person in people_list if person.get("birth_year")]
    async with Session() as db_session:
        db_session.add_all(person_data)
        await db_session.commit()


async def get_person(session, person_id):
    async with session.get(f"https://swapi.dev/api/people/{person_id}/") as response:
        return await response.json()


async def main():
    await init_db()
    session = aiohttp.ClientSession()
    for person_id_chunk in chunked(range(1, 100), MAX_CHUNK):
        coros = [get_person(session, person_id) for person_id in person_id_chunk]
        results = await asyncio.gather(*coros)
        await asyncio.create_task(insert_people(session, results))
    await session.close()
    all_tasks_set = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*all_tasks_set)


if __name__ == "__main__":
    start = datetime.datetime.now()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    # asyncio.run(main())
    print(datetime.datetime.now() - start)