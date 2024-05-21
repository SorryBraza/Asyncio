import asyncio
import aiohttp
from more_itertools import chunked
from models import Session, DeclarativeBase, PG_DSN, SwapiPeople, init_db

MAX_CHUNK = 10


async def extract_names(list_data):
    list_names = []
    async with aiohttp.ClientSession() as client_session:
        for data in list_data:
            response = await client_session.get(data)
            response_json = await response.json()
            list_names.append(response_json[list(response_json.keys())[0]])
    return ','.join(list_names)


async def insert_people(people_list):
    async with Session() as session:
        async with session.begin():
            for person in people_list:
                if person.get('birth_year'):
                    person_data = {
                        'birth_year': person['birth_year'],
                        'eye_color': person['eye_color'],
                        'films': await extract_names(person['films']),
                        'gender': person['gender'],
                        'hair_color': person['hair_color'],
                        'height': person['height'],
                        'homeworld': person['homeworld'],
                        'mass': person['mass'],
                        'name': person['name'],
                        'skin_color': person['skin_color'],
                        'species': await extract_names(person['species']),
                        'starships': await extract_names(person['starships']),
                        'vehicles': await extract_names(person['vehicles'])
                    }
                    new_person = SwapiPeople(**person_data)
                    session.add(new_person)


async def get_person(session, person_id):
    async with aiohttp.ClientSession() as client_session:
        async with client_session.get(f"https://swapi.dev/api/people/{person_id}/") as response:
            return await response.json()


async def main():
    await init_db()
    tasks = []
    async with Session() as session:
        for person_id_chunk in chunked(range(1, 100), MAX_CHUNK):
            coros = [get_person(session, person_id) for person_id in person_id_chunk]
            results = await asyncio.gather(*coros)
            tasks.append(insert_people(results))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
