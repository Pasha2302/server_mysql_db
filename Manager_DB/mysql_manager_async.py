import json
import time

from Auxiliary_Modules.toolbox import date_file, download_json_data
import aiomysql
from aiomysql.cursors import DictCursor


class BaseDB:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = None
        self.pool = None

    async def connect(self, database_name: str):
        self.database = database_name
        self.pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
            autocommit=True,
            cursorclass=DictCursor,  # Устанавливаем DictCursor (Получение данных из базы в формате dict().)
        )

    async def switch_database(self, database_name: str):
        # Метод для переключения на другую базу данных
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
        await self.connect(database_name)

    async def create_database(self, database):
        async with aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            autocommit=True,
        ) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")

    async def create_tables_all_casinos(self):
        if self.database is not None:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # SQL-запросы для создания таблиц
                    create_table_query = """
                    CREATE TABLE IF NOT EXISTS all_casinos (
                        id SERIAL PRIMARY KEY,
                        url_card VARCHAR(255) UNIQUE,
                        data JSON,
                        date_added DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                    )
                    """
                    await cur.execute(create_table_query)

    # async def Другие таблицы ...

    async def close(self):
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()


class WriteDB(BaseDB):
    async def insert_data_default(self, table, data):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                table_name = table
                column_names = ', '.join(data.keys())
                placeholders = ', '.join(['%s' for _ in data.values()])
                query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

                await cur.execute(query, tuple(data.values()))

    async def insert_all_casino_data(self, url, casino_data):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Проверьте, существует ли запись с такой ссылкой
                query = "SELECT id FROM all_casinos WHERE url_card = %s"
                await cur.execute(query, (url,))
                existing_record = await cur.fetchone()

                if existing_record:
                    # Если запись с такой ссылкой уже существует, вы можете выполнить обновление данных или
                    # выполнить другие действия по вашему усмотрению
                    # Пример обновления данных:
                    query = "UPDATE all_casinos SET data = %s, date_added = %s WHERE url_card = %s"
                    await cur.execute(query, (json.dumps(casino_data), date_file(time_int=time.time()), url))
                else:
                    # Если запись с такой ссылкой не существует, вставьте новые данные
                    query = "INSERT INTO all_casinos (url_card, data) VALUES (%s, %s)"
                    await cur.execute(query, (url, json.dumps(casino_data)))


class ReadDB(BaseDB):
    async def select_data(self, table, columns, condition=None):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                cur: aiomysql.Cursor
                query = f"SELECT {', '.join(columns)} FROM {table}"
                if condition:
                    query += f" WHERE {condition}"
                await cur.execute(query)
                result = await cur.fetchall()
                return result


# Пример использования:
async def main():
    name_db_casino_guru = 'casino_guru'
    table_name_all_casinos = 'all_casinos'

    write_db = WriteDB(host='127.0.0.1', port=3306, user='pavelpc', password='1234')
    await write_db.create_database(name_db_casino_guru)
    await write_db.connect(name_db_casino_guru)
    await write_db.create_tables_all_casinos()

    read_db = ReadDB(host='127.0.0.1', port=3306, user='pavelpc', password='1234')
    await read_db.connect(name_db_casino_guru)

    data_casinos_json = download_json_data(path_file="all_casinos.json")
    tasks_insert = []
    start_time = time.time()
    for data_casino in data_casinos_json:
        # await write_db.insert_all_casino_data(url=data_casino['url_card'], casino_data=data_casino)
        tasks_insert.append(asyncio.create_task(
            write_db.insert_all_casino_data(url=data_casino['url_card'], casino_data=data_casino))
        )

    await asyncio.gather(*tasks_insert)
    print(f"Запись данных выполнена за: {(time.time() - start_time):.4f} sec.")

    datas = await read_db.select_data(table=table_name_all_casinos, columns=['*'])

    for data in datas[:3]:
        data['date_added'] = data['date_added'].strftime("%Y-%m-%d %H:%M:%S.%f")
        print(data)
        print('--' * 60)
    print(f"{len(datas)=}")

    await write_db.close()
    await read_db.close()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
