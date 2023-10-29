import asyncio
import time

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

from starlette.staticfiles import StaticFiles
from starlette.routing import Route, Mount
from starlette.templating import Jinja2Templates
import uvicorn

from Manager_DB import *

templates = Jinja2Templates(directory='templates')


async def create_database_writer():
    write_db = WriteDB(host='127.0.0.1', port=3306, user='pavelpc', password='1234')
    app.state.write_db: WriteDB = write_db


async def create_database_reader():
    read_db = ReadDB(host='127.0.0.1', port=3306, user='pavelpc', password='1234')
    app.state.read_db: ReadDB = read_db


async def destroy_database_writer():
    write_db: WriteDB = app.state.write_db
    await write_db.close()

async def destroy_database_reader():
    read_db: ReadDB = app.state.read_db
    await read_db.close()
# =================================================================================================================== #


async def get_data_db_casino_guru_all_casinos(request: Request):
    table_name_all_casinos = 'all_casinos'
    name_db_casino_guru = 'casino_guru'

    try:
        read_db: ReadDB = request.app.state.read_db
        await read_db.connect(name_db_casino_guru)
        datas = await read_db.select_data(table=table_name_all_casinos, columns=['*'])

        json_data_to_send = []
        for data in datas:
            data['date_added'] = data['date_added'].strftime("%Y-%m-%d %H:%M:%S.%f")
            # print(data)
            # print('--' * 60)
            json_data_to_send.append(data)

        print("message: Данные успешно отправлены")
        print(f"{len(json_data_to_send)=}")
        return JSONResponse(json_data_to_send)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def add_data_db_casino_guru_all_casinos(request: Request):
    table_name_all_casinos = 'all_casinos'
    name_db_casino_guru = 'casino_guru'
    try:
        data_casinos_json = await request.json()
        print(f"[add_data_db_casino_guru_all_casinos]:\ndata_casinos_json:\n{data_casinos_json}")
        # Здесь можно добавить валидацию данных, например, проверку на наличие обязательных полей

        # Здесь можно вызвать метод вашей базы данных для записи данных
        # Пример: write_db.create_item(data)
        # -------------------------------------------------------------------------------------------
        write_db: WriteDB = request.app.state.write_db
        await write_db.create_database(name_db_casino_guru)
        await write_db.connect(name_db_casino_guru)
        await write_db.create_tables_all_casinos()

        tasks_insert = []
        start_time = time.time()
        for data_casino in data_casinos_json:
            # await write_db.insert_all_casino_data(url=data_casino['url_card'], casino_data=data_casino)
            tasks_insert.append(asyncio.create_task(
                write_db.insert_all_casino_data(url=data_casino['url_card'], casino_data=data_casino))
            )

        await asyncio.gather(*tasks_insert)
        print(f"Запись данных в db: {name_db_casino_guru} / Table: {table_name_all_casinos}\n"
              f"выполнена за: {(time.time() - start_time):.4f} sec.")

        return JSONResponse({"message": "Данные успешно записаны в базу данных"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def homepage(request: Request):
    # is_disconnected = await request.is_disconnected()
    # print(f"{is_disconnected=}")

    template = "index.html"
    context = {"request": request}
    return templates.TemplateResponse(template, context)


async def error(request):
    """
    An example error. Switch the `debug` setting to see either tracebacks or 500 pages.
    """
    raise RuntimeError("Oh no")


async def not_found(request: Request, exc: HTTPException):
    """
    Return an HTTP 404 page.
    """
    template = "404.html"
    context = {"request": request}
    return templates.TemplateResponse(template, context, status_code=404)
    # return HTMLResponse("<h1>Not Found</h1>", status_code=404)


async def server_error(request: Request, exc: HTTPException):
    """
    Return an HTTP 500 page.
    """
    template = "500.html"
    context = {"request": request}
    return templates.TemplateResponse(template, context, status_code=500)
    # return HTMLResponse("<h1>Internal Server Error</h1>", status_code=500)


routes = [
    Route('/', homepage),
    Route('/add_data_db_casino_guru_all_casinos', add_data_db_casino_guru_all_casinos, methods=['POST', ]),
    Route('/get_data_db_casino_guru_all_casinos', get_data_db_casino_guru_all_casinos, methods=['GET', ]),
    Route('/error', error),
    Mount('/static', app=StaticFiles(directory='statics'), name='static')
]

exception_handlers = {
    404: not_found,
    500: server_error
}

app = Starlette(
    debug=True, routes=routes,
    exception_handlers=exception_handlers,
    on_startup=[create_database_writer, create_database_reader],
    on_shutdown=[destroy_database_writer, destroy_database_reader]
)


if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=8000)
    
