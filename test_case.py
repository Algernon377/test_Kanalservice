import time
from datetime import date
import httplib2
import requests
import xmltodict
from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2

from config import *


class SheetsFile:

    def __init__(self, credentials_file: str, spreadsheet_id: str):
        self.CREDENTIALS_FILE = credentials_file
        self.SPREADSHEET_ID = spreadsheet_id

    def create_httpAuth(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.CREDENTIALS_FILE,
            ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive'])
        httpAuth = credentials.authorize(httplib2.Http())
        return httpAuth

    def authorization(self):
        httpAuth = self.create_httpAuth()
        service = discovery.build('sheets', 'v4', http=httpAuth)
        return service

    def create_table(self, name_table: str, your_email: str, rowCount: int = 100, columnCount: int = 15):
        service = self.authorization()
        spreadsheet = service.spreadsheets().create(body={
            'properties': {'title': f"{name_table}", 'locale': 'ru_RU'},
            'sheets': [{'properties': {'sheetType': 'GRID',
                                       'sheetId': 0,
                                       'title': 'Лист номер один',
                                       'gridProperties': {'rowCount': f"{rowCount}", 'columnCount': f"{columnCount}"}}}]
        }).execute()
        spreadsheet_id = spreadsheet['spreadsheetId']
        self.SPREADSHEET_ID = spreadsheet_id

        httpAuth = self.create_httpAuth()
        driveService = discovery.build('drive', 'v3', http=httpAuth)
        driveService.permissions().create(
            fileId=self.SPREADSHEET_ID,
            body={'type': 'user', 'role': 'writer', 'emailAddress': f"{your_email}"},
            fields='id'                                                 # Открываем доступ на редактирование
        ).execute()
        print('https://docs.google.com/spreadsheets/d/' + self.SPREADSHEET_ID)

    def get_data_table(self, cell_range: str, sheets: str = 'Лист1', majorDimension: str = 'ROWS'):
        service = self.authorization()
        values = service.spreadsheets().values().get(
            spreadsheetId=self.SPREADSHEET_ID,
            range=f"{sheets}!{cell_range}",
            majorDimension=f"{majorDimension}"
        ).execute()
        values_list = values.get('values')
        values_list.pop(0)
        values_list_filter = filter(lambda x: all(x) and len(x) == 4, values.get('values'))
        values_list_red = tuple(map(self.date_converter, values_list_filter))
        return values_list_red

    def set_data_table_renge(self, data_list: list):
        service = self.authorization()
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.SPREADSHEET_ID,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": data_list
            }
        ).execute()

    @staticmethod
    def get_hash_sheets(sheets_tabl: tuple):
        tuple_sheets_table = tuple(map(tuple, sheets_tabl))
        return hash(tuple_sheets_table)

    @staticmethod
    def date_converter(row: list) -> tuple:
        row_date = row[3].split('.')
        row_date.reverse()
        row[3] = '-'.join(row_date)
        return tuple(row)


class DataBase:

    def __init__(self,
                 name_table: str,
                 host: str = POSTGRESQL_HOST,
                 user: str = POSTGRESQL_USER,
                 password: str = POSTGRESQL_PASSWORD,
                 db_name: str = POSTGRESQL_DB_NAME,
                 port: int = POSTGRESQL_PORT):
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name
        self.port = port
        self.name_table = name_table

    def command_executor(self, request_func):
        connection = None
        try:
            connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.db_name,
                port=self.port
            )
            connection.autocommit = True
            with connection.cursor() as cursor:
                if len(request_func) == 2:
                    req, tup = request_func
                    if type(tup[0]) == tuple:
                        cursor.executemany(req, tup)
                    else:
                        cursor.execute(req, tup)
                else:
                    cursor.execute(request_func)
                    if cursor.description:
                        cur_save_data = cursor.fetchall()
                        return cur_save_data
        except Exception as ex:
            print('Ошибка в работе PostgreSQL', ex)
        finally:
            if connection:
                connection.close()

    def create_table(self):
        create_table_command = f'''CREATE TABLE IF NOT EXISTS 
                                    {self.name_table} (Номер INTEGER,                                    
                                                        Заказ INTEGER PRIMARY KEY,
                                                        Cтоимость_USD INTEGER,
                                                        Cрок_поставки DATE
                                                    );'''
        self.command_executor(create_table_command)

    def drop_table(self):
        drop_table_command = f"DROP TABLE IF EXISTS {self.name_table};"
        self.command_executor(drop_table_command)

    def set_data(self, data_list: tuple):
        if not self.name_table:
            raise 'Внимание перед тем как добавить в таблицу данные, необходимо создать таблицу'
        set_new_data_command = (f'''INSERT INTO {self.name_table} 
                                VALUES (%s, %s, %s, %s)ON CONFLICT (Заказ) DO UPDATE SET 
                                Номер= EXCLUDED.Номер, 
                                Cтоимость_USD = EXCLUDED.Cтоимость_USD, 
                                Cрок_поставки = EXCLUDED.Cрок_поставки''',
                                [x for x in data_list])
        self.command_executor(set_new_data_command)
        print('База Данных обновленна')

    def get_all_data(self) -> tuple:
        currency_rate_usd = self.get_currency_rate()
        get_all_data_command = f"SELECT *, Cтоимость_USD*{currency_rate_usd} AS Cтоимость_RUB FROM {self.name_table};"
        all_data = tuple(self.command_executor(get_all_data_command))
        return all_data

    def get_one_columns_data(self, name_columns: str) -> tuple:
        get_one_columns_data_command = f"SELECT {name_columns} FROM {self.name_table};"
        one_columns_data = tuple(self.command_executor(get_one_columns_data_command))
        return one_columns_data

    def get_sum_rub(self) -> int:
        currency_rate_usd = self.get_currency_rate()
        get_sum_rub_command = f"SELECT SUM (Cтоимость_USD*{currency_rate_usd}) FROM {self.name_table};"
        get_one_columns_data = self.command_executor(get_sum_rub_command)
        return int(*get_one_columns_data[0])

    def get_overdue_order(self) -> list:
        currency_rate_usd = self.get_currency_rate()
        date_now = date.today()
        get_overdue_order_command = f'''SELECT *, Cтоимость_USD * {currency_rate_usd} AS Cтоимость_RUB 
                                FROM {self.name_table} 
                                WHERE Cрок_поставки < '{date_now}';'''
        all_data = self.command_executor(get_overdue_order_command)
        all_data_ret = [(row[1], int(row[2] * currency_rate_usd), str(row[3]), (date_now - row[3]).days) for row in
                        all_data]
        return all_data_ret

    def delete_data(self, order_id: int):
        delete_data_command = f"DELETE FROM {self.name_table} WHERE Заказ = {order_id};"
        self.command_executor(delete_data_command)

    def create_table_tg_users(self):
        create_table_command = f'''CREATE TABLE IF NOT EXISTS 
                                            {self.name_table} (user_id INTEGER PRIMARY KEY,                                    
                                                                user_name VARCHAR(50)
                                                            );'''
        self.command_executor(create_table_command)

    def set_users_db(self, id_user: int, user_name: str):
        set_new_data_command = (f'''INSERT INTO {self.name_table} 
                                VALUES (%s, %s)''',
                                (id_user, user_name))
        self.command_executor(set_new_data_command)

    def get_all_users_id(self):
        get_all_users_id_command = f"SELECT user_id FROM {self.name_table}"
        return self.command_executor(get_all_users_id_command)

    @staticmethod
    def get_currency_rate() -> float:
        date_now = date.today()
        formatted_date = date_now.strftime("%d/%m/%Y")
        url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={formatted_date}"
        response = requests.get(url)
        if response:
            xml_data = response.content.decode(response.encoding)
            dict_data = xmltodict.parse(xml_data)
            currency_list = dict_data['ValCurs']['Valute']
            value_currency = None
            for cur in currency_list:
                if cur['CharCode'] == 'USD':
                    value_currency = cur.get('Value', None).replace(',', '.')
                    break
            return float(value_currency)
        return 0


class ScriptManager:

    def __init__(self, credentials_file: str, spreadsheet_id: str, name_table_db: str):
        self.CREDENTIALS_FILE = credentials_file            # Ссылка на json ключ от google sheets
        self.SPREADSHEET_ID = spreadsheet_id                # Ссылка на таблицу google sheets
        self.NAME_TABLE_DB = name_table_db                  # Имя создаваемой(имеющейся) таблицы в Базе данных
        self.SELECT_RANGE = 'A1:F400'                       # Диапазон ячеек из таблицы google sheets откуда беруться значения
        self.sheets_object = SheetsFile(self.CREDENTIALS_FILE,
                                        self.SPREADSHEET_ID)  # Создаем обект через который будем работать с google sheets
        self.sheets_data_tuple = self.sheets_object.get_data_table(
            self.SELECT_RANGE)                              # Запрашиваем данные из таблицы с ячеек A1:F400
        self.db_object = DataBase(self.NAME_TABLE_DB)       # Создаем объект через который будем работать с нашей БД

    def create_settings(self):
        self.db_object.create_table()                       # Создаем таблицу в БД. Если она есть, то ничего не происходит
        self.db_object.set_data(self.sheets_data_tuple)     # Заносим в таблицу БД данные из таблицы google sheets

    def run_script(self):
        self.create_settings()
        self.sheets_table_hash = self.sheets_object.get_hash_sheets(
            self.sheets_data_tuple)                          # Записываем хэш для дальнейшего мониторинга изменений
        while True:
            self.sheets_data_tuple = self.sheets_object.get_data_table(self.SELECT_RANGE)
            self.new_sheets_table_hash = self.sheets_object.get_hash_sheets(self.sheets_data_tuple)
            print(self.sheets_table_hash == self.new_sheets_table_hash)
            if self.sheets_table_hash == self.new_sheets_table_hash:
                time.sleep(2)
                continue
            self.update_data_from_db()                        # Обновляем БД исходя из изменений в google sheets
            self.sheets_table_hash = self.new_sheets_table_hash
            time.sleep(2)

    def update_data_from_db(self):
        order_list_from_db = self.db_object.get_one_columns_data('Заказ')
        order_list_from_sheets = [int(_[1]) for _ in self.sheets_data_tuple]
        for order in order_list_from_db:
            if order[0] not in order_list_from_sheets:
                self.db_object.delete_data(order[0])
        self.db_object.set_data(self.sheets_data_tuple)


CREDENTIALS_FILE = 'my-project-kanalservice-a62e8270f0ca.json'      # Ссылка на json ключ от google sheets
SPREADSHEET_ID = '10wX6mOhTMMn7EdOEQ6B736hy65rgcJo_ajVt8ulEZDc'     # Ссылка на таблицу google sheets
NAME_TABLE_DB = 'Таблица_заказов'                                   # Имя создаваемой таблицы в БД

if __name__ == "__main__":
    a = ScriptManager(CREDENTIALS_FILE, SPREADSHEET_ID, NAME_TABLE_DB)
    a.run_script()
