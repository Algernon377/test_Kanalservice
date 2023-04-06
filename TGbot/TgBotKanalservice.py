import asyncio
import aioschedule
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

from test_case import DataBase, NAME_TABLE_DB

TOKEN = '6240874670:AAGud-LOxO-9g494sfIurHd94Hk8YZdLnEQ'
bot = Bot(TOKEN)
dp = Dispatcher(bot)
data_base_order = None
data_base_users = None
TIME_SEND = '23:59'

async def on_startup(_):
    print('online')
    global data_base_order
    data_base_order = DataBase(NAME_TABLE_DB)
    global data_base_users
    data_base_users = DataBase('users_tg')
    data_base_users.create_table_tg_users()
    asyncio.create_task(scheduler())


msg_start = f'''Добро пожаловать в бот Каналсервиса. Каждое утро в {TIME_SEND} по МСК он будет присылать список просроченных
заказов по следующей форме (номер заказа, стоимость заказа в рублях, дата поставки, на сколько дней просрочен)
Так же есть команды:
/Текущий_курс_USD - Получить текущий курс USD согласно ЦБ
/Сумма_контрактов - Сумма всех текущих заказов в рублях
/Просроченная_поставка - Список всех просроченных заказов
'''

b1 = KeyboardButton('/Текущий_курс_USD')
b2 = KeyboardButton('/Сумма_контрактов')
b3 = KeyboardButton('/Просроченная_поставка')
b4 = KeyboardButton('/start')

kb_klient = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
kb_klient.row(b1, b2, b3).add(b4)


@dp.message_handler(commands=['start', 'help'])
async def command_start(message: types.Message):
    users = data_base_users.get_all_users_id()
    if (message.from_user.id,) not in users:
        data_base_users.set_users_db(message.from_user.id, message.from_user.username)
    await bot.send_message(message.from_user.id, msg_start, reply_markup=kb_klient)


@dp.message_handler(commands=['Текущий_курс_USD'])
async def command_start(message: types.Message):
    currency = data_base_order.get_currency_rate()
    msg_currency = f"Текущий курс рубля к доллару согласно ЦБ {currency}руб за 1 USD"
    await bot.send_message(message.from_user.id, msg_currency, reply_markup=kb_klient)


@dp.message_handler(commands=['Сумма_контрактов'])
async def command_start(message: types.Message):
    sum_rub = data_base_order.get_sum_rub()
    msg_sum_rub = f"Сумма всех контрактов {sum_rub} рублей"
    await bot.send_message(message.from_user.id, msg_sum_rub, reply_markup=kb_klient)


@dp.message_handler(commands=['Просроченная_поставка'])
async def command_overdue_mes(message: types.Message):
    prosrocheniy_contr = data_base_order.get_overdue_order()
    mes = ''.join([f"Заказ:{x[0]}, стоимость {x[1]} руб, дата поставки:{x[2]}, опаздывает на {x[3]} дней; \n" for x in
                   prosrocheniy_contr])
    await bot.send_message(message.from_user.id, mes, reply_markup=kb_klient)


async def command_time_messages():
    users = data_base_users.get_all_users_id()
    prosrocheniy_contr = data_base_order.get_overdue_order()
    mes = ''.join([f"Заказ:{x[0]}, стоимость {x[1]} руб, дата поставки:{x[2]}, опаздывает на {x[3]} дней; \n" for x in
                   prosrocheniy_contr])
    for user_id in users:
        await bot.send_message(chat_id=user_id[0], text=mes)


@dp.message_handler()
async def echo_send(message: types.Message):
    await message.answer('могу работать только с определенными командами', reply_markup=kb_klient)


async def scheduler():
    aioschedule.every().day.at(TIME_SEND).do(command_time_messages)
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
