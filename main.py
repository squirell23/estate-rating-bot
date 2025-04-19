import logging
import asyncio
import re
import math
import os
import aiohttp
import psycopg2
from aiogram.types.input_media_photo import InputMediaPhoto
from psycopg2.extras import RealDictCursor
import matplotlib.pyplot as plt
import numpy as np

from aiogram import Bot, Dispatcher, Router
from aiogram.enums import ParseMode
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
)
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

# ПАРАМЕТРЫ
API_TOKEN = ''
DB_HOST = ''
DB_NAME = ''
DB_USER = ''
DB_PASSWORD = ''

logging.basicConfig(level=logging.INFO)

# Создаём бота
bot = Bot(
    token=API_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
)

# Создаём диспетчер и роутер
router = Router()
dp = Dispatcher(bot=bot)
dp.include_router(router)

# Хранилище последних 5 запросов на пользователя
user_queries = {}  # user_id -> list[(lat, lon, radius, address)]

def add_user_query(user_id: int, lat: float, lon: float, radius: float, address: str):
    if user_id not in user_queries:
        user_queries[user_id] = []
    user_queries[user_id].insert(0, (lat, lon, radius, address))
    if len(user_queries[user_id]) > 5:
        user_queries[user_id].pop()

# Подключение к БД
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# Запрос информации о доме (включает дополнительные статистические данные)
def query_building_info(lat: float, lon: float, radius: float):
    if radius <= 0:
        radius = 1000

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    q = """
    SELECT b.building_id,
           b.address AS name,
           ROUND(br.total_score::numeric,2) AS total_score,
           ROUND(br.social_score::numeric,2) AS social_score,
           ROUND(br.quality_score::numeric,2) AS quality_score,
           ROUND(br.transport_score::numeric,2) AS transport_score,
           b.build_year,
           b.floors_number,
           b.is_emergency,
           b.square,
           b.apartments_number,
           b.building_type_id,
           b.living_area,
           b.not_living_area,
           b.is_cultural_heritage,
           b.latitude,
           b.longitude,
           ST_Distance(
               b.geom::geography,
               ST_SetSRID(ST_MakePoint(%s, %s),4326)::geography
           ) AS dist,
           ST_X(b.geom) AS geom_lon,
           ST_Y(b.geom) AS geom_lat
    FROM building b
    JOIN building_ratings br ON br.building_id = b.building_id
    ORDER BY ST_Distance(
             b.geom::geography,
             ST_SetSRID(ST_MakePoint(%s, %s),4326)::geography)
    LIMIT 1;
    """
    cur.execute(q, (lon, lat, lon, lat))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return None

    building_id = row['building_id']
    r_int = int(radius) if radius > 0 else 1000

    oq = """
    WITH center AS (
        SELECT geom::geography AS geog
        FROM building
        WHERE building_id = %s
    )
    SELECT 'Школа' AS type, s.name
    FROM school s, center
    WHERE ST_DWithin(center.geog, s.geom::geography, %s)

    UNION ALL
    SELECT 'Детский сад' AS type, k.name
    FROM kindergarten k, center
    WHERE ST_DWithin(center.geog, k.geom::geography, %s)

    UNION ALL
    SELECT 'Больница' AS type, h.name
    FROM hospital h, center
    WHERE ST_DWithin(center.geog, h.geom::geography, %s)

    UNION ALL
    SELECT 'Парк' AS type, p.name
    FROM park p, center
    WHERE ST_DWithin(center.geog, p.geom::geography, %s)
    ORDER BY type, name;
    """
    cur.execute(oq, (building_id, r_int, r_int, r_int, r_int))
    obs = cur.fetchall()

    cur.close()
    conn.close()

    row['objects'] = obs
    return row

def generate_comparison_plot(res1, res2, addr1, addr2):
    categories = [
        'total_score', 'social_score', 'quality_score', 'transport_score',
        'build_year', 'floors_number', 'square', 'apartments_number',
        'living_area', 'not_living_area'
    ]
    labels = [
        'Общий рейтинг', 'Соц. оценка', 'Качество', 'Транспорт',
        'Год постройки', 'Этажей', 'Площадь', 'Квартир',
        'Жилая пл.', 'Нежилая пл.'
    ]
    vals1 = [float(res1.get(c, 0) or 0) for c in categories]
    vals2 = [float(res2.get(c, 0) or 0) for c in categories]

    x = np.arange(len(labels))
    width = 0.35

    plt.figure(figsize=(10, 5))
    plt.bar(x - width/2, vals1, width, label=addr1[:20], color='skyblue')
    plt.bar(x + width/2, vals2, width, label=addr2[:20], color='salmon')
    plt.xticks(x, labels, rotation=45, ha='right')
    plt.title("Сравнение домов по показателям")
    plt.tight_layout()

    fname = "house_comparison.png"
    plt.savefig(fname)
    plt.close()
    return fname
    
# Геокодинг адреса через Nominatim
async def geocode_address(addr: str):
    url = f"https://nominatim.openstreetmap.org/search?format=json&q={addr}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={"User-Agent": "GeoBot"}) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            if not data:
                return None
            lat = float(data[0]['lat'])
            lon = float(data[0]['lon'])
            return lat, lon

# Основная клавиатура
def main_menu_kb() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="Ввести координаты"), KeyboardButton(text="Ввести адрес")],
        [KeyboardButton(text="Отправить локацию", request_location=True)],
        [KeyboardButton(text="Сравнить дома"), KeyboardButton(text="Мои запросы")],
        [KeyboardButton(text="Топ-10"), KeyboardButton(text="Распределение")],
        [KeyboardButton(text="О рейтинге"), KeyboardButton(text="О нас")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# Хендлеры (через router)

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я бот для оценки домов!\n"
        "Введи координаты `<lat>, <lon>, <radius>` или адрес `адрес: ...`,\n"
        "или нажми кнопки ниже.",
        reply_markup=main_menu_kb()
    )

@router.message(lambda msg: msg.text == "Ввести координаты")
async def ask_coords(message: Message):
    await message.answer(
        "Введите координаты, например:\n"
        "`55.7522, 37.6156, 1000`\n"
        "где третий параметр — радиус (по умолчанию 1000).",
        parse_mode=ParseMode.MARKDOWN
    )

@router.message(lambda msg: msg.text == "Ввести адрес")
async def ask_address(message: Message):
    await message.answer(
        "Введите адрес в формате:\n"
        "`адрес: Москва, Тверская, 12`",
        parse_mode=ParseMode.MARKDOWN
    )

@router.message(lambda msg: msg.location is not None)
async def handle_location(message: Message):
    loc = message.location
    lat = loc.latitude
    lon = loc.longitude
    radius = 1000  # Можно сделать настраиваемым
    user_id = message.from_user.id
    add_user_query(user_id, lat, lon, radius, f"локация: {lat},{lon}, r={radius}")
    await process_house_and_objects(message, lat, lon, radius)


@router.message(lambda msg: msg.text == "О рейтинге")
async def about_rating(message: Message):
    text = (
        "Рейтинг рассчитывается на основе трех критериев:\n"
        "• Социальная инфраструктура – наличие и качество инфраструктуры (школы, детсады, больницы, парки);\n"
        "• Качество недвижимости – возраст, этажность, аварийность и другие характеристики дома;\n"
        "• Транспортная доступность – близость к метро, остановкам, центру города и парковкам.\n"
        "Максимум суммарно – 100 баллов."
    )
    await message.answer(text)

@router.message(lambda msg: msg.text == "О нас")
async def about_us(message: Message):
    text = "Мы проект по помощи в устойчивом развитии г. Москвы"
    await message.answer(text)

@router.message(lambda msg: msg.text == "Мои запросы")
async def my_requests_cmd(message: Message):
    user_id = message.from_user.id
    hist = user_queries.get(user_id, [])
    if not hist:
        await message.answer("История запросов пуста.")
        return
    lines = ["Ваши последние запросы:"]
    for i, (lat, lon, r, addr) in enumerate(hist):
        lines.append(f"{i+1}. {addr}, r={r}")
    await message.answer("\n".join(lines))

@router.message(lambda msg: msg.text == "Топ-10")
async def top10_cmd(message: Message):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT b.building_id,
               b.address,
               ROUND(br.total_score::numeric,2) AS total_score
        FROM building b
        JOIN building_ratings br ON br.building_id = b.building_id
        ORDER BY br.total_score DESC
        LIMIT 10;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        await message.answer("Нет данных.")
        return

    lines = ["Топ-10 домов по рейтингу:"]
    for row in rows:
        lines.append(f"- {row['address']} => {row['total_score']}")
    await message.answer("\n".join(lines))

@router.message(lambda msg: msg.text == "Распределение")
async def distribution_cmd(message: Message):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT total_score FROM building_ratings;")
    data = [float(row[0]) for row in cur.fetchall()]
    cur.close()
    conn.close()

    if not data:
        await message.answer("Нет данных.")
        return

    plt.figure(figsize=(6,4))
    plt.hist(data, bins=10, range=(0,100), color='skyblue', edgecolor='black')
    plt.title("Распределение рейтингов")
    plt.xlabel("Рейтинг")
    plt.ylabel("Количество домов")

    fname = "distribution.png"
    plt.savefig(fname)
    plt.close()

    photo = FSInputFile(fname)
    await message.answer_photo(
        photo=photo,
        caption="Распределение рейтингов"
    )
    os.remove(fname)

@router.message(lambda msg: msg.text == "Сравнить дома")
async def compare_cmd(message: Message):
    user_id = message.from_user.id
    hist = user_queries.get(user_id, [])
    if not hist:
        await message.answer("История запросов пуста.")
        return
    lines = ["Выберите первый дом (введите число):"]
    for i, (lat, lon, r, addr) in enumerate(hist):
        lines.append(f"{i+1}. {addr}")
    lines.append("Например, 1")

    if not hasattr(dp, 'cache_data'):
        dp.cache_data = {}
    dp.cache_data[f"compare_state_{user_id}"] = 'choose_first'
    await message.answer("\n".join(lines))


@router.message()
async def universal_input(message: Message):
    user_id = message.from_user.id
    text = message.text.strip()

    if not hasattr(dp, 'cache_data'):
        dp.cache_data = {}
    state = dp.cache_data.get(f"compare_state_{user_id}")

    if state == 'choose_first':
        try:
            idx = int(text) - 1
        except:
            await message.answer("Неверный формат. Введите число.")
            return
        hist = user_queries.get(user_id, [])
        if idx < 0 or idx >= len(hist):
            await message.answer("Нет такого индекса.")
            return

        dp.cache_data[f"compare_state_{user_id}"] = ("first", idx)
        lines = ["Выберите второй дом:"]
        for i, (la, lo, rr, ad) in enumerate(hist):
            if i != idx:
                lines.append(f"{i+1}. {ad}")
        await message.answer("\n".join(lines))
        return

    elif isinstance(state, tuple) and state[0] == 'first':
        first_idx = state[1]
        try:
            idx2 = int(text) - 1
        except:
            await message.answer("Неверный формат. Введите число.")
            return
        hist = user_queries.get(user_id, [])
        if idx2 < 0 or idx2 >= len(hist) or idx2 == first_idx:
            await message.answer("Нет такого индекса.")
            return

        dp.cache_data[f"compare_state_{user_id}"] = None

        (lat1, lon1, r1, addr1) = hist[first_idx]
        (lat2, lon2, r2, addr2) = hist[idx2]
        res1 = query_building_info(lat1, lon1, r1)
        res2 = query_building_info(lat2, lon2, r2)
        if not res1 or not res2:
            await message.answer("Один из домов не найден.")
            return
        # Список параметров для сравнения (по ключам и подписям)
        fields = [
            ("total_score", "Общий рейтинг"),
            ("social_score", "Соц. оценка"),
            ("quality_score", "Качество"),
            ("transport_score", "Транспорт"),
            ("build_year", "Год постройки"),
            ("floors_number", "Этажей"),
            ("square", "Площадь"),
            ("apartments_number", "Квартир"),
            ("living_area", "Жилая пл."),
            ("not_living_area", "Нежилая пл.")
        ]

        # Отправка текстового сравнения
        lines = ["📊 *Сравнение домов по параметрам:*"]
        for key, label in fields:
            v1 = res1.get(key, 'нет данных')
            v2 = res2.get(key, 'нет данных')
            lines.append(f"- {label}: {v1} vs {v2}")
        await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

        # Генерация и отправка графиков по каждому параметру
        media = []
        temp_files = []

        for key, label in fields:
            v1 = res1.get(key)
            v2 = res2.get(key)
            if v1 is None or v2 is None:
                continue
            try:
                v1 = float(v1)
                v2 = float(v2)
            except:
                continue

            # Генерация графика
            plt.figure(figsize=(4, 3))
            plt.bar(["Дом 1", "Дом 2"], [v1, v2], color=["skyblue", "salmon"])
            plt.title(label)
            plt.tight_layout()
            fname = f"compare_{key}.png"
            plt.savefig(fname)
            plt.close()
            temp_files.append(fname)

            media.append(InputMediaPhoto(media=FSInputFile(fname), caption=f"Сравнение по: {label}"))

        # Отправка альбома
        if media:
            await message.answer_media_group(media)

        # Удаление файлов
        for fname in temp_files:
            os.remove(fname)

        fname = generate_comparison_plot(res1, res2, addr1, addr2)
        photo = FSInputFile(fname)
        await message.answer_photo(photo, caption="Сравнительная аналитика")
        os.remove(fname)

        lines = [
            "Сравнение:",
            f"[1] {addr1}",
            f"  Рейтинг: {res1['total_score']}",
            f"  Соц: {res1['social_score']}, Качество: {res1['quality_score']}, Транспорт: {res1['transport_score']}\n",
            f"[2] {addr2}",
            f"  Рейтинг: {res2['total_score']}",
            f"  Соц: {res2['social_score']}, Качество: {res2['quality_score']}, Транспорт: {res2['transport_score']}"
        ]
        f1 = float(res1['total_score'])
        f2 = float(res2['total_score'])
        if f1 > f2:
            lines.append("➡ Первый дом лучше.")
        elif f1 < f2:
            lines.append("➡ Второй дом лучше.")
        else:
            lines.append("➡ Рейтинги домов равны.")
        await message.answer("\n".join(lines))
        return

    if text.lower().startswith("адрес:"):
        addr = text[6:].strip()
        if not addr:
            await message.answer("Пустой адрес.")
            return
        await message.answer("Геокодирую адрес...")
        geo = await geocode_address(addr)
        if not geo:
            await message.answer("Не удалось определить координаты.")
            return
        lat, lon = geo
        r = 1000
        add_user_query(user_id, lat, lon, r, f"адрес: {addr}")
        await process_house_and_objects(message, lat, lon, r)
        return

    coords = re.split(r'\s*,\s*|\s+', text)
    coords = [c.strip(",") for c in coords if c]
    if 2 <= len(coords) <= 3:
        try:
            lat = float(coords[0])
            lon = float(coords[1])
            radius = 1000
            if len(coords) == 3:
                radius = float(coords[2])
            add_user_query(user_id, lat, lon, radius, f"coords: {lat},{lon}, r={radius}")
            await process_house_and_objects(message, lat, lon, radius)
        except Exception as e:
            await message.answer("Неверный формат координат.")
    else:
        await message.answer(
            "Не понял вас.\n"
            "Чтобы начать заново, введите /start."
        )

async def process_house_and_objects(message: Message, lat: float, lon: float, radius: float):
    await message.answer("Ищу ближайший дом...")
    res = query_building_info(lat, lon, radius)
    if not res:
        await message.answer("Дом не найден.")
        return

    dist = math.dist([lat, lon], [res['geom_lat'], res['geom_lon']]) * 111000
    lines = [
        f"🏠 *Дом:* {res['name']} (ID: {res['building_id']})",
        f"⭐ *Рейтинг:* {res['total_score']} / 100",
        "",
        "🏗 *Характеристики:*",
        f"- Год постройки: {res['build_year']}",
        f"- Этажей: {res['floors_number']}",
        f"- Аварийный: {'Да' if res['is_emergency'] else 'Нет'}",
        f"- Общая площадь: {res.get('square', 'нет данных')}",
        f"- Количество квартир: {res.get('apartments_number', 'нет данных')}",
        f"- Тип здания (ID): {res.get('building_type_id', 'нет данных')}",
        f"- Жилая площадь: {res.get('living_area', 'нет данных')}",
        f"- Нежилая площадь: {res.get('not_living_area', 'нет данных')}",
        f"- Культурное наследие: {'Да' if res.get('is_cultural_heritage') else 'Нет'}",
        f"- Координаты в БД: {res.get('latitude', 'нет данных')}, {res.get('longitude', 'нет данных')}",
        "",
        "📊 *Оценки по категориям:*",
        f"- Соц: {res['social_score']}",
        f"- Качество: {res['quality_score']}",
        f"- Транспорт: {res['transport_score']}",
        "",
        f"📍 Отклонение координат: {dist:.2f} м",
        "",
        f"Объекты в радиусе {int(radius)} м:"
    ]
    objs = res['objects']
    if objs:
        for o in objs:
            lines.append(f"- {o['type']}: {o['name']}")
    else:
        lines.append("Объекты не найдены.")

    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

def save_chart(categories, vals1, vals2, addr1, addr2, title, filename):
    import numpy as np
    x = np.arange(len(categories))
    width = 0.35

    fig, ax = plt.subplots(figsize=(6,4))
    ax.bar(x - width/2, vals1, width, label=addr1[:10], color='skyblue')
    ax.bar(x + width/2, vals2, width, label=addr2[:10], color='salmon')

    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=25)
    ax.legend()
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

async def send_comparison_charts(message: Message, res1, res2, addr1, addr2):
    charts = [
        ("Рейтинг", ["Общий", "Соц", "Качество", "Транспорт"],
         [res1['total_score'], res1['social_score'], res1['quality_score'], res1['transport_score']],
         [res2['total_score'], res2['social_score'], res2['quality_score'], res2['transport_score']],
         "chart_ratings.png"),

        ("Характеристики", ["Год постройки", "Этажей"],
         [res1['build_year'], res1['floors_number']],
         [res2['build_year'], res2['floors_number']],
         "chart_basic.png"),

        ("Площади", ["Общая", "Жилая", "Нежилая"],
         [res1.get('square') or 0, res1.get('living_area') or 0, res1.get('not_living_area') or 0],
         [res2.get('square') or 0, res2.get('living_area') or 0, res2.get('not_living_area') or 0],
         "chart_areas.png"),

        ("Квартиры", ["Квартир"],
         [res1.get('apartments_number') or 0],
         [res2.get('apartments_number') or 0],
         "chart_apts.png")
    ]

    for title, labels, vals1, vals2, fname in charts:
        save_chart(labels, vals1, vals2, addr1, addr2, f"Сравнение: {title}", fname)
        photo = FSInputFile(fname)
        await message.answer_photo(photo=photo, caption=f"{title}")
        os.remove(fname)

async def send_comparison_text(message: Message, res1, res2):
    def format_pair(label, key):
        v1 = res1.get(key, "нет данных")
        v2 = res2.get(key, "нет данных")
        return f"{label}: {v1} vs {v2}"

    lines = [
        "📊 *Сравнительная сводка:*",
        format_pair("Общий рейтинг", "total_score"),
        format_pair("Социальная оценка", "social_score"),
        format_pair("Качество", "quality_score"),
        format_pair("Транспорт", "transport_score"),
        "",
        format_pair("Год постройки", "build_year"),
        format_pair("Этажей", "floors_number"),
        format_pair("Площадь общая", "square"),
        format_pair("Квартир", "apartments_number"),
        format_pair("Жилая площадь", "living_area"),
        format_pair("Нежилая площадь", "not_living_area")
    ]
    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

# Запуск

async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
