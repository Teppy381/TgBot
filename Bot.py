import telebot
from telebot import types
import requests
import io
import threading
from collections import defaultdict
import concurrent.futures
import sqlite3
import hashlib
from datetime import datetime

bot = None
API_TOKEN = None
TARGET_CHAT_ID = None
db_connection = None

# Глобальная таблица для хранения медиа-альбомов
media_groups = defaultdict(list)
media_groups_lock = threading.Lock()
media_groups_timers = {}
media_groups_downloading = defaultdict(int)  # Счетчик загружающихся файлов для каждой группы

# Глобальная таблица для хранения сообщений с порядком по message_id для каждой media_group_id
media_groups_by_msgid = defaultdict(list)

# Пул потоков для загрузки файлов
download_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

# ==================== Функции для работы с базой данных ====================

def init_database():
    """Инициализирует базу данных для хранения информации о медиа-файлах"""
    global db_connection
    db_connection = sqlite3.connect('media_deduplication.db', check_same_thread=False)
    cursor = db_connection.cursor()

    # Создаем таблицу для хранения информации о медиа
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS media_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT NOT NULL UNIQUE,
            message_id INTEGER NOT NULL,
            data_hash TEXT,
            file_size INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Создаем индексы для быстрого поиска
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_id ON media_files(file_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash_size ON media_files(data_hash, file_size)')

    db_connection.commit()
    print("База данных инициализирована")

def check_media_by_file_id(file_id):
    """
    Проверяет, существует ли медиа с таким file_id в базе данных
    Возвращает message_id если найдено, иначе None
    """
    cursor = db_connection.cursor()
    cursor.execute('SELECT message_id FROM media_files WHERE file_id = ?', (file_id,))
    result = cursor.fetchone()
    return result[0] if result else None

def check_media_by_hash(data_hash, file_size):
    """
    Проверяет, существует ли медиа с таким хешем и размером в базе данных
    Возвращает message_id если найдено, иначе None
    """
    if not data_hash:
        return None

    cursor = db_connection.cursor()
    if file_size is not None:
        cursor.execute('SELECT message_id FROM media_files WHERE data_hash = ? AND file_size = ?',
                      (data_hash, file_size))
    else:
        cursor.execute('SELECT message_id FROM media_files WHERE data_hash = ?', (data_hash,))

    result = cursor.fetchone()
    return result[0] if result else None

def add_media_to_database(file_id, message_id, data_hash=None, file_size=None):
    """
    Добавляет информацию о медиа-файле в базу данных
    """
    try:
        cursor = db_connection.cursor()
        cursor.execute('''
            INSERT INTO media_files (file_id, message_id, data_hash, file_size)
            VALUES (?, ?, ?, ?)
        ''', (file_id, message_id, data_hash, file_size))
        db_connection.commit()
        print(f"Медиа добавлено в БД: file_id={file_id}, message_id={message_id}, hash={data_hash}, size={file_size}")
    except sqlite3.IntegrityError:
        print(f"Медиа с file_id={file_id} уже существует в БД")
    except Exception as e:
        print(f"Ошибка добавления медиа в БД: {e}")

def calculate_file_hash(file_data):
    """
    Вычисляет SHA256 хеш файла из BytesIO объекта
    """
    if not isinstance(file_data, io.BytesIO):
        return None

    try:
        file_data.seek(0)  # Возвращаемся в начало файла
        file_hash = hashlib.sha256(file_data.read()).hexdigest()
        file_data.seek(0)  # Возвращаемся в начало для последующего использования
        return file_hash
    except Exception as e:
        print(f"Ошибка вычисления хеша: {e}")
        return None

# ==================== Конец функций для работы с БД ====================

def get_file_size(message):
    """Извлекает размер файла из сообщения"""
    if message.photo:
        return message.photo[-1].file_size if hasattr(message.photo[-1], 'file_size') else None
    elif message.video:
        return message.video.file_size if hasattr(message.video, 'file_size') else None
    elif message.animation:
        return message.animation.file_size if hasattr(message.animation, 'file_size') else None
    elif message.document:
        return message.document.file_size if hasattr(message.document, 'file_size') else None
    elif message.audio:
        return message.audio.file_size if hasattr(message.audio, 'file_size') else None
    return None

def get_file_id(message):
    """Извлекает file_id из сообщения"""
    if message.photo:
        return message.photo[-1].file_id
    elif message.video:
        return message.video.file_id
    elif message.animation:
        return message.animation.file_id
    elif message.document:
        return message.document.file_id
    elif message.audio:
        return message.audio.file_id
    return None

def download_media_file(file_info):
    """
    Скачивает медиафайл и возвращает его как BytesIO объект
    Использует многопоточность для избежания блокировки
    """
    try:
        file_url = f"https://api.telegram.org/file/bot{API_TOKEN}/{file_info.file_path}"
        response = requests.get(file_url, timeout=30)
        if response.status_code == 200:
            return io.BytesIO(response.content)
        else:
            print(f"Ошибка скачивания {file_info.file_id}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Ошибка загрузки файла {file_info.file_id}: {e}")
        return None

def download_media_file_async(file_info, callback, *callback_args):
    """
    Запускает асинхронную загрузку файла и вызывает callback по завершению
    """
    def download_wrapper():
        file_data = download_media_file(file_info)
        callback(file_data, *callback_args)
    download_executor.submit(download_wrapper)

def create_input_media(message, file_data):
    """Создает объект InputMedia для отправки"""
    if message.photo:
        return types.InputMediaPhoto(media=file_data)
    elif message.video:
        if (message.video.cover):
            return types.InputMediaVideo(media=file_data, cover=message.video.cover[-1].file_id)
        return types.InputMediaVideo(media=file_data)
    elif message.animation:
        return types.InputMediaAnimation(media=file_data)
    elif message.document:
        return types.InputMediaDocument(media=file_data)
    elif message.audio:
        return types.InputMediaAudio(media=file_data)
    return None

def send_error_notification(chat_id, error_message, original_message=None):
    """Отправляет уведомление об ошибке пользователю"""
    try:
        print(error_message)
        if original_message:
            bot.send_message(chat_id, error_message, reply_to_message_id=original_message.message_id)
        else:
            bot.send_message(chat_id, error_message)
        print(f"(Уведомление об ошибке отправлено в чат {chat_id})")
    except Exception as e:
        print(f"Не удалось отправить уведомление об ошибке: {e}")

def send_media_group(media_group_id):
    """Отправляет собранный медиа-альбом в порядке message_id, без текста"""
    with media_groups_lock:
        # Проверяем, есть ли еще загружающиеся файлы
        if media_groups_downloading.get(media_group_id, 0) > 0:
            print(f"В группе {media_group_id} еще есть загружающиеся файлы, откладываем отправку")
            # Перезапускаем таймер еще на 1 секунду
            schedule_media_group_send(media_group_id, delay=1.0)
            return

        media_list = media_groups_by_msgid[media_group_id]
        if len(media_list) < 1:
            return

        # Сортируем по message_id
        media_list.sort(key=lambda x: x[0])

        # Проверяем каждое медиа на дедупликацию
        media_to_send = []  # [(message, file_data, file_id, file_hash, file_size)]
        duplicates = []  # [(message, existing_message_id, reason)]

        for _, (message, file_data) in media_list:
            file_id = get_file_id(message)
            if not file_id:
                continue

            # Проверяем по file_id
            existing_message_id = check_media_by_file_id(file_id)
            if existing_message_id:
                duplicates.append((message, existing_message_id, "file_id"))
                print(f"Дубликат в альбоме найден по file_id: {file_id}, message_id: {existing_message_id}")
                continue

            # Если файл скачан (BytesIO), проверяем по хешу
            file_hash = None
            file_size = None
            if isinstance(file_data, io.BytesIO):
                file_hash = calculate_file_hash(file_data)
                file_size = get_file_size(message)

                if file_hash:
                    existing_message_id = check_media_by_hash(file_hash, file_size)
                    if existing_message_id:
                        duplicates.append((message, existing_message_id, "hash"))
                        print(f"Дубликат в альбоме найден по хешу: {file_hash}, message_id: {existing_message_id}")
                        continue

            # Медиа не дубликат, добавляем в список для отправки
            media_to_send.append((message, file_data, file_id, file_hash, file_size))

        # Отправляем уведомления о дубликатах
        if duplicates:
            first_message = duplicates[0][0]
            dup_count = len(duplicates)
            if dup_count == 1:
                msg, existing_id, reason = duplicates[0]
                bot.reply_to(msg, f"⚠️ Это медиа уже было отправлено ранее (message_id: {existing_id})")
            else:
                dup_msg = f"⚠️ {dup_count} медиа из этого альбома уже были отправлены ранее"
                bot.reply_to(first_message, dup_msg)

        # Отправляем новые медиа
        if len(media_to_send) > 0:
            try:
                # Создаем список InputMedia для всех файлов (и для одиночных, и для альбомов)
                input_media_list = []
                for message, file_data, _, _, _ in media_to_send:
                    input_media = create_input_media(message, file_data)
                    if input_media:
                        input_media_list.append(input_media)

                # Отправляем через send_media_group независимо от количества файлов
                sent_messages = bot.send_media_group(TARGET_CHAT_ID, input_media_list)

                # Добавляем отправленные медиа в базу данных
                for i, (message, file_data, file_id, file_hash, file_size) in enumerate(media_to_send):
                    if i < len(sent_messages):
                        add_media_to_database(file_id, sent_messages[i].message_id, file_hash, file_size)

                print(f"Медиа-альбом {media_group_id} успешно отправлен ({len(media_to_send)} новых, {len(duplicates)} дубликатов)")

            except Exception as e:
                error_msg = f"Ошибка отправки медиа-альбома {media_group_id}:\n{str(e)}"
                print(error_msg)
                # Отправляем уведомление об ошибке для первого сообщения в группе
                if media_list:
                    first_message = media_list[0][1][0]
                    send_error_notification(first_message.chat.id, error_msg, first_message)
        elif len(duplicates) == 0:
            print(f"Медиа-альбом {media_group_id} пуст после обработки")

        # Очистка после обработки
        if media_group_id in media_groups_by_msgid:
            del media_groups_by_msgid[media_group_id]
        if media_group_id in media_groups:
            del media_groups[media_group_id]
        if media_group_id in media_groups_timers:
            del media_groups_timers[media_group_id]
        if media_group_id in media_groups_downloading:
            del media_groups_downloading[media_group_id]

def schedule_media_group_send(media_group_id, delay=3.0):
    """Планирует отправку медиа-альбома через указанное количество секунд"""
    if media_group_id in media_groups_timers:
        # Отменяем предыдущий таймер
        media_groups_timers[media_group_id].cancel()
    timer = threading.Timer(delay, send_media_group, [media_group_id])
    timer.daemon = True
    timer.start()
    media_groups_timers[media_group_id] = timer

def media_group_download_callback(file_data, media_group_id, message):
    """Callback для завершения загрузки файла в медиа-группе"""
    with media_groups_lock:
        if media_group_id in media_groups_downloading:
            media_groups_downloading[media_group_id] -= 1
            if file_data:
                # Добавляем загруженный файл в структуру с сортировкой по message_id
                media_groups_by_msgid[media_group_id].append((message.message_id, (message, file_data)))
                print(f"Загружено медиа для группы {media_group_id}, всего: {len(media_groups_by_msgid[media_group_id])}, "
                      f"еще загружается: {media_groups_downloading.get(media_group_id, 0)}")
            else:
                print(f"Ошибка загрузки медиа для группы {media_group_id}")
        else:
            print(f"Группа {media_group_id} не указана как загружающая")

def handle_message_from_media_group(message, media_group_id, file_id):
    # print("Обработка одного медиа из альбома...")
    try:
        # print("Попытка загрузки...")
        file_info = bot.get_file(file_id)
        download_media_file_async(file_info, media_group_download_callback, media_group_id, message)
        print(f"Начата загрузка медиа для группы {media_group_id}, всего загружается: {media_groups_downloading[media_group_id]}")
    except Exception as e:
        # print("Попытка загрузки не удалась: " + str(e))
        if "file is too big" in str(e).lower():
            with media_groups_lock:
                if media_group_id in media_groups_downloading:
                    media_groups_downloading[media_group_id] -= 1
                media_groups_by_msgid[media_group_id].append((message.message_id, (message, file_id)))
            print(f"Добавлено по ID медиа для группы {media_group_id}, всего: {len(media_groups_by_msgid[media_group_id])}, "
                  f"еще загружается: {media_groups_downloading.get(media_group_id, 0)}")


def handle_single_message(message, file_id):
    # Сначала проверяем file_id в базе данных
    existing_message_id = check_media_by_file_id(file_id)
    if existing_message_id:
        bot.reply_to(message, f"⚠️ Это медиа уже было отправлено ранее (message_id: {existing_message_id})")
        print(f"Дубликат медиа найден по file_id: {file_id}, message_id: {existing_message_id}")
        return

    def single_media_callback(data):
        if not data:
            send_error_notification(message.chat.id, "Не удалось скачать данные", message)
            return

        # Если data это BytesIO (файл скачан), проверяем по хешу
        file_hash = None
        file_size = None
        if isinstance(data, io.BytesIO):
            file_hash = calculate_file_hash(data)
            file_size = get_file_size(message)

            if file_hash:
                # Проверяем по хешу
                existing_message_id = check_media_by_hash(file_hash, file_size)
                if existing_message_id:
                    bot.reply_to(message, f"⚠️ Это медиа уже было отправлено ранее (найдено по содержимому, message_id: {existing_message_id})")
                    print(f"Дубликат медиа найден по хешу: {file_hash}, message_id: {existing_message_id}")
                    return

        # Медиа не является дубликатом, отправляем его
        try:
            media_list = []
            media_list.append(create_input_media(message, data))
            sent_messages = bot.send_media_group(TARGET_CHAT_ID, media=media_list)

            if sent_messages and len(sent_messages) > 0:
                # Добавляем в базу данных
                add_media_to_database(file_id, sent_messages[0].message_id, file_hash, file_size)
                print(f"Медиа из сообщения {message.message_id} отправлено, новый message_id: {sent_messages[0].message_id}")
        except Exception as e:
            error_msg = f"Ошибка отправки медиа: {str(e)}"
            send_error_notification(message.chat.id, error_msg, message)

    # print("Обработка одиночного медиа...")
    try:
        # print("Попытка загрузки...")
        file_info = bot.get_file(file_id)
        download_media_file_async(file_info, single_media_callback)
        print(f"Начата загрузка медиа для сообщения {message.message_id}")
    except Exception as e:
        # print("Попытка загрузки не удалась: " + str(e))
        if "file is too big" in str(e).lower():
            single_media_callback(file_id)
            print(f"Файл слишком большой, медиа из сообщения {message.message_id} будет отправлено по ID")

def handle_message(message):
    print("Обработка медиа...")
    if message.media_group_id is None:
        # Одиночное медиа
        try:
            if message.photo:
                file_id = message.photo[-1].file_id
                handle_single_message(message, file_id)
            elif message.video:
                file_id = message.video.file_id
                handle_single_message(message, file_id)
            elif message.animation:
                file_id = message.animation.file_id
                handle_single_message(message, file_id)
            elif message.document:
                file_id = message.document.file_id
                handle_single_message(message, file_id)
            elif message.audio:
                file_id = message.audio.file_id
                handle_single_message(message, file_id)
        except Exception as e:
            error_msg = f"Ошибка обработки одиночного медиа:\n{str(e)}"
            send_error_notification(message.chat.id, error_msg, message)
    else:
        # Медиа в группе
        media_group_id = message.media_group_id
        try:
            with media_groups_lock:
                media_groups_downloading[media_group_id] += 1
            if message.photo:
                file_id = message.photo[-1].file_id
                handle_message_from_media_group(message, media_group_id, file_id)
            elif message.video:
                file_id = message.video.file_id
                handle_message_from_media_group(message, media_group_id, file_id)
            elif message.animation:
                file_id = message.animation.file_id
                handle_message_from_media_group(message, media_group_id, file_id)
            elif message.document:
                file_id = message.document.file_id
                handle_message_from_media_group(message, media_group_id, file_id)
            elif message.audio:
                file_id = message.audio.file_id
                handle_message_from_media_group(message, media_group_id, file_id)
            # Планируем отправку через 3 секунды
            schedule_media_group_send(media_group_id, delay=3.0)
        except Exception as e:
            error_msg = f"Ошибка обработки медиа для группы {media_group_id}:\n{str(e)}"
            send_error_notification(message.chat.id, error_msg, message)
            with media_groups_lock:
                media_groups_downloading[media_group_id] -= 1

def send_chat_id(message):
    chat_id = message.chat.id
    bot.reply_to(message, f"ID этого чата: `{chat_id}`", parse_mode="Markdown")

def create_bot():
    global API_TOKEN
    with open('API_TOKEN.txt', 'r') as f:
        API_TOKEN = f.read().strip()
    bot = telebot.TeleBot(API_TOKEN)

    # Регистрируем все обработчики после создания бота
    bot.message_handler(content_types=['photo', 'video', 'animation', 'document', 'audio'])(handle_message)
    bot.message_handler(commands=['get_chat_id'])(send_chat_id)
    return bot

if __name__ == '__main__':
    bot = create_bot()

    with open('TARGET_CHAT_ID.txt', 'r') as f:
        TARGET_CHAT_ID = f.read().strip()

    # Инициализируем базу данных для дедупликации медиа
    init_database()

    print(f"Бот запущен, API_TOKEN = {API_TOKEN}, TARGET_CHAT_ID = {TARGET_CHAT_ID}")

    try:
        bot.polling(none_stop=True)
    except KeyboardInterrupt:
        print("Остановка бота...")
        download_executor.shutdown(wait=False)
        if db_connection:
            db_connection.close()
            print("Соединение с базой данных закрыто")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        download_executor.shutdown(wait=False)
        if db_connection:
            db_connection.close()
            print("Соединение с базой данных закрыто")
