import telebot
from telebot import types
import requests
import io
import threading
from collections import defaultdict
import concurrent.futures

bot = None
API_TOKEN = None
TARGET_CHAT_ID = None

# Глобальная таблица для хранения медиа-альбомов
media_groups = defaultdict(list)
media_groups_lock = threading.Lock()
media_groups_timers = {}
media_groups_downloading = defaultdict(int)  # Счетчик загружающихся файлов для каждой группы

# Глобальная таблица для хранения сообщений с порядком по message_id для каждой media_group_id
media_groups_by_msgid = defaultdict(list)

# Пул потоков для загрузки файлов
download_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

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

        input_media_list = []
        for _, (message, file_data) in media_list:
            input_media = create_input_media(message, file_data)
            if input_media:
                input_media_list.append(input_media)

        try:
            if len(input_media_list) == 1:
                # Одно медиа — отправляем отдельно без текста
                message, file_data = media_list[0][1]
                if message.photo:
                    bot.send_photo(TARGET_CHAT_ID, file_data)
                elif message.video:
                    bot.send_video(TARGET_CHAT_ID, file_data)
                elif message.animation:
                    bot.send_animation(TARGET_CHAT_ID, file_data)
                elif message.document:
                    original_filename = message.document.file_name
                    bot.send_document(TARGET_CHAT_ID, file_data,
                                      visible_file_name=original_filename)
                elif message.audio:
                    bot.send_audio(TARGET_CHAT_ID, file_data)
                else:
                    print(f"Неизвестный тип медиа для message_id {message.message_id}")
            else:
                # Несколько файлов — отправляем как альбом
                bot.send_media_group(TARGET_CHAT_ID, input_media_list)

            print(f"Медиа-альбом {media_group_id} успешно отправлен")

            # Очистка после отправки
            del media_groups_by_msgid[media_group_id]
            if media_group_id in media_groups:
                del media_groups[media_group_id]
            if media_group_id in media_groups_timers:
                del media_groups_timers[media_group_id]
            if media_group_id in media_groups_downloading:
                del media_groups_downloading[media_group_id]

        except Exception as e:
            error_msg = f"Ошибка отправки медиа-альбома {media_group_id}:\n{str(e)}"
            print(error_msg)
            # Отправляем уведомление об ошибке для первого сообщения в группе
            if media_list:
                first_message = media_list[0][1][0]
                send_error_notification(first_message.chat.id, error_msg, first_message)
            # Очищаем хранилище
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
    def single_media_callback(data):
        if data:
            media_list = []
            media_list.append(create_input_media(message, data))
            bot.send_media_group(TARGET_CHAT_ID, media=media_list)
            print(f"Медиа из сообщения {message.message_id} отправлено")
        else:
            send_error_notification(message.chat.id, "Не удалось скачать данные", message)

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
            print(f"Файл слишком большой, медиа из сообщения {message.message_id} отправлено по ID")

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

    print(f"Бот запущен, API_TOKEN = {API_TOKEN}, TARGET_CHAT_ID = {TARGET_CHAT_ID}")

    try:
        bot.polling(none_stop=True)
    except KeyboardInterrupt:
        print("Остановка бота...")
        download_executor.shutdown(wait=False)
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        download_executor.shutdown(wait=False)
