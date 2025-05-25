import csv
import io
import datetime
from data.config import Config


def parse_csv(csv_content):
    """
    Разбирает содержимое CSV-файла с информацией о проекте

    Args:
        csv_content (str): Содержимое CSV-файла

    Returns:
        list: Список словарей с данными о задачах
    """
    tasks = []
    errors = []  # Для сбора ошибок

    # Сначала соберем все имена задач для проверки зависимостей
    all_task_names = set()

    # Первый проход для сбора имен задач
    try:
        csv_file = io.StringIO(csv_content)
        task_reader = csv.DictReader(csv_file)
        for row in task_reader:
            task_name = row.get("Задача", "").strip()
            if task_name:
                all_task_names.add(task_name)
    except Exception as e:
        errors.append(f"Ошибка при чтении CSV: {str(e)}")
        return [], errors
    try:
        csv_file = io.StringIO(csv_content)
        reader = csv.DictReader(csv_file)

        # Словарь для отслеживания групповых задач
        group_tasks = {}
        row_number = 1  # Для отслеживания номеров строк

        for row in reader:
            row_number += 1
            try:
                # Проверка наличия обязательных полей
                if "Задача" not in row or not row.get("Задача", "").strip():
                    errors.append(f"Строка {row_number}: отсутствует название задачи")
                    continue
                task_name = row.get("Задача", "").strip()
                # Обработка длительности с проверкой на пустое значение
                duration_str = row.get("Длительность", "").strip()
                if not duration_str:
                    errors.append(f"Строка {row_number}: отсутствует длительность для задачи '{row.get('Задача', '')}'")
                    continue

                try:
                    duration = int(duration_str)
                    if duration <= 0:
                        errors.append(
                            f"Строка {row_number}: некорректная длительность ({duration_str}) для задачи '{row.get('Задача', '')}'")
                        continue
                except ValueError:
                    errors.append(
                        f"Строка {row_number}: длительность '{duration_str}' для задачи '{row.get('Задача', '')}' должна быть целым числом")
                    continue

                task = {
                    "name": task_name,
                    "duration": int(row.get("Длительность", 0)),
                    "is_group": row.get("Тип", "").lower().strip() == "групповая",
                    "position": row.get("Должность", "").strip(),
                }

                # Обрабатываем предшественников
                predecessors_str = row.get("Предшественники", "").strip()
                if predecessors_str:
                    predecessors = [pred.strip() for pred in predecessors_str.split(',')]
                    # Проверка существования предшественников
                    for pred in predecessors:
                        if pred and pred not in all_task_names:
                            errors.append(
                                f"Строка {row_number}: предшественник '{pred}' для задачи '{task_name}' не найден в списке задач")

                    task["predecessors"] = predecessors
                else:
                    task["predecessors"] = []

                # Обрабатываем групповые задачи
                parent_task = row.get("Родительская задача", "").strip()
                if parent_task:
                    # Проверка существования родительской задачи
                    if parent_task not in all_task_names:
                        errors.append(
                            f"Строка {row_number}: родительская задача '{parent_task}' для задачи '{task_name}' не найдена в списке задач")

                    # Это подзадача
                    if parent_task not in group_tasks:
                        # Создаем родительскую задачу, если ее еще нет
                        group_task = {
                            "name": parent_task,
                            "duration": 0,  # Будет рассчитано позже
                            "is_group": True,
                            "predecessors": [],
                            "subtasks": []
                        }
                        group_tasks[parent_task] = group_task
                        tasks.append(group_task)

                    # Добавляем подзадачу
                    subtask = {
                        "name": task["name"],
                        "duration": task["duration"],
                        "position": task["position"],
                        "parallel": row.get("Параллельная", "").lower().strip() in ("да", "yes", "true", "1")
                    }

                    group_tasks[parent_task]["subtasks"].append(subtask)

                    # Обновляем длительность групповой задачи
                    if subtask["parallel"]:
                        # При параллельном выполнении берем максимальную длительность
                        group_tasks[parent_task]["duration"] = max(
                            group_tasks[parent_task]["duration"],
                            subtask["duration"]
                        )
                    else:
                        # При последовательном выполнении суммируем длительности
                        group_tasks[parent_task]["duration"] += subtask["duration"]
                else:
                    # Это обычная задача или новая групповая задача
                    if task["is_group"]:
                        task["subtasks"] = []
                        group_tasks[task["name"]] = task

                    tasks.append(task)
            except Exception as e:
                errors.append(f"Строка {row_number}: ошибка обработки: {str(e)}")

        for task_name, group_task in group_tasks.items():
            if group_task["subtasks"]:
                # Пересчитываем длительность на основе подзадач
                total_duration = 0
                max_duration = 0
                has_parallel = False

                for subtask in group_task["subtasks"]:
                    if subtask.get("parallel", False):
                        has_parallel = True
                        max_duration = max(max_duration, subtask["duration"])
                    else:
                        total_duration += subtask["duration"]

                # Определяем итоговую длительность
                if has_parallel and total_duration > 0:
                    # Смешанный случай: есть и параллельные, и последовательные
                    # Берем сумму последовательных + максимум параллельных
                    group_task["duration"] = total_duration + max_duration
                elif has_parallel:
                    # Только параллельные подзадачи
                    group_task["duration"] = max_duration
                else:
                    # Только последовательные подзадачи
                    group_task["duration"] = total_duration

        # Добавляем новую проверку: групповые задачи должны иметь подзадачи
        for task_name, group_task in group_tasks.items():
            if not group_task["subtasks"]:
                errors.append(f"Групповая задача '{task_name}' не имеет подзадач в CSV файле")

        if errors and not tasks:
            # Если есть ошибки и не удалось создать ни одной задачи
            raise ValueError("\n".join(errors))
    except Exception as e:
        errors.append(f"Ошибка при обработке CSV: {str(e)}")
    return tasks, errors


def format_date(date_str):
    """
    Форматирует дату для отображения

    Args:
        date_str (str): Дата в формате YYYY-MM-DD

    Returns:
        str: Отформатированная дата (DD.MM.YYYY)
    """
    if not date_str:
        return "Не указана"

    try:
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        return date.strftime('%d.%m.%Y')
    except ValueError:
        return date_str


def is_authorized(user_id, db_manager=None):
    """
    Проверяет, есть ли у пользователя доступ к боту
    """
    if db_manager:
        # Проверяем пользователя в базе данных
        user = db_manager.get_user(user_id)
        # Администраторы всегда авторизованы, независимо от статуса активности
        if user and user['is_admin'] == 1:
            return True
        # Обычные пользователи должны быть активными
        return user is not None and user['is_active'] == 1
    else:
        # Резервный вариант
        return user_id in Config.ALLOWED_USER_IDS


def is_admin(user_id, db_manager=None):
    """
    Проверяет, является ли пользователь администратором

    Args:
        user_id (int): Идентификатор пользователя Telegram
        db_manager: Опциональный менеджер базы данных

    Returns:
        bool: True, если пользователь имеет права администратора, иначе False
    """
    if db_manager:
        # Проверяем права администратора в базе данных
        user = db_manager.get_user(user_id)
        return user is not None and user['is_admin'] == 1
    else:
        # Резервный вариант - проверка по первому ID в списке
        return Config.ALLOWED_USER_IDS and user_id == Config.ALLOWED_USER_IDS[0]


def add_days_to_date(date_str, days):
    """
    Добавляет указанное количество дней к дате

    Args:
        date_str (str): Дата в формате YYYY-MM-DD
        days (int): Количество дней для добавления

    Returns:
        str: Новая дата в формате YYYY-MM-DD
    """
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    new_date = date + datetime.timedelta(days=days)
    return new_date.strftime('%Y-%m-%d')


def calculate_end_date(start_date, duration):
    """
    Вычисляет дату окончания задачи

    Args:
        start_date (str): Дата начала в формате YYYY-MM-DD
        duration (int): Длительность в днях

    Returns:
        str: Дата окончания в формате YYYY-MM-DD
    """
    return add_days_to_date(start_date, duration)


def get_working_days(start_date, end_date, days_off):
    """
    Вычисляет количество рабочих дней в указанном интервале, исключая выходные дни

    Args:
        start_date (str): Дата начала в формате YYYY-MM-DD
        end_date (str): Дата окончания в формате YYYY-MM-DD
        days_off (list): Список дней недели, которые являются выходными (1 - понедельник, 7 - воскресенье)

    Returns:
        int: Количество рабочих дней
    """
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    # Преобразуем дни недели из 1-7 в 0-6 (формат Python)
    python_days_off = [(day - 1) % 7 for day in days_off]

    working_days = 0
    current = start

    while current <= end:
        if current.weekday() not in python_days_off:
            working_days += 1
        current += datetime.timedelta(days=1)

    return working_days


def adjust_date_for_days_off(date_str, duration, days_off):
    """
    Корректирует дату окончания задачи с учетом выходных дней

    Args:
        date_str (str): Дата начала в формате YYYY-MM-DD
        duration (int): Длительность в рабочих днях
        days_off (list): Список дней недели, которые являются выходными (1 - понедельник, 7 - воскресенье)

    Returns:
        str: Скорректированная дата окончания в формате YYYY-MM-DD
    """
    start = datetime.datetime.strptime(date_str, '%Y-%m-%d')

    # Преобразуем дни недели из 1-7 в 0-6 (формат Python)
    python_days_off = [(day - 1) % 7 for day in days_off]

    working_days = 0
    current = start

    while working_days < duration:
        current += datetime.timedelta(days=1)
        if current.weekday() not in python_days_off:
            working_days += 1

    return current.strftime('%Y-%m-%d')