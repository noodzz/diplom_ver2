"""
Функции для работы с доступностью сотрудников с учетом выходных дней.
"""
import datetime


def is_available_on_date(employee_id, date_str, employee_manager):
    """
    Проверяет, доступен ли сотрудник в указанную дату (не выходной)

    Args:
        employee_id (int): ID сотрудника
        date_str (str): Дата в формате 'YYYY-MM-DD'
        employee_manager: Менеджер сотрудников

    Returns:
        bool: True если сотрудник доступен, False если это выходной
    """
    try:
        is_available = employee_manager.is_available(employee_id, date_str)
        return is_available
    except Exception as e:
        print(f"Ошибка при проверке доступности сотрудника {employee_id} на дату {date_str}: {str(e)}")
        return False


def get_available_dates_for_task(employee_id, start_date_str, duration, employee_manager):
    """
    Находит подходящие даты для задачи с учетом выходных дней сотрудника

    Args:
        employee_id (int): ID сотрудника
        start_date_str (str): Предполагаемая дата начала в формате 'YYYY-MM-DD'
        duration (int): Длительность задачи в рабочих днях
        employee_manager: Менеджер сотрудников

    Returns:
        tuple: (start_date, end_date, calendar_duration) в формате YYYY-MM-DD или (None, None, None) в случае ошибки
    """
    try:
        # Преобразуем дату начала в объект datetime
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')

        # Ищем первый доступный день, начиная с даты начала
        current_date = start_date
        max_search_days = 30  # Ограничиваем поиск

        # Ищем первый рабочий день
        for _ in range(max_search_days):
            date_str = current_date.strftime('%Y-%m-%d')
            if is_available_on_date(employee_id, date_str, employee_manager):
                # Нашли первый рабочий день
                first_working_day = current_date
                break
            current_date += datetime.timedelta(days=1)
        else:
            # Не нашли рабочий день в течение max_search_days
            print(f"Не найден рабочий день для сотрудника {employee_id} в течение {max_search_days} дней")
            return None, None, None

        # Отсчитываем нужное количество рабочих дней для задачи
        current_date = first_working_day
        working_days_found = 0
        calendar_days = 0

        while working_days_found < duration and calendar_days < max_search_days * 2:
            date_str = current_date.strftime('%Y-%m-%d')
            calendar_days += 1

            if is_available_on_date(employee_id, date_str, employee_manager):
                working_days_found += 1
                last_working_day = current_date

            current_date += datetime.timedelta(days=1)

            if calendar_days >= max_search_days * 2:
                print(f"Превышено максимальное количество дней поиска для сотрудника {employee_id}")
                return None, None, None

        return (
            first_working_day.strftime('%Y-%m-%d'),
            last_working_day.strftime('%Y-%m-%d'),
            calendar_days
        )

    except Exception as e:
        print(f"Ошибка при поиске дат для сотрудника {employee_id}: {str(e)}")
        return None, None, None


def find_suitable_employee(position, start_date, duration, employee_manager, employee_workload=None):
    """
    Находит подходящего сотрудника для задачи с учетом выходных дней

    Args:
        position (str): Требуемая должность
        start_date (str): Дата начала задачи
        duration (int): Длительность задачи в рабочих днях
        employee_manager: Менеджер сотрудников
        employee_workload (dict): Словарь текущей загрузки сотрудников (employee_id -> рабочих дней)

    Returns:
        tuple: (employee_id, start_date, end_date, calendar_duration) или (None, None, None, None) в случае ошибки
    """
    try:
        # Получаем список сотрудников с указанной должностью
        suitable_employees = employee_manager.get_employees_by_position(position)

        if not suitable_employees:
            print(f"Не найдены сотрудники с должностью '{position}'")
            return None, None, None, None

        # Если workload не предоставлен, инициализируем пустой словарь
        if employee_workload is None:
            employee_workload = {}

        # Ищем наиболее подходящего сотрудника
        best_employee_id = None
        best_start_date = None
        best_end_date = None
        best_calendar_duration = float('inf')
        best_workload = float('inf')

        for employee in suitable_employees:
            employee_id = employee['id']

            # Получаем возможные даты выполнения задачи для данного сотрудника
            task_start, task_end, calendar_duration = get_available_dates_for_task(
                employee_id, start_date, duration, employee_manager
            )

            if task_start and task_end:
                # Учитываем текущую загрузку сотрудника
                current_workload = employee_workload.get(employee_id, 0)

                # Выбираем сотрудника с минимальной загрузкой
                # или с минимальной календарной длительностью при равной загрузке
                if (best_employee_id is None or
                        current_workload < best_workload or
                        (current_workload == best_workload and calendar_duration < best_calendar_duration)):
                    best_employee_id = employee_id
                    best_start_date = task_start
                    best_end_date = task_end
                    best_calendar_duration = calendar_duration
                    best_workload = current_workload

        if best_employee_id:
            # Обновляем загрузку выбранного сотрудника
            employee_workload[best_employee_id] = employee_workload.get(best_employee_id, 0) + duration

            return best_employee_id, best_start_date, best_end_date, best_calendar_duration
        else:
            print(f"Не удалось найти подходящего сотрудника для должности '{position}' на период с {start_date}")
            return None, None, None, None

    except Exception as e:
        print(f"Ошибка при поиске подходящего сотрудника: {str(e)}")
        return None, None, None, None