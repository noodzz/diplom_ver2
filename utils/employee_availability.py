"""
Улучшенная версия модуля для проверки доступности сотрудников, учитывающая выходные дни
и приоритизирующая сохранение исходных дат задач
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
    Находит подходящие даты для задачи с учетом выходных дней сотрудника.
    Дата окончания - день ПОСЛЕ завершения задачи (дедлайн в 00:00).

    Args:
        employee_id (int): ID сотрудника
        start_date_str (str): Предполагаемая дата начала в формате 'YYYY-MM-DD'
        duration (int): Длительность задачи в РАБОЧИХ днях
        employee_manager: Менеджер сотрудников

    Returns:
        tuple: (start_date, end_date, calendar_duration) в формате YYYY-MM-DD или (None, None, None) в случае ошибки
    """
    try:
        # Преобразуем дату начала в объект datetime
        current_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')

        # Проверка на очень длинные задачи
        if duration > 100:  # Если задача длится более 100 дней
            print(f"ВНИМАНИЕ: Задача очень длинная ({duration} дней). Игнорируем выходные дни.")
            end_date = current_date + datetime.timedelta(days=duration - 1)
            return (
                start_date_str,
                end_date.strftime('%Y-%m-%d'),
                duration
            )

        # Ищем первый доступный (рабочий) день, начиная с даты начала
        first_working_day = None
        max_search_days = 30  # Ограничиваем поиск 30 днями

        for _ in range(max_search_days):
            date_str = current_date.strftime('%Y-%m-%d')
            if is_available_on_date(employee_id, date_str, employee_manager):
                # Нашли первый рабочий день
                first_working_day = current_date
                break

            print(f"Дата {date_str} - выходной для сотрудника {employee_id}, пропускаем")
            current_date += datetime.timedelta(days=1)

        if first_working_day is None:
            # Не нашли рабочий день в течение max_search_days
            print(f"Не найден рабочий день для сотрудника {employee_id} в течение {max_search_days} дней")
            return None, None, None

        # Теперь отсчитываем необходимое количество РАБОЧИХ дней
        # и определяем дату окончания задачи
        current_date = first_working_day
        working_days_found = 0
        calendar_days = 0
        last_working_day = None

        while working_days_found < duration and calendar_days < max_search_days * 2:
            date_str = current_date.strftime('%Y-%m-%d')

            # Проверяем, является ли текущий день рабочим для сотрудника
            if is_available_on_date(employee_id, date_str, employee_manager):
                working_days_found += 1
                last_working_day = current_date
                print(f"Дата {date_str} - рабочий день для сотрудника {employee_id} ({working_days_found}/{duration})")
            else:
                print(f"Дата {date_str} - выходной для сотрудника {employee_id}, пропускаем, но включаем в календарную длительность")

            calendar_days += 1
            current_date += datetime.timedelta(days=1)

            if calendar_days >= max_search_days * 2:
                print(f"Превышено максимальное количество дней поиска для сотрудника {employee_id}")
                return None, None, None

        if last_working_day is None:
            print(f"Не удалось найти достаточное количество рабочих дней для сотрудника {employee_id}")
            return None, None, None

        # Эксклюзивная модель дат: дата окончания - день ПОСЛЕ завершения (дедлайн в 00:00)
        end_date = last_working_day

        # Календарная длительность = количество дней от начала до окончания в эксклюзивной модели
        calendar_duration = (end_date - first_working_day).days + 1

        print(f"Для сотрудника {employee_id} задача длительностью {duration} рабочих дней")
        print(f"  будет выполняться с {first_working_day.strftime('%Y-%m-%d')} по {last_working_day.strftime('%Y-%m-%d')}")
        print(f"  последний рабочий день: {last_working_day.strftime('%Y-%m-%d')}")
        print(f"  дата окончания (дедлайн): {end_date.strftime('%Y-%m-%d')}")
        print(f"  общая календарная длительность: {calendar_duration} дней")

        return (
            first_working_day.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d'),
            calendar_duration
        )

    except Exception as e:
        print(f"Ошибка при расчете дат задачи для сотрудника {employee_id}: {str(e)}")
        return None, None, None


def find_suitable_employee(position, start_date_str, duration, employee_manager, employee_workload=None):
    """
    Находит подходящего сотрудника для задачи, ПРИОРИТИЗИРУЯ СОХРАНЕНИЕ ИСХОДНОЙ ДАТЫ.
    Сначала ищет сотрудников, доступных в точную указанную дату, и только если таких нет,
    ищет ближайшую доступную дату со сдвигом.

    Args:
        position (str): Требуемая должность
        start_date_str (str): Исходная дата начала задачи
        duration (int): Длительность задачи в рабочих днях
        employee_manager: Менеджер сотрудников
        employee_workload (dict): Словарь текущей загрузки сотрудников (employee_id -> рабочих дней)

    Returns:
        tuple: (employee_id, start_date, end_date, calendar_duration) или (None, None, None, None) в случае ошибки
    """
    try:
        # Инициализируем словарь загрузки, если не предоставлен
        if employee_workload is None:
            employee_workload = {}

        print(f"Поиск сотрудника для должности '{position}' на дату {start_date_str}, длительность: {duration} дн.")

        # Получаем всех сотрудников с указанной должностью
        suitable_employees = employee_manager.get_employees_by_position(position)

        if not suitable_employees:
            print(f"Не найдены сотрудники с должностью '{position}'")
            return None, None, None, None

        print(f"Найдено {len(suitable_employees)} сотрудников с должностью '{position}'")

        # Выводим текущую загрузку всех сотрудников
        print("Текущая загрузка сотрудников:")
        for emp in suitable_employees:
            print(f"  {emp['name']} (ID:{emp['id']}): {employee_workload.get(emp['id'], 0)} дней")

        # НОВОЕ: Сначала ищем сотрудников, ДОСТУПНЫХ НА ИСХОДНУЮ ДАТУ
        available_on_original_date = []

        for employee in suitable_employees:
            employee_id = employee['id']
            # Проверяем, доступен ли сотрудник на исходную дату
            if is_available_on_date(employee_id, start_date_str, employee_manager):
                # Этот сотрудник доступен на исходную дату!
                available_on_original_date.append(employee)
                print(f"Сотрудник {employee['name']} (ID:{employee_id}) доступен на исходную дату {start_date_str}")

        # Если есть сотрудники, доступные на исходную дату, выбираем из них
        if available_on_original_date:
            # Сортируем по загрузке - наименее загруженные в начале
            sorted_by_workload = sorted(
                available_on_original_date,
                key=lambda e: employee_workload.get(e['id'], 0)
            )

            # Выбираем наименее загруженного
            best_employee = sorted_by_workload[0]
            best_employee_id = best_employee['id']

            # Получаем точные даты с учетом всех выходных
            employee_start, employee_end, calendar_duration = get_available_dates_for_task(
                best_employee_id, start_date_str, duration, employee_manager
            )

            if employee_start:
                print(f"Выбран сотрудник {best_employee['name']} (ID:{best_employee_id}) с загрузкой {employee_workload.get(best_employee_id, 0)} дней")
                return best_employee_id, employee_start, employee_end, calendar_duration

        # Если никто не доступен на исходную дату, ищем ближайшую доступную дату
        print(f"Нет сотрудников, доступных на исходную дату {start_date_str}, ищем ближайшие доступные даты")

        # Создаем список кандидатов с их ближайшими доступными датами
        candidates = []

        for employee in suitable_employees:
            employee_id = employee['id']
            current_workload = employee_workload.get(employee_id, 0)

            # Находим ближайшую доступную дату для этого сотрудника
            employee_start, employee_end, calendar_duration = get_available_dates_for_task(
                employee_id, start_date_str, duration, employee_manager
            )

            if employee_start:
                # Рассчитываем смещение от исходной даты
                start_date_obj = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
                employee_start_obj = datetime.datetime.strptime(employee_start, '%Y-%m-%d')
                date_shift = (employee_start_obj - start_date_obj).days

                candidates.append({
                    'employee': employee,
                    'employee_id': employee_id,
                    'employee_start': employee_start,
                    'employee_end': employee_end,
                    'calendar_duration': calendar_duration,
                    'workload': current_workload,
                    'date_shift': date_shift
                })

        if not candidates:
            print(f"Не найдено подходящих сотрудников для должности '{position}' на ближайшие даты")
            return None, None, None, None

        # Сортируем кандидатов: сначала по минимальному смещению даты, затем по загрузке
        sorted_candidates = sorted(
            candidates,
            key=lambda c: (c['date_shift'], c['workload'])
        )

        # Выбираем лучшего кандидата
        best_candidate = sorted_candidates[0]

        # Обновляем загрузку сотрудника
        employee_workload[best_candidate['employee_id']] = employee_workload.get(best_candidate['employee_id'], 0) + duration

        print(f"Выбран сотрудник {best_candidate['employee']['name']} (ID:{best_candidate['employee_id']}) "
              f"со смещением на {best_candidate['date_shift']} дней и загрузкой {best_candidate['workload']} дней")

        return (
            best_candidate['employee_id'],
            best_candidate['employee_start'],
            best_candidate['employee_end'],
            best_candidate['calendar_duration']
        )
    except Exception as e:
        print(f"Ошибка при поиске подходящего сотрудника: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return None, None, None, None