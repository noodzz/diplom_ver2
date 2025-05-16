def check_employee_availability(employee_id, start_date, duration, employee_manager):
    """
    Проверяет доступность сотрудника на указанный период с учетом выходных дней

    Args:
        employee_id (int): ID сотрудника
        start_date (str): Дата начала в формате 'YYYY-MM-DD'
        duration (int): Длительность задачи в днях
        employee_manager: Менеджер сотрудников

    Returns:
        bool: True, если сотрудник доступен на все дни периода, False в противном случае
    """
    import datetime

    try:
        # Преобразуем дату начала в объект datetime
        current_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        working_days = 0
        # Проверяем каждый день
        for i in range(duration * 2):  # Проверяем с запасом
            date_str = current_date.strftime('%Y-%m-%d')
            if employee_manager.is_available(employee_id, date_str):
                working_days += 1
                if working_days >= duration:
                    return True
            current_date += datetime.timedelta(days=1)

    except Exception as e:
        print(f"Ошибка при проверке доступности сотрудника: {str(e)}")
        return False


def find_suitable_employee_with_days_off(position, start_date, duration, employee_manager, employee_workload):
    """
    Находит подходящего сотрудника с учетом его выходных дней

    Args:
        position (str): Требуемая должность
        start_date (str): Дата начала задачи
        duration (int): Длительность задачи в днях
        employee_manager: Менеджер сотрудников
        employee_workload (dict): Словарь текущей загрузки сотрудников

    Returns:
        int: ID подходящего сотрудника или None, если не найден
    """
    # Получаем список сотрудников требуемой должности
    suitable_employees = employee_manager.get_employees_by_position(position)

    if not suitable_employees:
        print(f"Не найдены сотрудники с должностью '{position}'")
        return None

    # Отфильтровываем сотрудников, у которых в этот период нет выходных
    available_employees = []

    for employee in suitable_employees:
        if check_employee_availability(employee['id'], start_date, duration, employee_manager):
            available_employees.append(employee)

    if not available_employees:
        print(f"Не найдены доступные сотрудники с должностью '{position}' на период с {start_date} на {duration} дней")
        return None

    # Выбираем наименее загруженного сотрудника из доступных
    best_employee = min(available_employees, key=lambda e: employee_workload.get(e['id'], 0))
    employee_workload[best_employee['id']] = employee_workload.get(best_employee['id'], 0) + duration

    print(
        f"Выбран сотрудник {best_employee['name']} (ID: {best_employee['id']}) с загрузкой {employee_workload[best_employee['id']]} дней")
    return best_employee['id']


def find_available_date(employee_id, start_date, duration, employee_manager):
    """
    Находит ближайшую доступную дату для сотрудника с учетом выходных

    Args:
        employee_id (int): ID сотрудника
        start_date (str): Предполагаемая дата начала
        duration (int): Длительность задачи в днях
        employee_manager: Менеджер сотрудников

    Returns:
        tuple: (start_date, end_date) - новая дата начала и окончания или (None, None), если не найдена
    """
    import datetime

    # Максимальное количество дней для поиска
    max_days = 30

    try:
        current_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date = current_date

        # Ищем период без выходных дней достаточной длительности
        for _ in range(max_days):
            # Проверяем текущую дату как потенциальное начало
            found_period = True
            test_date = current_date
            working_days = 0

            while working_days < duration:
                date_str = test_date.strftime('%Y-%m-%d')

                if not employee_manager.is_available(employee_id, date_str):
                    # Если встретили выходной, сдвигаем текущую дату и пробуем снова
                    found_period = False
                    break

                # Увеличиваем счетчик рабочих дней
                working_days += 1
                end_date = test_date
                test_date += datetime.timedelta(days=1)

            if found_period:
                return current_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

            # Сдвигаем текущую дату на один день вперед
            current_date += datetime.timedelta(days=1)

        print(f"Не удалось найти доступный период для сотрудника {employee_id} в течение {max_days} дней")
        return None, None

    except Exception as e:
        print(f"Ошибка при поиске доступной даты: {str(e)}")
        return None, None