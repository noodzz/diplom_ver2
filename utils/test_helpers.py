def disable_days_off_for_testing():
    """
    Временно отключает учет выходных дней у всех сотрудников для тестирования.

    Эта функция модифицирует Config.EMPLOYEES, устанавливая пустой список выходных
    для всех сотрудников, чтобы проверить расчет календарного плана без учета выходных.

    Returns:
        list: Оригинальные данные о сотрудниках для последующего восстановления
    """
    from data.config import Config

    # Сохраняем оригинальные данные
    original_employees = [employee.copy() for employee in Config.EMPLOYEES]

    # Модифицируем данные - устанавливаем пустые списки выходных дней
    for employee in Config.EMPLOYEES:
        employee['days_off'] = []

    print("ТЕСТОВЫЙ РЕЖИМ: Выходные дни сотрудников временно отключены для тестирования")
    return original_employees


def restore_days_off(original_employees):
    """
    Восстанавливает оригинальные выходные дни сотрудников после тестирования.

    Args:
        original_employees (list): Оригинальные данные о сотрудниках
    """
    from data.config import Config

    # Восстанавливаем оригинальные данные
    Config.EMPLOYEES = original_employees

    print("Восстановлены оригинальные данные о выходных днях сотрудников")


def update_employees_in_db(db_manager):
    """
    Обновляет данные о сотрудниках в базе данных на основе текущих данных в Config.

    Args:
        db_manager: Менеджер базы данных
    """
    from data.config import Config
    import json

    db_manager.connect()

    try:
        # Обновляем каждого сотрудника
        for employee in Config.EMPLOYEES:
            db_manager.cursor.execute(
                "UPDATE employees SET days_off = ? WHERE id = ?",
                (json.dumps(employee['days_off']), employee['id'])
            )

        db_manager.connection.commit()
        print(f"Обновлены данные о выходных днях для {len(Config.EMPLOYEES)} сотрудников в базе данных")
    finally:
        db_manager.close()