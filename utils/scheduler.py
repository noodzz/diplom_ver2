import datetime
import json
from collections import defaultdict, deque

from utils.employee_availability import find_suitable_employee_with_days_off, find_available_date


def build_dependency_graph(tasks, task_manager):
    """
    Строит граф зависимостей между задачами

    Args:
        tasks (list): Список задач
        task_manager: Менеджер задач

    Returns:
        dict: Граф зависимостей, где ключ - ID задачи, значение - список ID предшественников
    """
    # Инициализируем граф
    graph = {}
    task_map = {}  # Для быстрого доступа к задачам по ID

    # Создаем словарь задач по ID для быстрого доступа
    for task in tasks:
        task_id = task['id']
        graph[task_id] = []
        task_map[task_id] = task

    # Заполняем граф зависимостями
    for task in tasks:
        task_id = task['id']

        # Получаем зависимости из поля predecessors
        predecessors = []
        if 'predecessors' in task and task['predecessors']:
            if isinstance(task['predecessors'], list):
                predecessors.extend(task['predecessors'])
            elif isinstance(task['predecessors'], str):
                try:
                    pred_list = json.loads(task['predecessors'])
                    if isinstance(pred_list, list):
                        predecessors.extend(pred_list)
                except:
                    # Если не JSON, пробуем разделить по запятым
                    if ',' in task['predecessors']:
                        preds = [p.strip() for p in task['predecessors'].split(',')]
                        predecessors.extend(preds)

        # Получаем зависимости из базы данных
        db_deps = task_manager.get_task_dependencies(task_id)
        for dep in db_deps:
            predecessor_id = dep['predecessor_id']
            if predecessor_id not in predecessors:
                predecessors.append(predecessor_id)

        # Добавляем зависимости в граф
        for pred_id in predecessors:
            if pred_id in graph:  # Проверяем, что предшественник существует
                graph[task_id].append(pred_id)

    return graph, task_map


def topological_sort(graph):
    """
    Выполняет топологическую сортировку графа

    Args:
        graph (dict): Граф зависимостей

    Returns:
        list: Отсортированный список задач
    """
    # Подсчитываем входящие связи для каждой вершины
    in_degree = {node: 0 for node in graph}
    for node in graph:
        for neighbor in graph[node]:
            in_degree[neighbor] = in_degree.get(neighbor, 0) + 1

    # Инициализируем очередь вершинами без входящих связей
    queue = deque([node for node in graph if in_degree[node] == 0])
    result = []

    # Обходим граф
    while queue:
        node = queue.popleft()
        result.append(node)

        # Удаляем текущую вершину из графа
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # Проверяем на циклы
    if len(result) != len(graph):
        print("ПРЕДУПРЕЖДЕНИЕ: В графе обнаружены циклы! Некоторые зависимости могут быть нарушены.")

        # Добавляем оставшиеся вершины
        remaining = [node for node in graph if node not in result]
        for node in remaining:
            result.append(node)

    # Результат содержит задачи в порядке зависимостей от предшественников к последователям
    # Нам нужен обратный порядок - от источников к стокам
    return list(reversed(result))


def calculate_task_dates(sorted_tasks, graph, task_map, project_start_date, employee_manager):
    """
    Вычисляет даты начала и окончания задач с учетом зависимостей и выходных дней

    Args:
        sorted_tasks (list): Отсортированный список ID задач
        graph (dict): Граф зависимостей
        task_map (dict): Словарь задач по ID
        project_start_date (str): Дата начала проекта
        employee_manager: Менеджер сотрудников

    Returns:
        dict: Словарь с датами задач
    """
    # Форматируем даты
    project_start = datetime.datetime.strptime(project_start_date, '%Y-%m-%d')

    # Инициализируем словари для хранения дат
    earliest_start = {}  # Самое раннее время начала для каждой задачи
    task_dates = {}  # Итоговые даты для каждой задачи

    # Инициализируем загрузку сотрудников
    workload = defaultdict(lambda: defaultdict(int))

    # Распределяем задачи по сотрудникам и вычисляем даты
    for task_id in sorted_tasks:
        task = task_map[task_id]

        # Определяем самое раннее время начала задачи
        # Если нет предшественников, берем дату начала проекта
        if not graph[task_id]:
            earliest_start[task_id] = project_start
        else:
            # Иначе берем максимальную дату окончания среди предшественников
            max_end = project_start
            for pred_id in graph[task_id]:
                if pred_id in task_dates:
                    pred_end = datetime.datetime.strptime(task_dates[pred_id]['end'], '%Y-%m-%d')
                    next_day = pred_end + datetime.timedelta(days=1)
                    if next_day > max_end:
                        max_end = next_day
            earliest_start[task_id] = max_end

        # Если это групповая задача, пропускаем назначение сотрудника
        if task.get('is_group'):
            # Для групповой задачи устанавливаем предварительные даты
            task_dates[task_id] = {
                'start': earliest_start[task_id].strftime('%Y-%m-%d'),
                'end': (earliest_start[task_id] + datetime.timedelta(days=task['duration'] - 1)).strftime('%Y-%m-%d')
            }
            continue

        # Назначаем сотрудника и вычисляем реальные даты с учетом выходных
        employee_id = task.get('employee_id')
        position = task.get('position')

        if not position:
            # Если позиция не указана, используем предварительные даты
            task_dates[task_id] = {
                'start': earliest_start[task_id].strftime('%Y-%m-%d'),
                'end': (earliest_start[task_id] + datetime.timedelta(days=task['duration'] - 1)).strftime('%Y-%m-%d')
            }
            continue

        # Если сотрудник уже назначен, используем его
        if employee_id:
            # Проверяем, возможно ли выполнение задачи с учетом выходных
            start_date, end_date, calendar_duration = calculate_dates_with_days_off(
                task, earliest_start[task_id].strftime('%Y-%m-%d'),
                employee_id, employee_manager
            )

            if start_date and end_date:
                task_dates[task_id] = {
                    'start': start_date,
                    'end': end_date,
                    'employee_id': employee_id,
                    'duration': calendar_duration
                }
            else:
                # Если сотрудник не может выполнить задачу, ищем другого
                print(
                    f"Сотрудник {employee_id} не может выполнить задачу {task['name']} из-за выходных. Ищем другого...")
                employee_id = None

        # Если сотрудник не назначен, ищем подходящего
        if not employee_id:
            # Выбираем сотрудника с правильной должностью и наименьшей загрузкой
            result = find_suitable_employee(
                task, earliest_start[task_id].strftime('%Y-%m-%d'),
                position, employee_manager, workload
            )

            if result:
                employee_id, start_date, end_date, calendar_duration = result
                task_dates[task_id] = {
                    'start': start_date,
                    'end': end_date,
                    'employee_id': employee_id,
                    'duration': calendar_duration
                }

                # Обновляем загрузку сотрудника
                start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
                current_date = start_dt
                while current_date <= end_dt:
                    date_key = current_date.strftime('%Y-%m-%d')
                    workload[employee_id][date_key] += 1
                    current_date += datetime.timedelta(days=1)
            else:
                # Если не нашли подходящего сотрудника, используем предварительные даты
                print(f"Не найден подходящий сотрудник для задачи {task['name']}!")
                task_dates[task_id] = {
                    'start': earliest_start[task_id].strftime('%Y-%m-%d'),
                    'end': (earliest_start[task_id] + datetime.timedelta(days=task['duration'] - 1)).strftime(
                        '%Y-%m-%d')
                }

    # Обрабатываем групповые задачи после того, как все подзадачи обработаны
    for task_id in sorted_tasks:
        task = task_map[task_id]

        if task.get('is_group'):
            # Находим все подзадачи данной групповой задачи
            subtasks = [subtask_id for subtask_id in task_map
                        if task_map[subtask_id].get('parent_id') == task_id]

            if subtasks:
                # Определяем даты на основе подзадач
                start_dates = []
                end_dates = []

                for subtask_id in subtasks:
                    if subtask_id in task_dates:
                        start_dates.append(datetime.datetime.strptime(task_dates[subtask_id]['start'], '%Y-%m-%d'))
                        end_dates.append(datetime.datetime.strptime(task_dates[subtask_id]['end'], '%Y-%m-%d'))

                if start_dates and end_dates:
                    group_start = min(start_dates)
                    group_end = max(end_dates)

                    task_dates[task_id] = {
                        'start': group_start.strftime('%Y-%m-%d'),
                        'end': group_end.strftime('%Y-%m-%d')
                    }

    return task_dates


def calculate_dates_with_days_off(task, start_date_str, employee_id, employee_manager):
    # Конвертируем дату в объект datetime
    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')

    # Определяем длительность задачи в рабочих днях
    duration = task.get('duration', 1)

    # Сначала найдем первый доступный рабочий день
    first_day_found = False
    current_date = start_date
    calendar_days = 0

    while not first_day_found and calendar_days < duration * 3:
        date_str = current_date.strftime('%Y-%m-%d')
        if employee_manager.is_available(employee_id, date_str):
            first_day_found = True
            start_date = current_date
        else:
            current_date += datetime.timedelta(days=1)
            calendar_days += 1

    # Если не найден первый рабочий день, возвращаем None
    if not first_day_found:
        return None, None, None

    # Теперь считаем, сколько дней нужно для выполнения задачи
    working_days = 0
    calendar_days = 0
    current_date = start_date

    while working_days < duration and calendar_days < duration * 3:
        date_str = current_date.strftime('%Y-%m-%d')

        if employee_manager.is_available(employee_id, date_str):
            working_days += 1

        calendar_days += 1
        current_date += datetime.timedelta(days=1)

    # Вычисляем конечную дату (предыдущий день)
    end_date = current_date - datetime.timedelta(days=1)

    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), calendar_days

def find_suitable_employee(task, start_date_str, position, employee_manager, workload):
    """
    Находит подходящего сотрудника для выполнения задачи

    Args:
        task (dict): Задача
        start_date_str (str): Дата начала задачи
        position (str): Требуемая должность
        employee_manager: Менеджер сотрудников
        workload (dict): Текущая загрузка сотрудников

    Returns:
        tuple: (employee_id, start_date, end_date, calendar_duration) или None, если не найден
    """
    try:
        # Получаем список сотрудников требуемой должности
        employees = employee_manager.get_employees_by_position(position)

        if not employees:
            print(f"Не найдены сотрудники с должностью '{position}'")
            return None

        best_employee = None
        best_start_date = None
        best_end_date = None
        best_duration = float('inf')
        best_workload = float('inf')

        for employee in employees:
            employee_id = employee['id']

            # Рассчитываем даты с учетом выходных
            result = calculate_dates_with_days_off(
                task, start_date_str, employee_id, employee_manager
            )

            if not result:
                # Если не удалось рассчитать даты, пропускаем сотрудника
                continue

            start_date, end_date, calendar_duration = result

            # Рассчитываем текущую загрузку сотрудника
            current_load = sum(workload[employee_id].values())

            # Выбираем сотрудника с минимальной загрузкой или с минимальной длительностью выполнения
            if (best_employee is None or
                    current_load < best_workload or
                    (current_load == best_workload and calendar_duration < best_duration)):
                best_employee = employee
                best_start_date = start_date
                best_end_date = end_date
                best_duration = calendar_duration
                best_workload = current_load

        if best_employee:
            return best_employee['id'], best_start_date, best_end_date, best_duration
        else:
            return None

    except Exception as e:
        print(f"Ошибка при поиске подходящего сотрудника: {str(e)}")
        return None


def identify_critical_path(task_dates, graph, task_map):
    """
    Определяет критический путь проекта

    Args:
        task_dates (dict): Словарь с датами задач
        graph (dict): Граф зависимостей
        task_map (dict): Словарь задач по ID

    Returns:
        list: Список ID задач, образующих критический путь
    """
    # Находим задачу с самой поздней датой окончания
    latest_end_date = None
    latest_task_id = None

    for task_id, dates in task_dates.items():
        if 'end' in dates:
            end_date = datetime.datetime.strptime(dates['end'], '%Y-%m-%d')

            if latest_end_date is None or end_date > latest_end_date:
                latest_end_date = end_date
                latest_task_id = task_id

    if not latest_task_id:
        return []

    # Находим начальную задачу (без предшественников)
    start_tasks = [task_id for task_id in graph if not graph[task_id]]

    if not start_tasks:
        return []

    start_task_id = min(start_tasks, key=lambda tid: datetime.datetime.strptime(task_dates[tid]['start'], '%Y-%m-%d')
    if tid in task_dates and 'start' in task_dates[tid] else datetime.datetime.max)

    # Строим все возможные пути от начальной задачи к конечной
    all_paths = []

    def dfs(current, path, visited):
        path.append(current)
        visited.add(current)

        if current == latest_task_id:
            all_paths.append(list(path))
        else:
            # Находим всех потомков текущей задачи
            children = [tid for tid in graph if current in graph[tid]]

            for child in children:
                if child not in visited:
                    dfs(child, path, visited)

        path.pop()
        visited.remove(current)

    dfs(start_task_id, [], set())

    # Если пути не найдены, возвращаем самый длинный путь
    if not all_paths:
        # Находим самый длинный путь
        visited = set()
        max_path = []

        def find_longest_path(current, path):
            nonlocal max_path

            path.append(current)
            visited.add(current)

            # Если текущий путь длиннее максимального, обновляем максимальный
            if len(path) > len(max_path):
                max_path = list(path)

            # Находим всех потомков
            children = [tid for tid in graph if current in graph[tid]]

            for child in children:
                if child not in visited:
                    find_longest_path(child, path)

            path.pop()
            visited.remove(current)

        find_longest_path(start_task_id, [])
        return max_path

    # Находим путь с наибольшей длительностью
    path_durations = []

    for path in all_paths:
        # Рассчитываем длительность пути
        path_start = datetime.datetime.strptime(task_dates[path[0]]['start'], '%Y-%m-%d') if path[
                                                                                                 0] in task_dates and 'start' in \
                                                                                             task_dates[
                                                                                                 path[0]] else None
        path_end = datetime.datetime.strptime(task_dates[path[-1]]['end'], '%Y-%m-%d') if path[
                                                                                              -1] in task_dates and 'end' in \
                                                                                          task_dates[path[-1]] else None

        if path_start and path_end:
            duration = (path_end - path_start).days + 1
            path_durations.append((path, duration))

    if not path_durations:
        return []

    # Возвращаем путь с наибольшей длительностью
    return max(path_durations, key=lambda x: x[1])[0]


def schedule_project(project, tasks, task_manager, employee_manager):
    """
    Главная функция для планирования проекта

    Args:
        project (dict): Информация о проекте
        tasks (list): Список задач
        task_manager: Менеджер задач
        employee_manager: Менеджер сотрудников

    Returns:
        dict: Результаты планирования
    """
    for task in tasks:
        if task.get('employee_id'):
            task['project_start_date'] = project['start_date']
            adjust_task_duration_for_days_off(task, employee_manager)
    print(f"Начинаем планирование проекта '{project['name']}'...")

    # Шаг 1: Строим граф зависимостей
    graph, task_map = build_dependency_graph(tasks, task_manager)
    print(f"Построен граф зависимостей с {len(graph)} вершинами")

    # Шаг 2: Выполняем топологическую сортировку
    sorted_tasks = topological_sort(graph)
    print(f"Задачи отсортированы в порядке зависимостей")

    # Шаг 3: Вычисляем даты задач
    task_dates = calculate_task_dates(sorted_tasks, graph, task_map, project['start_date'], employee_manager)
    print(f"Рассчитаны даты для {len(task_dates)} задач")

    # Шаг 4: Определяем критический путь
    critical_path = identify_critical_path(task_dates, graph, task_map)
    print(f"Критический путь содержит {len(critical_path)} задач")

    # Шаг 5: Рассчитываем длительность проекта
    if task_dates:
        # Находим самую раннюю дату начала и самую позднюю дату окончания
        start_dates = [datetime.datetime.strptime(dates['start'], '%Y-%m-%d')
                       for dates in task_dates.values() if 'start' in dates]
        end_dates = [datetime.datetime.strptime(dates['end'], '%Y-%m-%d')
                     for dates in task_dates.values() if 'end' in dates]

        if start_dates and end_dates:
            project_start = min(start_dates)
            project_end = max(end_dates)
            project_duration = (project_end - project_start).days + 1

            print(f"Длительность проекта: {project_duration} дней")
            print(f"Дата начала: {project_start.strftime('%Y-%m-%d')}")
            print(f"Дата окончания: {project_end.strftime('%Y-%m-%d')}")
        else:
            project_duration = 0
    else:
        project_duration = 0

    # Формируем результат
    return {
        'task_dates': task_dates,
        'critical_path': critical_path,
        'duration': project_duration
    }


def update_database_assignments(task_dates, task_manager, employee_manager=None):
    """
    Обновляет назначения сотрудников и даты в базе данных

    Args:
        task_dates (dict): Словарь с датами и назначениями задач
        task_manager: Менеджер задач для обновления базы данных
        employee_manager: Менеджер сотрудников (опционально)
    """
    print(f"Обновление базы данных для {len(task_dates)} задач...")

    # Сохраняем даты и назначения в базу данных
    for task_id, task_data in task_dates.items():
        # Извлекаем данные
        start_date = task_data.get('start')
        end_date = task_data.get('end')
        employee_id = task_data.get('employee_id')

        # Обновляем даты задачи
        if start_date and end_date:
            task_manager.db.execute(
                "UPDATE tasks SET start_date = ?, end_date = ? WHERE id = ?",
                (start_date, end_date, task_id)
            )
            print(f"Обновлены даты для задачи {task_id}: {start_date} - {end_date}")

        # Обновляем назначенного сотрудника
        if employee_id:
            task_manager.db.execute(
                "UPDATE tasks SET employee_id = ? WHERE id = ?",
                (employee_id, task_id)
            )
            print(f"Назначен сотрудник {employee_id} на задачу {task_id}")

    # Обрабатываем подзадачи для групповых задач
    process_subtasks_for_groups(task_dates, task_manager, employee_manager)


def process_subtasks_for_groups(task_dates, task_manager, employee_manager=None):
    """
    Обрабатывает подзадачи для групповых задач, назначая даты и сотрудников с учетом выходных дней

    Args:
        task_dates (dict): Словарь с датами задач
        task_manager: Менеджер задач для работы с базой данных
        employee_manager: Менеджер сотрудников (опционально)
    """
    # Сначала импортируем все необходимые функции и модули
    import datetime
    from utils.employee_availability import check_employee_availability, find_available_date

    print("Обработка подзадач для групповых задач...")

    # Получаем список всех групповых задач с датами
    group_tasks = []
    for task_id, task_data in task_dates.items():
        if 'start' in task_data and 'end' in task_data:
            # Проверяем, является ли задача групповой
            task = task_manager.get_task(task_id)
            if task and task.get('is_group'):
                group_tasks.append((task_id, task_data))

    print(f"Найдено {len(group_tasks)} групповых задач для обработки")

    # Подготовка отслеживания загрузки сотрудников
    employee_workload = {}
    if employee_manager:
        employees = employee_manager.get_all_employees()
        for employee in employees:
            employee_workload[employee['id']] = 0

    # Вспомогательная функция для поиска доступного сотрудника
    def find_and_assign_employee(subtask, start_dt, duration, position):
        """Ищет доступного сотрудника на указанные даты с учетом выходных дней"""
        assigned_employee_id = None
        start_date = start_dt
        end_date = start_dt + datetime.timedelta(days=duration - 1)

        # Если должность указана и есть менеджер сотрудников
        if position and employee_manager:
            # Пытаемся найти подходящего сотрудника с учетом выходных дней
            suitable_employees = employee_manager.get_employees_by_position(position)

            if suitable_employees:
                # Сначала пробуем найти сотрудника, у которого нет выходных в этот период
                for employee in suitable_employees:
                    is_available = check_employee_availability(
                        employee['id'],
                        start_dt.strftime('%Y-%m-%d'),
                        duration,
                        employee_manager
                    )

                    if is_available:
                        # Нашли доступного сотрудника, выбираем с наименьшей загрузкой
                        employee_load = employee_workload.get(employee['id'], 0)
                        if assigned_employee_id is None or employee_load < employee_workload.get(assigned_employee_id,
                                                                                                 0):
                            assigned_employee_id = employee['id']

                # Если не нашли доступного сотрудника, выбираем наименее загруженного
                # и ищем для него подходящие даты
                if not assigned_employee_id and suitable_employees:
                    best_employee = min(suitable_employees, key=lambda e: employee_workload.get(e['id'], 0))
                    assigned_employee_id = best_employee['id']

                    # Ищем доступные даты с учетом выходных
                    new_start, new_end = find_available_date(
                        assigned_employee_id,
                        start_dt.strftime('%Y-%m-%d'),
                        duration,
                        employee_manager
                    )

                    if new_start and new_end:
                        # Обновляем даты начала и окончания
                        start_date = datetime.datetime.strptime(new_start, '%Y-%m-%d')
                        end_date = datetime.datetime.strptime(new_end, '%Y-%m-%d')
                        print(
                            f"Перенесли даты подзадачи {subtask['id']} на {new_start} - {new_end} для сотрудника {assigned_employee_id}")

        # Если сотрудник уже назначен в базе данных, используем его
        if subtask.get('employee_id'):
            assigned_employee_id = subtask['employee_id']

            # Проверяем доступность сотрудника с учетом выходных
            if employee_manager:
                is_available = check_employee_availability(
                    assigned_employee_id,
                    start_date.strftime('%Y-%m-%d'),
                    duration,
                    employee_manager
                )

                # Если сотрудник недоступен, ищем другие даты
                if not is_available:
                    new_start, new_end = find_available_date(
                        assigned_employee_id,
                        start_date.strftime('%Y-%m-%d'),
                        duration,
                        employee_manager
                    )

                    if new_start and new_end:
                        start_date = datetime.datetime.strptime(new_start, '%Y-%m-%d')
                        end_date = datetime.datetime.strptime(new_end, '%Y-%m-%d')
                        print(f"Перенесли даты подзадачи {subtask['id']} на {new_start} - {new_end} из-за выходных")

        # Обновляем загрузку назначенного сотрудника
        if assigned_employee_id:
            employee_workload[assigned_employee_id] = employee_workload.get(assigned_employee_id, 0) + duration

        return assigned_employee_id, start_date, end_date

    # Обрабатываем каждую групповую задачу
    for group_id, group_data in group_tasks:
        try:
            # Получаем подзадачи
            subtasks = task_manager.get_subtasks(group_id)

            if not subtasks:
                continue

            print(f"Обработка {len(subtasks)} подзадач для групповой задачи {group_id}")

            # Преобразуем строковые даты в объекты datetime
            group_start_dt = datetime.datetime.strptime(group_data.get('start'), '%Y-%m-%d')
            group_end_dt = datetime.datetime.strptime(group_data.get('end'), '%Y-%m-%d')

            # Пропускаем подзадачи, которые уже обработаны
            already_processed = [st['id'] for st in subtasks if st['id'] in task_dates]

            # Обрабатываем параллельные подзадачи
            parallel_subtasks = [st for st in subtasks if st.get('parallel') and st['id'] not in already_processed]
            for subtask in parallel_subtasks:
                subtask_id = subtask['id']
                subtask_duration = subtask.get('duration', 1)

                # Начинаем с даты начала групповой задачи
                subtask_start_dt = group_start_dt

                # Находим подходящего сотрудника и даты выполнения
                assigned_employee_id, actual_start_dt, actual_end_dt = find_and_assign_employee(
                    subtask,
                    subtask_start_dt,
                    subtask_duration,
                    subtask.get('position')
                )

                # Проверяем, чтобы конечная дата не выходила за пределы групповой задачи
                if actual_end_dt > group_end_dt:
                    actual_end_dt = group_end_dt

                # Обновляем даты и назначение в базе данных
                if assigned_employee_id:
                    task_manager.db.execute(
                        "UPDATE tasks SET start_date = ?, end_date = ?, employee_id = ? WHERE id = ?",
                        (actual_start_dt.strftime('%Y-%m-%d'), actual_end_dt.strftime('%Y-%m-%d'),
                         assigned_employee_id, subtask_id)
                    )
                    print(
                        f"Обновлены даты и назначен сотрудник {assigned_employee_id} для параллельной подзадачи {subtask_id}")
                else:
                    task_manager.db.execute(
                        "UPDATE tasks SET start_date = ?, end_date = ? WHERE id = ?",
                        (actual_start_dt.strftime('%Y-%m-%d'), actual_end_dt.strftime('%Y-%m-%d'),
                         subtask_id)
                    )
                    print(f"Обновлены даты для параллельной подзадачи {subtask_id} (сотрудник не назначен)")

            # Обрабатываем последовательные подзадачи
            sequential_subtasks = [st for st in subtasks if not st.get('parallel')]
            current_date = group_start_dt

            for subtask in sequential_subtasks:
                subtask_id = subtask['id']
                if subtask_id not in task_dates:  # Если подзадача еще не обработана
                    # Устанавливаем даты в зависимости от длительности
                    subtask_duration = subtask.get('duration', 1)
                    subtask_start = current_date
                    subtask_end = min(group_end_dt, subtask_start + datetime.timedelta(days=subtask_duration - 1))

                    # Назначаем сотрудника для подзадачи
                    assigned_employee_id = None

                    if employee_manager and subtask.get('position'):
                        position = subtask.get('position')
                        suitable_employees = employee_manager.get_employees_by_position(position)

                        if suitable_employees:
                            # Выбираем наименее загруженного сотрудника
                            best_employee = min(suitable_employees, key=lambda e: employee_workload.get(e['id'], 0))
                            assigned_employee_id = best_employee['id']

                            # ВАЖНО! Проверяем доступность и корректируем даты
                            is_available = check_employee_availability(
                                assigned_employee_id,
                                subtask_start.strftime('%Y-%m-%d'),
                                subtask_duration,
                                employee_manager
                            )

                            if not is_available:
                                # Ищем доступные даты с учетом выходных
                                new_start, new_end = find_available_date(
                                    assigned_employee_id,
                                    subtask_start.strftime('%Y-%m-%d'),
                                    subtask_duration,
                                    employee_manager
                                )

                                if new_start and new_end:
                                    # ИСПРАВЛЕНО: Обновляем переменные, которые используются ниже
                                    subtask_start = datetime.datetime.strptime(new_start, '%Y-%m-%d')
                                    subtask_end = datetime.datetime.strptime(new_end, '%Y-%m-%d')
                                    print(
                                        f"Перенесли даты подзадачи {subtask_id} на {new_start}-{new_end} из-за выходных")

                            # Обновляем загрузку сотрудника
                            employee_workload[assigned_employee_id] = employee_workload.get(assigned_employee_id,
                                                                                            0) + subtask_duration

                    # Если сотрудник уже назначен, используем его
                    if subtask.get('employee_id'):
                        assigned_employee_id = subtask['employee_id']

                        # Проверяем доступность с учетом выходных
                        is_available = check_employee_availability(
                            assigned_employee_id,
                            subtask_start.strftime('%Y-%m-%d'),
                            subtask_duration,
                            employee_manager
                        )

                        if not is_available:
                            print(f"Сотрудник {assigned_employee_id} недоступен на дату {subtask_start.strftime('%Y-%m-%d')}")
                            # Ищем доступные даты
                            new_start, new_end = find_available_date(
                                assigned_employee_id,
                                subtask_start.strftime('%Y-%m-%d'),
                                subtask_duration,
                                employee_manager
                            )

                            if new_start and new_end:
                                print(f"Найдены новые даты: {new_start} - {new_end}")
                                # ИСПРАВЛЕНО: Обновляем переменные subtask_start и subtask_end
                                subtask_start = datetime.datetime.strptime(new_start, '%Y-%m-%d')
                                subtask_end = datetime.datetime.strptime(new_end, '%Y-%m-%d')
                                print(f"Перенесли даты подзадачи {subtask_id} на {new_start}-{new_end} из-за выходных")

                    # Обновляем даты и назначение в базе данных
                    if assigned_employee_id:
                        employee_workload[assigned_employee_id] = employee_workload.get(assigned_employee_id, 0) + subtask_duration
                        # Обновляем даты и назначенного сотрудника
                        task_manager.db.execute(
                            "UPDATE tasks SET start_date = ?, end_date = ?, employee_id = ? WHERE id = ?",
                            (subtask_start.strftime('%Y-%m-%d'), subtask_end.strftime('%Y-%m-%d'), assigned_employee_id,
                             subtask_id)
                        )
                        print(
                            f"Обновлены даты и назначен сотрудник {assigned_employee_id} для последовательной подзадачи {subtask_id}")
                    else:
                        # Обновляем только даты
                        task_manager.db.execute(
                            "UPDATE tasks SET start_date = ?, end_date = ? WHERE id = ?",
                            (subtask_start.strftime('%Y-%m-%d'), subtask_end.strftime('%Y-%m-%d'), subtask_id)
                        )
                        print(f"Обновлены даты для последовательной подзадачи {subtask_id} (сотрудник не назначен)")

                    # Переходим к следующей дате - ВАЖНО использовать актуальную дату окончания
                    current_date = subtask_end + datetime.timedelta(days=1)

        except Exception as e:
            print(f"Ошибка при обработке подзадач для групповой задачи {group_id}: {str(e)}")
            import traceback
            print(traceback.format_exc())

def adjust_task_duration_for_days_off(task, employee_manager):
    """
    Корректирует длительность задачи с учетом выходных дней сотрудника
    """
    try:
        employee_id = task.get('employee_id')
        if not employee_id:
            return

        original_duration = task.get('duration', 0)
        if not original_duration:
            return

        # Предполагаем начало с даты проекта
        from datetime import datetime, timedelta
        project_start_date = datetime.strptime(task.get('project_start_date', '2025-01-01'), '%Y-%m-%d')

        # Рассчитываем, сколько календарных дней потребуется для выполнения задачи
        calendar_days = 0
        working_days = 0
        current_date = project_start_date

        while working_days < original_duration and calendar_days < 100:  # Защита от бесконечного цикла
            calendar_days += 1
            date_str = current_date.strftime('%Y-%m-%d')

            if employee_manager.is_available(employee_id, date_str):
                working_days += 1

            current_date += timedelta(days=1)

        # Обновляем длительность задачи
        if calendar_days > original_duration:
            print(f"Задача '{task.get('name', 'Неизвестная')}' (ID: {task.get('id', 'Неизвестный')}): "
                  f"календарная длительность скорректирована с {original_duration} до {calendar_days} дней")
            task['adjusted_duration'] = calendar_days

            # Возвращаем скорректированные данные
            return calendar_days, original_duration  # calendar_duration, working_duration
    except Exception as e:
        print(f"Ошибка при корректировке длительности задачи {task.get('name', 'Unknown')}: {str(e)}")

    return None