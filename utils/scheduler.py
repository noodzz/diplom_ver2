"""
Полная обновленная версия файла utils/scheduler.py с поддержкой подзадач
"""
import datetime
import json
from collections import defaultdict, deque

# Импортируем новые функции работы с доступностью сотрудников
from utils.employee_availability import find_suitable_employee, get_available_dates_for_task


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
    print(f"Начинаем планирование проекта '{project['name']}'...")

    # Шаг 1: Строим граф зависимостей
    graph, task_map = build_dependency_graph(tasks, task_manager)
    print(f"Построен граф зависимостей с {len(graph)} вершинами")

    # Шаг 2: Выполняем топологическую сортировку - получаем задачи в порядке выполнения
    sorted_tasks = topological_sort(graph)
    print(f"Задачи отсортированы в порядке зависимостей")

    # Шаг 3: Вычисляем даты задач
    task_dates = calculate_task_dates(sorted_tasks, graph, task_map, project['start_date'],
                                     task_manager, employee_manager)
    print(f"Рассчитаны даты для {len(task_dates)} задач")

    # Шаг 3.5: Обрабатываем подзадачи групповых задач
    process_subtasks(task_dates, task_map, graph, task_manager, employee_manager)
    print(f"Обработаны подзадачи, теперь дат в task_dates: {len(task_dates)}")

    # Шаг 4: Определяем критический путь
    critical_path = identify_critical_path(task_dates, graph, task_map)
    print(f"Критический путь содержит {len(critical_path)} задач")

    # Шаг 5: Рассчитываем длительность проекта
    project_duration = calculate_project_duration(project['start_date'], task_dates)
    print(f"Длительность проекта: {project_duration} дней")

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

    # Обновляем даты и назначения в базе данных
    for task_id, task_data in task_dates.items():
        # Извлекаем данные
        start_date = task_data.get('start')
        end_date = task_data.get('end')
        employee_id = task_data.get('employee_id')

        try:
            # Обновляем даты задачи
            if start_date and end_date:
                task_manager.db.execute(
                    "UPDATE tasks SET start_date = ?, end_date = ? WHERE id = ?",
                    (start_date, end_date, task_id)
                )
                print(f"Задача {task_id}: обновлены даты {start_date} - {end_date}")

            # Обновляем назначенного сотрудника
            if employee_id is not None:
                task_manager.db.execute(
                    "UPDATE tasks SET employee_id = ? WHERE id = ?",
                    (employee_id, task_id)
                )

                # Если есть менеджер сотрудников, выводим информацию о назначении
                if employee_manager:
                    try:
                        employee = employee_manager.get_employee(employee_id)
                        print(f"Задача {task_id}: назначен сотрудник {employee['name']} ({employee['position']})")
                    except Exception:
                        print(f"Задача {task_id}: назначен сотрудник {employee_id}")
        except Exception as e:
            print(f"Ошибка при обновлении задачи {task_id}: {str(e)}")


def build_dependency_graph(tasks, task_manager):
    """
    Строит граф зависимостей между задачами

    Args:
        tasks (list): Список задач
        task_manager: Менеджер задач

    Returns:
        tuple: (graph, task_map) - граф зависимостей и словарь задач по ID
    """
    # Инициализируем граф и словарь задач
    graph = {}  # task_id -> список ID предшественников
    task_map = {}  # task_id -> task

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
    Выполняет топологическую сортировку графа - упорядочивает задачи по зависимостям

    Args:
        graph (dict): Граф зависимостей

    Returns:
        list: Отсортированный список ID задач
    """
    # Подсчитываем входящие связи для каждой вершины
    in_degree = {node: 0 for node in graph}
    for node, predecessors in graph.items():
        for pred in predecessors:
            in_degree[pred] = in_degree.get(pred, 0) + 1

    # Инициализируем очередь вершинами без предшественников
    queue = deque([node for node in graph if not graph[node]])
    result = []

    # Обходим граф
    while queue:
        node = queue.popleft()
        result.append(node)

        # Для каждой зависимой задачи
        for dependent in graph:
            if node in graph[dependent]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

    # Проверяем на циклы
    if len(result) != len(graph):
        print("ПРЕДУПРЕЖДЕНИЕ: В графе обнаружены циклы! Некоторые зависимости могут быть нарушены.")

        # Добавляем оставшиеся вершины в результат
        remaining = [node for node in graph if node not in result]
        result.extend(remaining)

    return result


def calculate_task_dates(sorted_tasks, graph, task_map, project_start_date,
                         task_manager, employee_manager):
    """
    Вычисляет даты начала и окончания задач с учетом зависимостей и выходных дней

    Args:
        sorted_tasks (list): Отсортированный список ID задач
        graph (dict): Граф зависимостей
        task_map (dict): Словарь задач по ID
        project_start_date (str): Дата начала проекта
        task_manager: Менеджер задач
        employee_manager: Менеджер сотрудников

    Returns:
        dict: Словарь с датами задач
    """
    # Инициализируем словарь для хранения дат и загрузки сотрудников
    task_dates = {}
    employee_workload = {}

    # Получаем дату начала проекта
    project_start = datetime.datetime.strptime(project_start_date, '%Y-%m-%d')

    # Обрабатываем задачи в порядке зависимостей
    for task_id in sorted_tasks:
        task = task_map[task_id]
        task_duration = task.get('duration', 1)

        # Определяем самую раннюю дату начала задачи
        # Если у задачи нет предшественников, используем дату начала проекта
        if not graph[task_id]:
            start_date = project_start
        else:
            # Иначе берем максимальную дату окончания среди предшественников
            max_end_date = None
            for pred_id in graph[task_id]:
                if pred_id in task_dates:
                    pred_end_date = datetime.datetime.strptime(task_dates[pred_id]['end'], '%Y-%m-%d')
                    pred_next_day = pred_end_date + datetime.timedelta(days=1)

                    if max_end_date is None or pred_next_day > max_end_date:
                        max_end_date = pred_next_day

            start_date = max_end_date if max_end_date else project_start

        start_date_str = start_date.strftime('%Y-%m-%d')

        # Обработка групповой задачи
        if task.get('is_group'):
            # Для групповой задачи устанавливаем предварительные даты
            task_end = start_date + datetime.timedelta(days=task_duration - 1)
            task_dates[task_id] = {
                'start': start_date_str,
                'end': task_end.strftime('%Y-%m-%d')
            }
            continue

        # Проверяем, назначен ли уже сотрудник на задачу
        employee_id = task.get('employee_id')
        position = task.get('position')

        if employee_id:
            # Если сотрудник уже назначен, проверяем его доступность
            employee_start, employee_end, calendar_duration = get_available_dates_for_task(
                employee_id, start_date_str, task_duration, employee_manager
            )

            if employee_start:
                # Сотрудник доступен, используем рассчитанные даты
                task_dates[task_id] = {
                    'start': employee_start,
                    'end': employee_end,
                    'employee_id': employee_id
                }

                # Обновляем загрузку сотрудника
                employee_workload[employee_id] = employee_workload.get(employee_id, 0) + task_duration
            else:
                # Если сотрудник недоступен, ищем другого сотрудника с той же должностью
                if position:
                    new_employee_id, new_start, new_end, new_duration = find_suitable_employee(
                        position, start_date_str, task_duration, employee_manager, employee_workload
                    )

                    if new_employee_id:
                        task_dates[task_id] = {
                            'start': new_start,
                            'end': new_end,
                            'employee_id': new_employee_id
                        }
                    else:
                        # Если не удалось найти подходящего сотрудника, используем исходные даты
                        end_date = start_date + datetime.timedelta(days=task_duration - 1)
                        task_dates[task_id] = {
                            'start': start_date_str,
                            'end': end_date.strftime('%Y-%m-%d'),
                            'employee_id': employee_id  # Сохраняем исходного сотрудника
                        }
                else:
                    # Если должность не указана, используем стандартные даты
                    end_date = start_date + datetime.timedelta(days=task_duration - 1)
                    task_dates[task_id] = {
                        'start': start_date_str,
                        'end': end_date.strftime('%Y-%m-%d'),
                        'employee_id': employee_id
                    }
        elif position:
            # Если сотрудник не назначен, но должность указана, ищем подходящего сотрудника
            new_employee_id, new_start, new_end, new_duration = find_suitable_employee(
                position, start_date_str, task_duration, employee_manager, employee_workload
            )

            if new_employee_id:
                task_dates[task_id] = {
                    'start': new_start,
                    'end': new_end,
                    'employee_id': new_employee_id
                }
            else:
                # Если не удалось найти подходящего сотрудника, используем стандартные даты
                end_date = start_date + datetime.timedelta(days=task_duration - 1)
                task_dates[task_id] = {
                    'start': start_date_str,
                    'end': end_date.strftime('%Y-%m-%d')
                }
        else:
            # Если не указана должность, используем стандартные даты
            end_date = start_date + datetime.timedelta(days=task_duration - 1)
            task_dates[task_id] = {
                'start': start_date_str,
                'end': end_date.strftime('%Y-%m-%d')
            }

    # Обрабатываем групповые задачи на основе дат подзадач
    process_group_tasks(task_dates, task_map, graph)

    return task_dates


def process_subtasks(task_dates, task_map, graph, task_manager, employee_manager):
    """
    Обрабатывает подзадачи групповых задач, устанавливая для них даты и назначая исполнителей

    Args:
        task_dates (dict): Словарь с датами задач
        task_map (dict): Словарь задач по ID
        graph (dict): Граф зависимостей
        task_manager: Менеджер задач
        employee_manager: Менеджер сотрудников
    """
    import datetime
    from utils.employee_availability import find_suitable_employee, get_available_dates_for_task

    print("Обработка подзадач групповых задач...")

    # Словарь для отслеживания загрузки сотрудников
    employee_workload = {}

    # Сначала проверим все задачи в task_map и найдем потенциальные подзадачи
    all_subtasks_count = 0
    for task_id, task in task_map.items():
        if task.get('parent_id'):
            all_subtasks_count += 1
            parent_id = task.get('parent_id')
            print(
                f"ОТЛАДКА: Найдена потенциальная подзадача {task_id}: {task.get('name', 'Без имени')} с parent_id={parent_id}")

            # Проверим, существует ли родительская задача
            if parent_id in task_map:
                parent_task = task_map[parent_id]
                print(
                    f"  Родительская задача существует: {parent_id}: {parent_task.get('name', 'Без имени')}, is_group={parent_task.get('is_group', False)}")
            else:
                print(f"  ОШИБКА: Родительская задача {parent_id} не найдена в task_map!")

    print(f"ОТЛАДКА: Всего найдено {all_subtasks_count} потенциальных подзадач")

    # Дополнительно, получим подзадачи напрямую через task_manager
    for group_id, task in task_map.items():
        if task.get('is_group'):
            try:
                db_subtasks = task_manager.get_subtasks(group_id)
                print(
                    f"ОТЛАДКА: Для групповой задачи {group_id}: {task.get('name', 'Без имени')} task_manager вернул {len(db_subtasks)} подзадач")

                for subtask in db_subtasks:
                    st_id = subtask.get('id')
                    st_name = subtask.get('name', 'Без имени')
                    st_parent = subtask.get('parent_id')
                    print(f"  Подзадача в БД: id={st_id}, name={st_name}, parent_id={st_parent}")

                    # Проверим, есть ли эта подзадача в task_map
                    if st_id in task_map:
                        print(f"    Подзадача найдена в task_map")
                    else:
                        print(f"    ОШИБКА: Подзадача НЕ найдена в task_map!")
            except Exception as e:
                print(f"  Ошибка при получении подзадач из БД: {str(e)}")

    # Находим все групповые задачи с установленными датами
    group_tasks = [(task_id, task) for task_id, task in task_map.items()
                   if task.get('is_group') and task_id in task_dates]

    print(f"Найдено {len(group_tasks)} групповых задач для обработки подзадач")

    # Для каждой групповой задачи обрабатываем ее подзадачи
    for group_id, group_task in group_tasks:
        # Получаем даты групповой задачи
        if 'start' not in task_dates[group_id] or 'end' not in task_dates[group_id]:
            continue

        group_start_str = task_dates[group_id]['start']
        group_end_str = task_dates[group_id]['end']

        group_start = datetime.datetime.strptime(group_start_str, '%Y-%m-%d')
        group_end = datetime.datetime.strptime(group_end_str, '%Y-%m-%d')

        print(
            f"Обработка групповой задачи {group_id}: {group_task.get('name', 'Без имени')} ({group_start_str} - {group_end_str})")

        # Получаем все подзадачи данной групповой задачи двумя способами
        subtasks_from_map = []
        for task_id, task in task_map.items():
            parent_id = task.get('parent_id')
            # Проверяем разные типы parent_id, так как проблема может быть в несовпадении типов
            if parent_id == group_id or str(parent_id) == str(group_id):
                subtasks_from_map.append(task)
                print(f"  Найдена подзадача в task_map: {task_id}: {task.get('name', 'Без имени')}")

        # Получаем подзадачи из базы данных
        try:
            subtasks_from_db = task_manager.get_subtasks(group_id)
            print(f"  Найдено {len(subtasks_from_db)} подзадач в базе данных")

            # Добавляем подзадачи из базы данных, которых нет в task_map
            for subtask in subtasks_from_db:
                st_id = subtask.get('id')
                if st_id not in [t.get('id') for t in subtasks_from_map]:
                    subtasks_from_map.append(subtask)
                    task_map[st_id] = subtask
                    print(f"  Добавлена подзадача из БД: {st_id}: {subtask.get('name', 'Без имени')}")
        except Exception as e:
            print(f"  Ошибка при получении подзадач из БД: {str(e)}")

        # Если все еще нет подзадач, проверяем поле subtasks в групповой задаче
        if not subtasks_from_map and 'subtasks' in group_task:
            print(f"  В групповой задаче найдено поле subtasks с {len(group_task['subtasks'])} подзадачами")

            for subtask_data in group_task['subtasks']:
                # Преобразуем данные подзадачи из поля subtasks в формат задачи
                subtask = {
                    'id': f"{group_id}_sub_{len(subtasks_from_map)}",  # Генерируем временный ID
                    'name': subtask_data.get('name', 'Подзадача'),
                    'duration': subtask_data.get('duration', 1),
                    'position': subtask_data.get('position', ''),
                    'parallel': subtask_data.get('parallel', False),
                    'parent_id': group_id
                }
                subtasks_from_map.append(subtask)
                task_map[subtask['id']] = subtask
                print(f"  Добавлена подзадача из поля subtasks: {subtask['id']}: {subtask['name']}")

        if not subtasks_from_map:
            print(f"  ПРЕДУПРЕЖДЕНИЕ: Не найдено подзадач для групповой задачи {group_id}")
            continue

        print(f"Обработка {len(subtasks_from_map)} подзадач для групповой задачи {group_id}")

        # Разделяем подзадачи на параллельные и последовательные
        parallel_subtasks = [task for task in subtasks_from_map if task.get('parallel')]
        sequential_subtasks = [task for task in subtasks_from_map if not task.get('parallel')]

        print(f"Параллельных подзадач: {len(parallel_subtasks)}, последовательных: {len(sequential_subtasks)}")

        # Обрабатываем параллельные подзадачи - все начинаются одновременно с начала групповой задачи
        for subtask in parallel_subtasks:
            subtask_id = subtask['id']
            subtask_duration = subtask.get('duration', 1)
            subtask_position = subtask.get('position')
            employee_id = subtask.get('employee_id')

            print(f"Обработка параллельной подзадачи {subtask_id}: {subtask.get('name', 'Без имени')}")

            # Если у подзадачи уже назначен сотрудник
            if employee_id:
                # Проверяем его доступность
                avail_start, avail_end, calendar_duration = get_available_dates_for_task(
                    employee_id, group_start_str, subtask_duration, employee_manager
                )

                if avail_start:
                    # Сотрудник доступен
                    task_dates[subtask_id] = {
                        'start': avail_start,
                        'end': avail_end,
                        'employee_id': employee_id
                    }

                    # Обновляем загрузку сотрудника
                    employee_workload[employee_id] = employee_workload.get(employee_id, 0) + subtask_duration

                    print(
                        f"  Для подзадачи {subtask_id} назначен сотрудник {employee_id}, даты: {avail_start} - {avail_end}")
                else:
                    # Ищем другого подходящего сотрудника
                    if subtask_position:
                        new_employee_id, new_start, new_end, new_duration = find_suitable_employee(
                            subtask_position, group_start_str, subtask_duration, employee_manager, employee_workload
                        )

                        if new_employee_id:
                            task_dates[subtask_id] = {
                                'start': new_start,
                                'end': new_end,
                                'employee_id': new_employee_id
                            }
                            print(
                                f"  Для подзадачи {subtask_id} назначен новый сотрудник {new_employee_id}, даты: {new_start} - {new_end}")
                        else:
                            # Не нашли подходящего сотрудника, используем исходные даты
                            end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
                            task_dates[subtask_id] = {
                                'start': group_start_str,
                                'end': end_date.strftime('%Y-%m-%d'),
                                'employee_id': employee_id
                            }
                            print(
                                f"  Для подзадачи {subtask_id} нет доступных сотрудников, используем стандартные даты")
                    else:
                        # Нет должности, используем стандартные даты
                        end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
                        task_dates[subtask_id] = {
                            'start': group_start_str,
                            'end': end_date.strftime('%Y-%m-%d'),
                            'employee_id': employee_id
                        }
                        print(f"  Для подзадачи {subtask_id} нет должности, используем стандартные даты")
            elif subtask_position:
                # Ищем подходящего сотрудника
                new_employee_id, new_start, new_end, new_duration = find_suitable_employee(
                    subtask_position, group_start_str, subtask_duration, employee_manager, employee_workload
                )

                if new_employee_id:
                    task_dates[subtask_id] = {
                        'start': new_start,
                        'end': new_end,
                        'employee_id': new_employee_id
                    }
                    print(
                        f"  Для подзадачи {subtask_id} назначен сотрудник {new_employee_id}, даты: {new_start} - {new_end}")
                else:
                    # Не нашли подходящего сотрудника, используем стандартные даты
                    end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
                    task_dates[subtask_id] = {
                        'start': group_start_str,
                        'end': end_date.strftime('%Y-%m-%d')
                    }
                    print(f"  Для подзадачи {subtask_id} нет доступных сотрудников, используем стандартные даты")
            else:
                # Нет ни сотрудника, ни должности, используем стандартные даты
                end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
                task_dates[subtask_id] = {
                    'start': group_start_str,
                    'end': end_date.strftime('%Y-%m-%d')
                }
                print(f"  Для подзадачи {subtask_id} нет ни сотрудника, ни должности, используем стандартные даты")

        # Обрабатываем последовательные подзадачи - идут одна за другой
        current_date = group_start

        for subtask in sequential_subtasks:
            subtask_id = subtask['id']
            subtask_duration = subtask.get('duration', 1)
            subtask_position = subtask.get('position')
            employee_id = subtask.get('employee_id')

            print(f"Обработка последовательной подзадачи {subtask_id}: {subtask.get('name', 'Без имени')}")

            # Текущая дата начала подзадачи
            start_date_str = current_date.strftime('%Y-%m-%d')

            # Если у подзадачи уже назначен сотрудник
            if employee_id:
                # Проверяем его доступность
                avail_start, avail_end, calendar_duration = get_available_dates_for_task(
                    employee_id, start_date_str, subtask_duration, employee_manager
                )

                if avail_start:
                    # Сотрудник доступен
                    task_dates[subtask_id] = {
                        'start': avail_start,
                        'end': avail_end,
                        'employee_id': employee_id
                    }

                    # Обновляем загрузку сотрудника
                    employee_workload[employee_id] = employee_workload.get(employee_id, 0) + subtask_duration

                    # Следующая подзадача начинается после текущей
                    next_date = datetime.datetime.strptime(avail_end, '%Y-%m-%d') + datetime.timedelta(days=1)

                    print(
                        f"  Для подзадачи {subtask_id} назначен сотрудник {employee_id}, даты: {avail_start} - {avail_end}")

                    # Обновляем текущую дату для следующей подзадачи
                    current_date = next_date
                else:
                    # Ищем другого подходящего сотрудника
                    if subtask_position:
                        new_employee_id, new_start, new_end, new_duration = find_suitable_employee(
                            subtask_position, start_date_str, subtask_duration, employee_manager, employee_workload
                        )

                        if new_employee_id:
                            task_dates[subtask_id] = {
                                'start': new_start,
                                'end': new_end,
                                'employee_id': new_employee_id
                            }

                            # Следующая подзадача начинается после текущей
                            next_date = datetime.datetime.strptime(new_end, '%Y-%m-%d') + datetime.timedelta(days=1)
                            current_date = next_date

                            print(
                                f"  Для подзадачи {subtask_id} назначен новый сотрудник {new_employee_id}, даты: {new_start} - {new_end}")
                        else:
                            # Не нашли подходящего сотрудника, используем стандартные даты
                            end_date = current_date + datetime.timedelta(days=subtask_duration - 1)
                            task_dates[subtask_id] = {
                                'start': start_date_str,
                                'end': end_date.strftime('%Y-%m-%d'),
                                'employee_id': employee_id
                            }

                            # Следующая подзадача начинается после текущей
                            current_date = end_date + datetime.timedelta(days=1)

                            print(
                                f"  Для подзадачи {subtask_id} нет доступных сотрудников, используем стандартные даты")
                    else:
                        # Нет должности, используем стандартные даты
                        end_date = current_date + datetime.timedelta(days=subtask_duration - 1)
                        task_dates[subtask_id] = {
                            'start': start_date_str,
                            'end': end_date.strftime('%Y-%m-%d'),
                            'employee_id': employee_id
                        }

                        # Следующая подзадача начинается после текущей
                        current_date = end_date + datetime.timedelta(days=1)

                        print(f"  Для подзадачи {subtask_id} нет должности, используем стандартные даты")
            elif subtask_position:
                # Ищем подходящего сотрудника
                new_employee_id, new_start, new_end, new_duration = find_suitable_employee(
                    subtask_position, start_date_str, subtask_duration, employee_manager, employee_workload
                )

                if new_employee_id:
                    task_dates[subtask_id] = {
                        'start': new_start,
                        'end': new_end,
                        'employee_id': new_employee_id
                    }

                    # Следующая подзадача начинается после текущей
                    next_date = datetime.datetime.strptime(new_end, '%Y-%m-%d') + datetime.timedelta(days=1)
                    current_date = next_date

                    print(
                        f"  Для подзадачи {subtask_id} назначен сотрудник {new_employee_id}, даты: {new_start} - {new_end}")
                else:
                    # Не нашли подходящего сотрудника, используем стандартные даты
                    end_date = current_date + datetime.timedelta(days=subtask_duration - 1)
                    task_dates[subtask_id] = {
                        'start': start_date_str,
                        'end': end_date.strftime('%Y-%m-%d')
                    }

                    # Следующая подзадача начинается после текущей
                    current_date = end_date + datetime.timedelta(days=1)

                    print(f"  Для подзадачи {subtask_id} нет доступных сотрудников, используем стандартные даты")
            else:
                # Нет ни сотрудника, ни должности, используем стандартные даты
                end_date = current_date + datetime.timedelta(days=subtask_duration - 1)
                task_dates[subtask_id] = {
                    'start': start_date_str,
                    'end': end_date.strftime('%Y-%m-%d')
                }

                # Следующая подзадача начинается после текущей
                current_date = end_date + datetime.timedelta(days=1)

                print(f"  Для подзадачи {subtask_id} нет ни сотрудника, ни должности, используем стандартные даты")

        # Проверяем, что ни одна подзадача не выходит за пределы групповой задачи
        # Если выходит, корректируем даты групповой задачи
        latest_subtask_end = None
        for subtask in subtasks_from_map:
            subtask_id = subtask['id']
            if subtask_id in task_dates and 'end' in task_dates[subtask_id]:
                subtask_end = datetime.datetime.strptime(task_dates[subtask_id]['end'], '%Y-%m-%d')
                if latest_subtask_end is None or subtask_end > latest_subtask_end:
                    latest_subtask_end = subtask_end

        # Если последняя подзадача заканчивается позже, чем групповая задача,
        # обновляем дату окончания групповой задачи
        if latest_subtask_end and latest_subtask_end > group_end:
            task_dates[group_id]['end'] = latest_subtask_end.strftime('%Y-%m-%d')
            print(f"Дата окончания групповой задачи {group_id} обновлена до {latest_subtask_end.strftime('%Y-%m-%d')}")

    processed_subtasks = sum(1 for tid in task_dates if tid in task_map and task_map[tid].get('parent_id'))
    print(f"Обработка подзадач завершена. Обработано {processed_subtasks} подзадач.")

def process_group_tasks(task_dates, task_map, graph):
    """
    Обрабатывает даты групповых задач на основе дат их подзадач

    Args:
        task_dates (dict): Словарь с датами задач
        task_map (dict): Словарь задач по ID
        graph (dict): Граф зависимостей
    """
    # Находим все групповые задачи
    group_tasks = [task_id for task_id, task in task_map.items() if task.get('is_group')]

    for group_id in group_tasks:
        # Находим все подзадачи данной групповой задачи
        subtasks = [task_id for task_id, task in task_map.items()
                    if task.get('parent_id') == group_id]

        if subtasks:
            # Собираем даты начала и окончания подзадач
            start_dates = []
            end_dates = []

            for subtask_id in subtasks:
                if subtask_id in task_dates:
                    if 'start' in task_dates[subtask_id]:
                        start_dates.append(datetime.datetime.strptime(
                            task_dates[subtask_id]['start'], '%Y-%m-%d'))
                    if 'end' in task_dates[subtask_id]:
                        end_dates.append(datetime.datetime.strptime(
                            task_dates[subtask_id]['end'], '%Y-%m-%d'))

            # Если у подзадач есть даты, обновляем даты групповой задачи
            if start_dates and end_dates:
                group_start = min(start_dates)
                group_end = max(end_dates)

                task_dates[group_id] = {
                    'start': group_start.strftime('%Y-%m-%d'),
                    'end': group_end.strftime('%Y-%m-%d')
                }


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
    if not task_dates:
        return []

    # Создаем обратный граф (для поиска зависимых задач)
    reverse_graph = defaultdict(list)
    for task_id, predecessors in graph.items():
        for pred_id in predecessors:
            reverse_graph[pred_id].append(task_id)

    # Находим задачи без зависимых задач (конечные)
    end_tasks = [task_id for task_id in graph if not reverse_graph[task_id]]

    # Если нет конечных задач, находим задачу с самой поздней датой окончания
    if not end_tasks:
        end_task_id = max(
            task_dates.keys(),
            key=lambda tid: datetime.datetime.strptime(task_dates[tid]['end'], '%Y-%m-%d')
            if 'end' in task_dates[tid] else datetime.datetime.min
        )
    else:
        # Из конечных задач выбираем ту, что заканчивается позже всех
        end_task_id = max(
            end_tasks,
            key=lambda tid: datetime.datetime.strptime(task_dates[tid]['end'], '%Y-%m-%d')
            if tid in task_dates and 'end' in task_dates[tid] else datetime.datetime.min
        )

    # Находим задачи без предшественников (начальные)
    start_tasks = [task_id for task_id in graph if not graph[task_id]]
    if not start_tasks:
        start_task_id = min(
            task_dates.keys(),
            key=lambda tid: datetime.datetime.strptime(task_dates[tid]['start'], '%Y-%m-%d')
            if 'start' in task_dates[tid] else datetime.datetime.max
        )
    else:
        # Из начальных задач выбираем ту, что начинается раньше всех
        start_task_id = min(
            start_tasks,
            key=lambda tid: datetime.datetime.strptime(task_dates[tid]['start'], '%Y-%m-%d')
            if tid in task_dates and 'start' in task_dates[tid] else datetime.datetime.max
        )

    # Находим все пути от начальной до конечной задачи
    all_paths = []

    def find_paths(current, path, visited):
        path.append(current)
        visited.add(current)

        if current == end_task_id:
            all_paths.append(list(path))
        else:
            for next_id in reverse_graph[current]:
                if next_id not in visited:
                    find_paths(next_id, path, visited)

        path.pop()
        visited.remove(current)

    # Ищем пути от конечной задачи к начальным (используя обратный граф)
    find_paths(start_task_id, [], set())

    # Если нет путей, возвращаем пустой список
    if not all_paths:
        return []

    # Вычисляем длительность каждого пути
    path_durations = []
    for path in all_paths:
        path_duration = 0
        for task_id in path:
            if task_id in task_map:
                path_duration += task_map[task_id].get('duration', 0)

        path_durations.append((path, path_duration))

    # Возвращаем путь с наибольшей длительностью
    if path_durations:
        critical_path, _ = max(path_durations, key=lambda x: x[1])
        return critical_path
    return []


def calculate_project_duration(project_start_date, task_dates):
    """
    Рассчитывает общую длительность проекта в днях

    Args:
        project_start_date (str): Дата начала проекта
        task_dates (dict): Словарь с датами задач

    Returns:
        int: Длительность проекта в днях
    """
    if not task_dates:
        return 0

    try:
        # Находим самую раннюю дату начала и самую позднюю дату окончания
        project_start = datetime.datetime.strptime(project_start_date, '%Y-%m-%d')

        earliest_start = project_start
        latest_end = project_start

        for task_id, dates in task_dates.items():
            if 'start' in dates:
                start = datetime.datetime.strptime(dates['start'], '%Y-%m-%d')
                if start < earliest_start:
                    earliest_start = start

            if 'end' in dates:
                end = datetime.datetime.strptime(dates['end'], '%Y-%m-%d')
                if end > latest_end:
                    latest_end = end

        # Вычисляем длительность в днях
        duration = (latest_end - earliest_start).days + 1  # +1 так как включаем день окончания
        return duration
    except Exception as e:
        print(f"Ошибка при расчете длительности проекта: {str(e)}")
        return 0