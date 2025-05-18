"""
Updated version of utils/scheduler.py with improved dependency handling and parallel subtask assignment
"""
import datetime
import json
from collections import defaultdict, deque

# Импортируем новые функции работы с доступностью сотрудников
from utils.employee_availability import find_suitable_employee, get_available_dates_for_task


def schedule_project(project, tasks, task_manager, employee_manager):
    """
    Главная функция для планирования проекта с исправленной обработкой дублирующихся назначений

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

    # Выводим зависимости для отладки
    for task_id, predecessors in graph.items():
        if predecessors:
            task_name = task_map[task_id]['name'] if task_id in task_map else f"Неизвестная задача {task_id}"
            pred_names = []
            for pred_id in predecessors:
                pred_name = task_map[pred_id]['name'] if pred_id in task_map else f"Неизвестная задача {pred_id}"
                pred_names.append(f"{pred_id}:{pred_name}")
            print(f"Задача {task_id}:{task_name} зависит от: {', '.join(pred_names)}")

    # Шаг 2: Выполняем топологическую сортировку - получаем задачи в порядке выполнения
    sorted_tasks = topological_sort(graph)
    print(f"Задачи отсортированы в порядке зависимостей, всего {len(sorted_tasks)} задач")

    # Дополнительная проверка корректности топологической сортировки
    for i, task_id in enumerate(sorted_tasks):
        if task_id in graph:
            for pred_id in graph[task_id]:
                try:
                    pred_index = sorted_tasks.index(pred_id)
                    if pred_index > i:
                        print(f"ОШИБКА: Задача {task_id} обрабатывается раньше её предшественника {pred_id}!")
                except ValueError:
                    print(f"ОШИБКА: Предшественник {pred_id} задачи {task_id} отсутствует в отсортированном списке!")

    # Шаг 3: Вычисляем даты задач
    task_dates = {}  # Предварительно инициализируем пустой словарь

    # Проходим по всем задачам в отсортированном порядке
    for task_id in sorted_tasks:
        # Проверяем, есть ли задача в task_map
        if task_id not in task_map:
            print(f"ПРЕДУПРЕЖДЕНИЕ: Задача {task_id} есть в графе, но отсутствует в task_map!")
            continue

        task = task_map[task_id]
        task_name = task.get('name', f"Задача {task_id}")

        # Получаем предшественников задачи
        predecessors = graph.get(task_id, [])

        # Определяем дату начала задачи на основе предшественников
        start_date = None
        if not predecessors:
            # Если нет предшественников, начинаем с даты начала проекта
            import datetime
            start_date = datetime.datetime.strptime(project['start_date'], '%Y-%m-%d')
            print(f"Задача {task_id}: {task_name} - начало с даты начала проекта: {start_date.strftime('%Y-%m-%d')}")
        else:
            # Проверяем, все ли предшественники имеют даты окончания
            missing_predecessors = []
            for pred_id in predecessors:
                if pred_id not in task_dates or 'end' not in task_dates[pred_id]:
                    missing_predecessors.append(pred_id)

            if missing_predecessors:
                # У некоторых предшественников нет дат окончания
                print(
                    f"ПРЕДУПРЕЖДЕНИЕ: Предшественники {missing_predecessors} задачи {task_id}: {task_name} не имеют дат окончания!")

                # Пытаемся вычислить даты отсутствующих предшественников
                for missing_pred in missing_predecessors:
                    # Если предшественник в task_map, пытаемся вычислить его даты
                    if missing_pred in task_map:
                        missing_task = task_map[missing_pred]
                        missing_name = missing_task.get('name', f"Задача {missing_pred}")

                        # Определяем предварительную дату начала для отсутствующего предшественника
                        import datetime
                        pred_start_date = datetime.datetime.strptime(project['start_date'], '%Y-%m-%d')
                        pred_duration = missing_task.get('duration', 1)

                        # Вычисляем дату окончания
                        pred_end_date = pred_start_date + datetime.timedelta(days=pred_duration - 1)

                        # Добавляем в task_dates
                        task_dates[missing_pred] = {
                            'start': pred_start_date.strftime('%Y-%m-%d'),
                            'end': pred_end_date.strftime('%Y-%m-%d')
                        }

                        print(
                            f"Для предшественника {missing_pred}: {missing_name} были установлены даты: {pred_start_date.strftime('%Y-%m-%d')} - {pred_end_date.strftime('%Y-%m-%d')}")

            # Определяем дату начала на основе самой поздней даты окончания предшественников
            import datetime
            latest_end_date = None

            for pred_id in predecessors:
                if pred_id in task_dates and 'end' in task_dates[pred_id]:
                    pred_end = datetime.datetime.strptime(task_dates[pred_id]['end'], '%Y-%m-%d')
                    pred_next_day = pred_end + datetime.timedelta(days=1)

                    if latest_end_date is None or pred_next_day > latest_end_date:
                        latest_end_date = pred_next_day
                        print(
                            f"  Предшественник {pred_id} заканчивается {pred_end.strftime('%Y-%m-%d')}, следующий день: {pred_next_day.strftime('%Y-%m-%d')}")

            if latest_end_date:
                start_date = latest_end_date
                print(
                    f"Задача {task_id}: {task_name} - начало определено на основе предшественников: {start_date.strftime('%Y-%m-%d')}")
            else:
                # Если не удалось определить даты предшественников, используем дату начала проекта
                start_date = datetime.datetime.strptime(project['start_date'], '%Y-%m-%d')
                print(
                    f"ПРЕДУПРЕЖДЕНИЕ: Для задачи {task_id}: {task_name} не удалось определить даты предшественников, используем дату начала проекта: {start_date.strftime('%Y-%m-%d')}")

        # Теперь, когда у нас есть start_date, вычисляем дату окончания и сохраняем в task_dates
        import datetime
        task_duration = task.get('duration', 1)

        if task.get('is_group'):
            # Для групповой задачи пока устанавливаем предварительные даты
            end_date = start_date + datetime.timedelta(days=task_duration - 1)
            task_dates[task_id] = {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            }
            print(
                f"Групповая задача {task_id}: {task_name} - предварительные даты: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
        else:
            # Для обычной задачи учитываем выходные дни
            employee_id = task.get('employee_id')
            position = task.get('position')

            if employee_id:
                # Проверяем доступность сотрудника
                from utils.employee_availability import get_available_dates_for_task
                employee_start, employee_end, calendar_duration = get_available_dates_for_task(
                    employee_id, start_date.strftime('%Y-%m-%d'), task_duration, employee_manager
                )

                if employee_start:
                    task_dates[task_id] = {
                        'start': employee_start,
                        'end': employee_end,
                        'employee_id': employee_id
                    }
                    print(
                        f"Задача {task_id}: {task_name} - назначен сотрудник {employee_id}, даты: {employee_start} - {employee_end}")
                else:
                    # Не удалось назначить сотрудника, используем стандартные даты
                    end_date = start_date + datetime.timedelta(days=task_duration - 1)
                    task_dates[task_id] = {
                        'start': start_date.strftime('%Y-%m-%d'),
                        'end': end_date.strftime('%Y-%m-%d'),
                        'employee_id': employee_id
                    }
                    print(
                        f"Задача {task_id}: {task_name} - сотрудник {employee_id} недоступен, используем стандартные даты: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
            elif position:
                # Ищем подходящего сотрудника
                from utils.employee_availability import find_suitable_employee
                new_employee_id, new_start, new_end, new_duration = find_suitable_employee(
                    position, start_date.strftime('%Y-%m-%d'), task_duration, employee_manager
                )

                if new_employee_id:
                    task_dates[task_id] = {
                        'start': new_start,
                        'end': new_end,
                        'employee_id': new_employee_id
                    }
                    print(
                        f"Задача {task_id}: {task_name} - назначен новый сотрудник {new_employee_id}, даты: {new_start} - {new_end}")
                else:
                    # Не нашли подходящего сотрудника
                    end_date = start_date + datetime.timedelta(days=task_duration - 1)
                    task_dates[task_id] = {
                        'start': start_date.strftime('%Y-%m-%d'),
                        'end': end_date.strftime('%Y-%m-%d')
                    }
                    print(
                        f"Задача {task_id}: {task_name} - не найден подходящий сотрудник, используем стандартные даты: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
            else:
                # Ни сотрудник, ни должность не указаны
                end_date = start_date + datetime.timedelta(days=task_duration - 1)
                task_dates[task_id] = {
                    'start': start_date.strftime('%Y-%m-%d'),
                    'end': end_date.strftime('%Y-%m-%d')
                }
                print(
                    f"Задача {task_id}: {task_name} - ни сотрудник, ни должность не указаны, используем стандартные даты: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")

    # Шаг 3.5: Обрабатываем подзадачи групповых задач с обновленным алгоритмом
    process_subtasks(task_dates, task_map, graph, task_manager, employee_manager)
    print(f"Обработаны подзадачи, теперь дат в task_dates: {len(task_dates)}")

    # Шаг 3.6: Проверяем и исправляем зависимости после изменений в датах
    revalidate_dependent_tasks(task_dates, graph, task_map, task_manager, employee_manager)
    print(f"Проведена повторная проверка зависимостей между задачами")

    # ОТКЛЮЧАЕМ стандартную балансировку нагрузки, так как она может снова создать дубликаты
    # Новая обработка уже балансирует нагрузку более интеллектуально
    task_dates = balance_employee_workload(task_dates, task_map, employee_manager)
    print("Стандартная балансировка нагрузки отключена, так как интеллектуальное распределение уже выполнено")

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


def process_subtasks(task_dates, task_map, graph, task_manager, employee_manager):
    """
    Обрабатывает подзадачи групповых задач, устанавливая для них даты и назначая исполнителей,
    со специальной обработкой параллельных подзадач с учетом их должностей.

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

    # ИЗМЕНЕНИЕ: Словарь для отслеживания уже назначенных типов подзадач с учетом должности
    # Ключ: (group_id, subtask_name, position, timeframe) -> Значение: список назначенных сотрудников
    subtask_assignments = {}

    # ИЗМЕНЕНИЕ: Структура для отслеживания дублирующихся подзадач с учетом должности
    # Ключ: (group_id, subtask_name, position) -> Список объектов подзадач
    duplicate_subtasks = {}

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

        # Получаем все подзадачи данной групповой задачи
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

        # ИЗМЕНЕНИЕ: Группируем подзадачи по имени И должности
        subtask_groups = {}  # (имя задачи, должность) -> список задач с этим именем и должностью

        for subtask in subtasks_from_map:
            subtask_name = subtask.get('name', '')
            subtask_position = subtask.get('position', '')  # Учитываем должность
            group_key = (subtask_name, subtask_position)

            if group_key not in subtask_groups:
                subtask_groups[group_key] = []
            subtask_groups[group_key].append(subtask)

            # Отслеживаем дублирующиеся подзадачи для последующего анализа
            duplicate_key = (str(group_id), subtask_name, subtask_position)
            if duplicate_key not in duplicate_subtasks:
                duplicate_subtasks[duplicate_key] = []
            duplicate_subtasks[duplicate_key].append(subtask)

        # Выводим информацию о дублирующихся подзадачах по группам должностей
        for (subtask_name, position), tasks_list in subtask_groups.items():
            if len(tasks_list) > 1:
                print(f"  Найдено {len(tasks_list)} экземпляров подзадачи '{subtask_name}' с должностью '{position}'")
                parallel_count = sum(1 for task in tasks_list if task.get('parallel'))
                print(
                    f"    Из них параллельных: {parallel_count}, последовательных: {len(tasks_list) - parallel_count}")

        # Разделяем подзадачи на параллельные и последовательные
        parallel_subtasks = [task for task in subtasks_from_map if task.get('parallel')]
        sequential_subtasks = [task for task in subtasks_from_map if not task.get('parallel')]

        print(f"Параллельных подзадач: {len(parallel_subtasks)}, последовательных: {len(sequential_subtasks)}")

        # ИЗМЕНЕНИЕ: Обрабатываем параллельные подзадачи по группам с учетом должности
        for (subtask_name, position), similar_subtasks in subtask_groups.items():
            # Отфильтровываем только параллельные подзадачи этого типа и должности
            parallel_group = [task for task in similar_subtasks if task.get('parallel')]

            if not parallel_group:
                continue  # Пропускаем, если нет параллельных подзадач этого типа

            print(f"Обработка {len(parallel_group)} параллельных подзадач типа '{subtask_name}' с должностью '{position}'")

            # Получаем доступных сотрудников для данной должности
            if not position:
                print(f"  ПРЕДУПРЕЖДЕНИЕ: Не указана должность для подзадачи '{subtask_name}'")
                continue

            try:
                available_employees = employee_manager.get_employees_by_position(position)
                print(f"  Найдено {len(available_employees)} сотрудников с должностью '{position}'")

                # Если сотрудников меньше, чем подзадач, выводим предупреждение
                if len(available_employees) < len(parallel_group):
                    print(
                        f"  ПРЕДУПРЕЖДЕНИЕ: Количество сотрудников ({len(available_employees)}) меньше количества подзадач ({len(parallel_group)})")
                    print(f"  Некоторым сотрудникам придется назначить более одной подзадачи этого типа")

                # Определяем ключ для отслеживания назначений группы подзадач с учетом должности
                group_key = (str(group_id), subtask_name, position, group_start_str + "-" + group_end_str)
                if group_key not in subtask_assignments:
                    subtask_assignments[group_key] = []

                # Инициализируем список для отслеживания назначенных сотрудников в этой группе
                assigned_employees = []

                # Назначаем каждую подзадачу наиболее подходящему сотруднику, избегая дублирования
                for idx, subtask in enumerate(parallel_group):
                    subtask_id = subtask['id']
                    subtask_duration = subtask.get('duration', 1)

                    # Если сотрудник уже назначен, проверяем его доступность
                    employee_id = subtask.get('employee_id')

                    if employee_id and employee_id not in assigned_employees:
                        # Проверяем доступность сотрудника
                        avail_start, avail_end, _ = get_available_dates_for_task(
                            employee_id, group_start_str, subtask_duration, employee_manager
                        )

                        if avail_start:
                            # Сотрудник доступен и еще не назначен на другую подзадачу этого типа
                            task_dates[subtask_id] = {
                                'start': avail_start,
                                'end': avail_end,
                                'employee_id': employee_id
                            }

                            # Добавляем сотрудника в список назначенных
                            assigned_employees.append(employee_id)
                            subtask_assignments[group_key].append(employee_id)

                            # Обновляем загрузку сотрудника
                            employee_workload[employee_id] = employee_workload.get(employee_id, 0) + subtask_duration

                            print(f"  Подзадача {subtask_id}: сохранен назначенный сотрудник {employee_id}")
                            continue  # Переходим к следующей подзадаче

                    # Если уже назначенный сотрудник недоступен или уже назначен на другую подзадачу,
                    # либо сотрудник еще не назначен, выбираем нового сотрудника

                    # Отфильтровываем уже назначенных сотрудников
                    available_unassigned = [e for e in available_employees if e['id'] not in assigned_employees]

                    if available_unassigned:
                        # Есть доступные ненаряженные сотрудники
                        # Сортируем по нагрузке
                        sorted_by_workload = sorted(
                            available_unassigned,
                            key=lambda e: employee_workload.get(e['id'], 0)
                        )

                        # Выбираем наименее загруженного
                        new_employee = sorted_by_workload[0]
                        new_employee_id = new_employee['id']

                        # Проверяем его доступность
                        avail_start, avail_end, _ = get_available_dates_for_task(
                            new_employee_id, group_start_str, subtask_duration, employee_manager
                        )

                        if avail_start:
                            # Назначаем сотрудника
                            task_dates[subtask_id] = {
                                'start': avail_start,
                                'end': avail_end,
                                'employee_id': new_employee_id
                            }

                            # Добавляем в списки назначенных
                            assigned_employees.append(new_employee_id)
                            subtask_assignments[group_key].append(new_employee_id)

                            # Обновляем загрузку
                            employee_workload[new_employee_id] = employee_workload.get(new_employee_id,
                                                                                       0) + subtask_duration

                            try:
                                employee_name = new_employee.get('name', f"ID:{new_employee_id}")
                                print(f"  Подзадача {subtask_id}: назначен новый сотрудник {employee_name}")
                            except:
                                print(f"  Подзадача {subtask_id}: назначен новый сотрудник {new_employee_id}")
                        else:
                            print(
                                f"  ПРЕДУПРЕЖДЕНИЕ: Выбранный сотрудник {new_employee_id} недоступен в указанный период")
                            # Используем стандартные даты и оставляем без назначения
                            end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
                            task_dates[subtask_id] = {
                                'start': group_start_str,
                                'end': end_date.strftime('%Y-%m-%d')
                            }
                    else:
                        # Если все сотрудники уже назначены, начинаем назначать повторно,
                        # выбирая наименее загруженных
                        sorted_by_workload = sorted(
                            available_employees,
                            key=lambda e: employee_workload.get(e['id'], 0)
                        )

                        # Берем наименее загруженного из всех
                        best_employee = sorted_by_workload[0] if sorted_by_workload else None

                        if best_employee:
                            best_employee_id = best_employee['id']

                            # Проверяем доступность
                            avail_start, avail_end, _ = get_available_dates_for_task(
                                best_employee_id, group_start_str, subtask_duration, employee_manager
                            )

                            if avail_start:
                                # Назначаем этого сотрудника (повторно)
                                task_dates[subtask_id] = {
                                    'start': avail_start,
                                    'end': avail_end,
                                    'employee_id': best_employee_id
                                }

                                # Не добавляем в assigned_employees, так как это повторное назначение
                                subtask_assignments[group_key].append(best_employee_id)

                                # Обновляем загрузку
                                employee_workload[best_employee_id] = employee_workload.get(best_employee_id,
                                                                                            0) + subtask_duration

                                print(
                                    f"  Подзадача {subtask_id}: назначен повторно сотрудник {best_employee_id} (нет других свободных сотрудников)")
                            else:
                                # Если сотрудник недоступен, используем стандартные даты
                                end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
                                task_dates[subtask_id] = {
                                    'start': group_start_str,
                                    'end': end_date.strftime('%Y-%m-%d')
                                }
                                print(f"  ПРЕДУПРЕЖДЕНИЕ: Не удалось назначить сотрудника на подзадачу {subtask_id}")
                        else:
                            # Если вообще нет сотрудников (странная ситуация)
                            end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
                            task_dates[subtask_id] = {
                                'start': group_start_str,
                                'end': end_date.strftime('%Y-%m-%d')
                            }
                            print(f"  КРИТИЧЕСКАЯ ОШИБКА: Не найдены сотрудники для должности '{position}'")
            except Exception as e:
                print(f"  Ошибка при обработке подзадач типа '{subtask_name}' с должностью '{position}': {str(e)}")
                import traceback
                print(traceback.format_exc())

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

            # Проверяем, что дата начала подзадачи не выходит за пределы групповой задачи
            if current_date > group_end:
                print(f"  ПРЕДУПРЕЖДЕНИЕ: Подзадача {subtask_id} не помещается в рамки групповой задачи!")

                # Выбираем наиболее разумное поведение:
                # 1. Можно принудительно уменьшить длительность подзадачи
                # 2. Можно расширить рамки групповой задачи (этот вариант будем использовать)
                group_end = current_date + datetime.timedelta(days=subtask_duration - 1)
                task_dates[group_id]['end'] = group_end.strftime('%Y-%m-%d')
                print(f"  Расширяем групповую задачу до {group_end.strftime('%Y-%m-%d')}")

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

    # Выводим статистику по назначениям
    print("\nСтатистика по назначениям подзадач:")
    for group_key, assigned_employees in subtask_assignments.items():
        group_id, subtask_name, position, timeframe = group_key
        emp_count = len(assigned_employees)
        unique_emp_count = len(set(assigned_employees))

        if emp_count > unique_emp_count:
            print(
                f"ВНИМАНИЕ: Группа '{subtask_name}' с должностью '{position}' в {group_id}: назначено {emp_count} подзадач на {unique_emp_count} сотрудников")
            # Подсчитываем количество назначений для каждого сотрудника
            from collections import Counter
            employee_counts = Counter(assigned_employees)
            duplicates = {emp_id: count for emp_id, count in employee_counts.items() if count > 1}
            if duplicates:
                print(f"  Дублирующиеся назначения: {duplicates}")
        else:
            print(
                f"Группа '{subtask_name}' с должностью '{position}' в {group_id}: назначено {emp_count} подзадач на {unique_emp_count} сотрудников (OK)")

    processed_subtasks = sum(1 for tid in task_dates if tid in task_map and task_map[tid].get('parent_id'))
    print(f"Обработка подзадач завершена. Обработано {processed_subtasks} подзадач.")


def revalidate_dependent_tasks(task_dates, graph, task_map, task_manager, employee_manager):
    """
    Проверяет и обновляет даты задач, имеющих зависимости, после обновления групповых задач

    Args:
        task_dates (dict): Словарь с датами задач
        graph (dict): Граф зависимостей
        task_map (dict): Словарь задач по ID
        task_manager: Менеджер задач
        employee_manager: Менеджер сотрудников
    """
    import datetime

    print("Проверка и корректировка зависимостей между задачами...")

    # Создаем обратный граф для поиска зависимых задач
    reverse_graph = {}
    for task_id, predecessors in graph.items():
        for pred_id in predecessors:
            if pred_id not in reverse_graph:
                reverse_graph[pred_id] = []
            reverse_graph[pred_id].append(task_id)

    # Отладочная информация
    print(f"Построен обратный граф зависимостей с {len(reverse_graph)} вершинами")

    # Проходим по всем задачам, от которых зависят другие задачи
    updates_needed = []

    for task_id in reverse_graph:
        if task_id not in task_dates or 'end' not in task_dates[task_id]:
            continue

        # Получаем дату окончания текущей задачи
        end_date_str = task_dates[task_id]['end']
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
        next_day_date = end_date + datetime.timedelta(days=1)
        next_day_str = next_day_date.strftime('%Y-%m-%d')

        # Проверяем все зависимые задачи
        for dependent_id in reverse_graph[task_id]:
            if dependent_id not in task_dates or 'start' not in task_dates[dependent_id]:
                continue

            dependent_start_str = task_dates[dependent_id]['start']
            dependent_start = datetime.datetime.strptime(dependent_start_str, '%Y-%m-%d')

            # Если дата начала зависимой задачи раньше или равна дате окончания предшествующей задачи,
            # нужно сдвинуть зависимую задачу на следующий день
            if dependent_start <= end_date:
                # Получаем имя задачи для логирования
                task_name = task_map[task_id]['name'] if task_id in task_map else f"Задача {task_id}"
                dep_name = task_map[dependent_id]['name'] if dependent_id in task_map else f"Задача {dependent_id}"

                print(f"Обнаружен конфликт: задача {dependent_id} ({dep_name}) начинается {dependent_start_str}, "
                      f"но её предшественник {task_id} ({task_name}) заканчивается {end_date_str}")

                # Добавляем в список задач для обновления
                updates_needed.append((dependent_id, next_day_str))

    # Применяем обновления
    for dependent_id, new_start_str in updates_needed:
        task = task_map.get(dependent_id)
        if not task:
            continue

        task_name = task.get('name', f"Задача {dependent_id}")
        task_duration = task.get('duration', 1)
        position = task.get('position')  # Получаем должность для задачи

        print(f"Обновление даты начала для задачи {dependent_id}: {task_name} на {new_start_str}")

        # Обновляем дату начала и рассчитываем новую дату окончания
        if task.get('is_group'):
            # Для групповой задачи устанавливаем стандартные даты
            new_start = datetime.datetime.strptime(new_start_str, '%Y-%m-%d')
            new_end = new_start + datetime.timedelta(days=task_duration - 1)

            task_dates[dependent_id] = {
                'start': new_start_str,
                'end': new_end.strftime('%Y-%m-%d')
            }

            print(f"Групповая задача {dependent_id}: {task_name} - обновленные даты: "
                  f"{new_start_str} - {new_end.strftime('%Y-%m-%d')}")
        else:
            # Для обычной задачи учитываем выходные дни
            employee_id = task.get('employee_id')

            if employee_id:
                # Проверяем доступность сотрудника
                employee_start, employee_end, calendar_duration = get_available_dates_for_task(
                    employee_id, new_start_str, task_duration, employee_manager
                )

                if employee_start:
                    task_dates[dependent_id] = {
                        'start': employee_start,
                        'end': employee_end,
                        'employee_id': employee_id
                    }
                    print(f"Задача {dependent_id}: {task_name} - обновленные даты: {employee_start} - {employee_end}")
                else:
                    # Сотрудник недоступен, используем стандартные даты
                    new_start = datetime.datetime.strptime(new_start_str, '%Y-%m-%d')
                    new_end = new_start + datetime.timedelta(days=task_duration - 1)

                    task_dates[dependent_id] = {
                        'start': new_start_str,
                        'end': new_end.strftime('%Y-%m-%d'),
                        'employee_id': employee_id
                    }
            elif position:  # Если не назначен сотрудник, но указана должность
                # Ищем подходящего сотрудника
                new_employee_id, new_start, new_end, new_duration = find_suitable_employee(
                    position, new_start_str, task_duration, employee_manager
                )

                if new_employee_id:
                    task_dates[dependent_id] = {
                        'start': new_start,
                        'end': new_end,
                        'employee_id': new_employee_id
                    }
                    print(f"Задача {dependent_id}: {task_name} - назначен сотрудник {new_employee_id}, даты: {new_start} - {new_end}")
                else:
                    # Используем стандартные даты
                    new_start = datetime.datetime.strptime(new_start_str, '%Y-%m-%d')
                    new_end = new_start + datetime.timedelta(days=task_duration - 1)

                    task_dates[dependent_id] = {
                        'start': new_start_str,
                        'end': new_end.strftime('%Y-%m-%d')
                    }
                    print(f"Задача {dependent_id}: {task_name} - не найден подходящий сотрудник")
            else:
                # Используем стандартные даты
                new_start = datetime.datetime.strptime(new_start_str, '%Y-%m-%d')
                new_end = new_start + datetime.timedelta(days=task_duration - 1)

                task_dates[dependent_id] = {
                    'start': new_start_str,
                    'end': new_end.strftime('%Y-%m-%d')
                }

        # Рекурсивно обновляем задачи, зависящие от текущей
        if dependent_id in reverse_graph:
            for next_dependent_id in reverse_graph[dependent_id]:
                # Если задача уже в списке обновлений, пропускаем
                if not any(next_dependent_id == task_id for task_id, _ in updates_needed):
                    next_start = datetime.datetime.strptime(task_dates[dependent_id]['end'], '%Y-%m-%d') + datetime.timedelta(days=1)
                    updates_needed.append((next_dependent_id, next_start.strftime('%Y-%m-%d')))


def balance_employee_workload(task_dates, task_map, employee_manager):
    """
    Балансирует нагрузку между сотрудниками одной должности,
    предотвращая дублирование назначений на идентичные подзадачи

    Args:
        task_dates (dict): Словарь с датами и назначениями задач
        task_map (dict): Словарь задач по ID
        employee_manager: Менеджер сотрудников

    Returns:
        dict: Обновленный словарь task_dates с более сбалансированной нагрузкой
    """
    import datetime

    print("Запуск балансировки нагрузки сотрудников...")

    # Собираем текущую нагрузку по сотрудникам и должностям
    employee_workload = {}  # employee_id -> рабочих дней
    position_employees = {}  # должность -> список сотрудников

    # Словарь для отслеживания задач каждого сотрудника
    employee_tasks = {}  # employee_id -> список (task_id, task, duration)

    # Создаем структуры для отслеживания подзадач одного типа с учетом должности
    # Ключ: (parent_id, name, position, timeframe) -> Значение: список назначенных сотрудников
    similar_subtask_assignments = {}

    # Структура для отслеживания групповых задач и их подзадач
    group_tasks = {}  # group_id -> list of subtask_ids
    subtask_to_group = {}  # subtask_id -> group_id

    # Шаг 1: Получаем все должности из конфигурации
    all_positions = set()
    try:
        from data.config import Config
        all_positions = set(Config.POSITIONS)
        print(f"Получены все должности из конфигурации: {all_positions}")
    except Exception as e:
        print(f"Ошибка при получении списка должностей: {str(e)}")
        # В случае неудачи, получаем должности из сотрудников
        try:
            all_employees = employee_manager.get_all_employees()
            all_positions = set(emp.get('position') for emp in all_employees if emp.get('position'))
            print(f"Получены должности из списка сотрудников: {all_positions}")
        except:
            print("Не удалось получить список должностей")

    # Шаг 2: Для каждой должности получаем всех доступных сотрудников
    all_available_employees = {}  # должность -> список сотрудников
    for position in all_positions:
        try:
            employees = employee_manager.get_employees_by_position(position)
            if employees:
                all_available_employees[position] = employees
                position_employees[position] = [emp['id'] for emp in employees]

                # Инициализируем словари для всех сотрудников
                for emp in employees:
                    emp_id = emp['id']
                    if emp_id not in employee_workload:
                        employee_workload[emp_id] = 0
                        employee_tasks[emp_id] = []

                print(f"Должность '{position}': найдено {len(employees)} сотрудников")
        except Exception as e:
            print(f"Ошибка при получении сотрудников для должности '{position}': {str(e)}")

    # Шаг 3: Анализируем текущие назначения и идентифицируем групповые задачи и подзадачи
    for task_id, dates in task_dates.items():
        try:
            # Преобразуем task_id в числовой формат, если это строка
            task_id_num = int(task_id) if isinstance(task_id, str) and task_id.isdigit() else task_id

            # Получаем информацию о задаче
            task = None
            try:
                # Пробуем разные варианты поиска задачи в task_map
                if str(task_id) in task_map:
                    task = task_map[str(task_id)]
                elif task_id in task_map:
                    task = task_map[task_id]
                elif isinstance(task_id, str) and task_id.isdigit() and int(task_id) in task_map:
                    task = task_map[int(task_id)]
            except Exception as e:
                print(f"Ошибка при поиске задачи {task_id}: {str(e)}")
                continue

            if not task:
                print(f"Не найдена задача с ID {task_id} в task_map")
                continue

            # Отслеживаем групповые задачи и подзадачи
            if task.get('is_group'):
                group_tasks[task_id] = []

            # Если это подзадача, добавляем в соответствующую группу
            parent_id = task.get('parent_id')
            if parent_id:
                parent_id_str = str(parent_id)
                if parent_id_str in group_tasks:
                    group_tasks[parent_id_str].append(task_id)
                elif parent_id in group_tasks:
                    group_tasks[parent_id].append(task_id)

                # Запоминаем к какой группе относится подзадача
                subtask_to_group[task_id] = parent_id

            # Отслеживаем назначения сотрудников
            employee_id = dates.get('employee_id')
            if not employee_id:
                continue

            # Получаем длительность задачи
            task_duration = task.get('duration', 1)

            # Суммируем нагрузку для сотрудника
            if employee_id not in employee_workload:
                employee_workload[employee_id] = 0
                employee_tasks[employee_id] = []

            employee_workload[employee_id] += task_duration
            employee_tasks[employee_id].append((task_id, task, task_duration))

            # ИЗМЕНЕНИЕ: Для подзадач создаем ключ с учетом должности
            if parent_id:
                task_name = task.get('name', '')
                position = task.get('position', '')  # Добавляем позицию
                timeframe = f"{dates.get('start')}-{dates.get('end')}"
                subtask_key = (str(parent_id), task_name, position, timeframe)

                if subtask_key not in similar_subtask_assignments:
                    similar_subtask_assignments[subtask_key] = []

                if employee_id not in similar_subtask_assignments[subtask_key]:
                    similar_subtask_assignments[subtask_key].append(employee_id)

            # Получаем должность сотрудника и добавляем в position_employees, если ещё не там
            try:
                employee = employee_manager.get_employee(employee_id)
                position = employee.get('position')

                if position:
                    if position not in position_employees:
                        position_employees[position] = []
                    if employee_id not in position_employees[position]:
                        position_employees[position].append(employee_id)
            except Exception as e:
                print(f"Ошибка при получении информации о сотруднике {employee_id}: {str(e)}")
        except Exception as e:
            print(f"Ошибка при обработке задачи {task_id}: {str(e)}")

    # Выводим текущую нагрузку и обнаруженные дубликаты
    print("Текущее распределение нагрузки:")
    for position, employees in position_employees.items():
        print(f"Должность: {position}")
        for emp_id in employees:
            try:
                employee = employee_manager.get_employee(emp_id)
                print(f"  - {employee['name']}: {employee_workload.get(emp_id, 0)} дней")
            except Exception as e:
                print(f"  - Сотрудник ID {emp_id}: {employee_workload.get(emp_id, 0)} дней - Ошибка: {str(e)}")

    # Проверяем дубликаты назначений с учетом должностей
    print("Проверка дублирования назначений подзадач:")
    duplicate_assignments = []
    for subtask_key, assigned_employees in similar_subtask_assignments.items():
        if len(set(assigned_employees)) < len(assigned_employees):
            parent_id, task_name, position, timeframe = subtask_key
            print(
                f"  Найдено дублирование назначений для подзадачи '{task_name}' с должностью '{position}' в группе {parent_id}, период {timeframe}")
            print(f"  Назначенные сотрудники: {assigned_employees}")
            duplicate_assignments.append(subtask_key)

    # Шаг 4: Сначала исправляем дублирующиеся назначения
    if duplicate_assignments:
        print(f"Исправление {len(duplicate_assignments)} случаев дублирования назначений...")
        for subtask_key in duplicate_assignments:
            parent_id, task_name, position, timeframe = subtask_key
            assigned_employees = similar_subtask_assignments[subtask_key]

            # Находим дубликаты (сотрудники, назначенные более одного раза)
            from collections import Counter
            employee_counts = Counter(assigned_employees)
            duplicate_employees = [emp_id for emp_id, count in employee_counts.items() if count > 1]

            print(f"Коррекция назначений для подзадачи '{task_name}' с должностью '{position}' в группе {parent_id}")
            print(f"  Сотрудники с дублированием: {duplicate_employees}")

            # Получаем подзадачи этой группы с этим именем, должностью и временным периодом
            matching_subtasks = []
            for task_id, dates in task_dates.items():
                if task_id not in task_map:
                    continue

                task = task_map[task_id]
                task_position = task.get('position', '')  # Добавляем проверку должности

                if (task.get('parent_id') == parent_id or str(task.get('parent_id')) == parent_id) and \
                        task.get('name') == task_name and \
                        task_position == position and \
                        dates.get('start') and dates.get('end') and \
                        f"{dates.get('start')}-{dates.get('end')}" == timeframe:
                    matching_subtasks.append((task_id, task, dates))

            print(f"  Найдено {len(matching_subtasks)} подзадач с подходящими параметрами")

            # Для каждого дублирующего сотрудника, оставляем только одно назначение
            for emp_id in duplicate_employees:
                # Подсчитываем, сколько раз этот сотрудник назначен
                assignments_count = employee_counts[emp_id]
                if assignments_count <= 1:
                    continue

                # Находим все подзадачи, назначенные на этого сотрудника
                emp_assigned_subtasks = [
                    (task_id, task, dates) for task_id, task, dates in matching_subtasks
                    if dates.get('employee_id') == emp_id
                ]

                print(f"  Сотрудник {emp_id} назначен на {len(emp_assigned_subtasks)} подзадач")

                # Оставляем только первое назначение, для остальных ищем других сотрудников
                for idx, (task_id, task, dates) in enumerate(emp_assigned_subtasks[1:], 1):
                    task_position = task.get('position')
                    if not task_position:
                        continue

                    print(f"  Переназначение подзадачи {task_id} с сотрудника {emp_id}")

                    # Ищем альтернативного сотрудника с той же должностью
                    try:
                        available_employees = employee_manager.get_employees_by_position(task_position)
                        if not available_employees:
                            continue

                        # Исключаем сотрудников, которые уже назначены на подзадачи этого типа
                        already_assigned = similar_subtask_assignments[subtask_key]
                        alternative_employees = [e for e in available_employees if
                                                 e['id'] not in already_assigned or e['id'] == emp_id]

                        if not alternative_employees:
                            # Если нет альтернатив, выбираем наименее загруженного из всех
                            sorted_by_workload = sorted(available_employees,
                                                        key=lambda e: employee_workload.get(e['id'], 0))
                            new_employee = sorted_by_workload[0] if sorted_by_workload else None
                        else:
                            # Выбираем наименее загруженного из альтернативных
                            sorted_by_workload = sorted(alternative_employees,
                                                        key=lambda e: employee_workload.get(e['id'], 0))
                            new_employee = sorted_by_workload[0] if sorted_by_workload else None

                        if new_employee:
                            new_emp_id = new_employee['id']

                            # Проверяем доступность нового сотрудника
                            from utils.employee_availability import get_available_dates_for_task
                            task_duration = task.get('duration', 1)
                            avail_start, avail_end, _ = get_available_dates_for_task(
                                new_emp_id, dates.get('start'), task_duration, employee_manager
                            )

                            if avail_start and avail_end:
                                # Обновляем назначение
                                old_emp_name = None
                                new_emp_name = None
                                try:
                                    old_emp = employee_manager.get_employee(emp_id)
                                    old_emp_name = old_emp['name']
                                except:
                                    old_emp_name = f"ID: {emp_id}"

                                try:
                                    new_emp = new_employee
                                    new_emp_name = new_emp['name']
                                except:
                                    new_emp_name = f"ID: {new_emp_id}"

                                print(f"  Переназначаем подзадачу {task_id} с {old_emp_name} на {new_emp_name}")

                                # Обновляем workload
                                employee_workload[emp_id] -= task_duration
                                employee_workload[new_emp_id] = employee_workload.get(new_emp_id, 0) + task_duration

                                # Обновляем task_dates
                                task_dates[task_id] = {
                                    'start': avail_start,
                                    'end': avail_end,
                                    'employee_id': new_emp_id
                                }

                                # Обновляем список назначений
                                similar_subtask_assignments[subtask_key].remove(emp_id)
                                similar_subtask_assignments[subtask_key].append(new_emp_id)

                                # Обновляем employee_tasks
                                employee_tasks[emp_id] = [t for t in employee_tasks[emp_id] if t[0] != task_id]
                                if new_emp_id not in employee_tasks:
                                    employee_tasks[new_emp_id] = []
                                employee_tasks[new_emp_id].append((task_id, task, task_duration))

                                print(f"  Успешно переназначена подзадача {task_id}")
                            else:
                                print(
                                    f"  Не удалось переназначить: сотрудник {new_emp_id} недоступен в период {dates.get('start')} - {dates.get('end')}")
                        else:
                            print(f"  Не найден подходящий сотрудник для переназначения")
                    except Exception as e:
                        print(f"  Ошибка при переназначении подзадачи {task_id}: {str(e)}")
                        import traceback
                        print(traceback.format_exc())

    # Шаг 5: Выявляем дисбаланс в нагрузке - сниженный порог до 1 дня
    position_imbalances = []  # список кортежей (position, imbalance)

    for position, employees in position_employees.items():
        if len(employees) <= 1:
            continue  # Нет смысла балансировать, если только один сотрудник

        # Находим минимальную и максимальную нагрузку
        workloads = [employee_workload.get(emp_id, 0) for emp_id in employees]
        min_load = min(workloads)
        max_load = max(workloads)

        # Вычисляем дисбаланс
        imbalance = max_load - min_load

        # Используем порог в 1 день для более агрессивной балансировки
        if imbalance >= 1:
            position_imbalances.append((position, imbalance))
            print(f"Выявлен дисбаланс для должности {position}: {imbalance} дней")

    # Сортируем позиции по степени дисбаланса - вначале обрабатываем наибольшие дисбалансы
    position_imbalances.sort(key=lambda x: x[1], reverse=True)

    # Шаг 6: Перераспределяем задачи для балансировки нагрузки с учетом дублирования
    balancing_changes = {}  # task_id -> new_employee_id

    for position, imbalance in position_imbalances:
        employees = position_employees[position]

        # Получаем сотрудников с максимальной и минимальной нагрузкой
        sorted_employees = sorted(employees, key=lambda emp_id: employee_workload.get(emp_id, 0))

        # Работаем со всеми парами сотрудников, начиная с наибольшего дисбаланса
        for i in range(len(sorted_employees) - 1):
            least_loaded_emp = sorted_employees[i]
            least_loaded = employee_workload.get(least_loaded_emp, 0)

            # Перебираем более загруженных сотрудников
            for j in range(len(sorted_employees) - 1, i, -1):
                most_loaded_emp = sorted_employees[j]
                most_loaded = employee_workload.get(most_loaded_emp, 0)

                current_imbalance = most_loaded - least_loaded
                # Уменьшаем порог до 1 дня
                if current_imbalance < 1:
                    continue  # Минимальный дисбаланс допустим

                print(
                    f"Перераспределение задач от {most_loaded_emp} (нагрузка: {most_loaded}) к {least_loaded_emp} (нагрузка: {least_loaded})")

                # Получаем все задачи загруженного сотрудника
                tasks_to_reassign = employee_tasks.get(most_loaded_emp, [])

                # Сортируем задачи от большей к меньшей длительности
                tasks_to_reassign.sort(key=lambda x: x[2], reverse=True)

                # Находим задачи, подходящие для перераспределения
                tasks_for_balance = []
                for task_info in tasks_to_reassign:
                    task_id, task, duration = task_info

                    # Проверяем, что эта должность подходит для задачи
                    if task.get('position') and task.get('position') != position:
                        continue  # Пропускаем задачи, требующие другую должность

                    # Проверяем, не приведет ли переназначение к дублированию
                    # для идентичных подзадач в одной группе с учетом должности
                    if task.get('parent_id'):
                        parent_id = task.get('parent_id')
                        task_name = task.get('name', '')
                        task_position = task.get('position', '')  # Учитываем должность

                        # Получаем даты задачи
                        if task_id in task_dates:
                            dates = task_dates[task_id]
                            timeframe = f"{dates.get('start')}-{dates.get('end')}"

                            # Ключ с учетом должности
                            subtask_key = (str(parent_id), task_name, task_position, timeframe)

                            # Если сотрудник уже назначен на подзадачу такого типа, пропускаем
                            if subtask_key in similar_subtask_assignments and \
                                    least_loaded_emp in similar_subtask_assignments[subtask_key]:
                                print(
                                    f"  Пропускаем подзадачу {task_id}: наименее загруженный сотрудник {least_loaded_emp} уже назначен на аналогичную подзадачу")
                                continue

                    # Добавляем в список потенциальных задач все подходящие по должности
                    tasks_for_balance.append(task_info)

                # Если нет подходящих задач, пропускаем эту пару сотрудников
                if not tasks_for_balance:
                    print(f"  У сотрудника {most_loaded_emp} нет подходящих задач для перераспределения")
                    continue

                # Перераспределяем задачи пока не достигнем баланса
                reassigned_count = 0
                for task_id, task, duration in tasks_for_balance:
                    # Проверяем текущий дисбаланс
                    current_imbalance = employee_workload.get(most_loaded_emp, 0) - employee_workload.get(
                        least_loaded_emp, 0)

                    # Если дисбаланс меньше длительности задачи, это может сделать ситуацию хуже
                    if current_imbalance < duration:
                        # Только если это последняя попытка и перераспределений еще не было,
                        # можно перебросить маленькую задачу
                        if reassigned_count == 0 and len(tasks_for_balance) == 1:
                            pass  # Продолжаем и перераспределяем единственную задачу
                        else:
                            continue  # Иначе пропускаем задачу

                    # Проверяем доступность менее загруженного сотрудника
                    task_start = None
                    if task_id in task_dates and 'start' in task_dates[task_id]:
                        task_start = task_dates[task_id]['start']
                    elif task.get('start_date'):
                        task_start = task.get('start_date')
                    else:
                        continue  # Нет даты начала, пропускаем

                    try:
                        # Получаем имена сотрудников для логирования
                        most_loaded_name = "Неизвестный"
                        least_loaded_name = "Неизвестный"
                        try:
                            most_loaded_emp_obj = employee_manager.get_employee(most_loaded_emp)
                            most_loaded_name = most_loaded_emp_obj.get('name', str(most_loaded_emp))
                        except:
                            pass
                        try:
                            least_loaded_emp_obj = employee_manager.get_employee(least_loaded_emp)
                            least_loaded_name = least_loaded_emp_obj.get('name', str(least_loaded_emp))
                        except:
                            pass

                        # Проверяем доступность наименее загруженного сотрудника для задачи
                        from utils.employee_availability import get_available_dates_for_task
                        emp_start, emp_end, _ = get_available_dates_for_task(
                            least_loaded_emp, task_start, duration, employee_manager
                        )

                        if not emp_start or not emp_end:
                            print(
                                f"  Сотрудник {least_loaded_name} недоступен для задачи {task_id} ({task.get('name', 'Без имени')}) в даты {task_start}")
                            continue

                        # Проверяем, не создаст ли переназначение конфликт для подзадач с учетом должности
                        if task.get('parent_id'):
                            parent_id = task.get('parent_id')
                            task_name = task.get('name', '')
                            task_position = task.get('position', '')  # Учитываем должность

                            # Получаем новый timeframe
                            timeframe = f"{emp_start}-{emp_end}"
                            subtask_key = (str(parent_id), task_name, task_position, timeframe)

                            # Проверяем, не назначен ли сотрудник уже на аналогичную подзадачу
                            if subtask_key in similar_subtask_assignments and \
                                    least_loaded_emp in similar_subtask_assignments[subtask_key]:
                                print(
                                    f"  Пропускаем: переназначение создаст конфликт для сотрудника {least_loaded_name}")
                                continue

                            # Обновляем отслеживание подзадач
                            old_dates = task_dates[task_id]
                            old_timeframe = f"{old_dates.get('start')}-{old_dates.get('end')}"
                            old_key = (str(parent_id), task_name, task_position, old_timeframe)

                            if old_key in similar_subtask_assignments:
                                if most_loaded_emp in similar_subtask_assignments[old_key]:
                                    similar_subtask_assignments[old_key].remove(most_loaded_emp)

                            if subtask_key not in similar_subtask_assignments:
                                similar_subtask_assignments[subtask_key] = []
                            similar_subtask_assignments[subtask_key].append(least_loaded_emp)

                        # Перераспределяем задачу
                        print(
                            f"  Перераспределяем задачу {task_id} ({task.get('name', 'Без имени')}) от {most_loaded_name} к {least_loaded_name}")

                        # Сохраняем информацию о перераспределении
                        task_dates[task_id]['employee_id'] = least_loaded_emp
                        task_dates[task_id]['start'] = emp_start
                        task_dates[task_id]['end'] = emp_end

                        # Обновляем нагрузку
                        employee_workload[most_loaded_emp] -= duration
                        employee_workload[least_loaded_emp] += duration

                        # Обновляем списки задач
                        employee_tasks[most_loaded_emp].remove((task_id, task, duration))
                        employee_tasks[least_loaded_emp].append((task_id, task, duration))

                        balancing_changes[task_id] = least_loaded_emp
                        reassigned_count += 1

                        # Если достигли баланса, переходим к следующей паре
                        if employee_workload[most_loaded_emp] - employee_workload[least_loaded_emp] < 1:
                            break

                    except Exception as e:
                        print(f"  Ошибка при перераспределении задачи {task_id}: {str(e)}")
                        import traceback
                        print(traceback.format_exc())

                # Выводим результат для пары сотрудников
                if reassigned_count > 0:
                    print(f"  Перераспределено {reassigned_count} задач между сотрудниками")
                    try:
                        most_loaded_name = employee_manager.get_employee(most_loaded_emp)['name']
                    except:
                        most_loaded_name = f"Сотрудник {most_loaded_emp}"
                    try:
                        least_loaded_name = employee_manager.get_employee(least_loaded_emp)['name']
                    except:
                        least_loaded_name = f"Сотрудник {least_loaded_emp}"
                    print(f"  Новая нагрузка {most_loaded_name}: {employee_workload[most_loaded_emp]} дней")
                    print(f"  Новая нагрузка {least_loaded_name}: {employee_workload[least_loaded_emp]} дней")

    # Выводим итоговые результаты балансировки
    print("\nИтоговые результаты балансировки нагрузки:")
    for position, employees in position_employees.items():
        print(f"Должность: {position}")
        # Сортируем сотрудников по нагрузке для лучшей читаемости
        sorted_emp = sorted(employees, key=lambda emp_id: employee_workload.get(emp_id, 0), reverse=True)
        for emp_id in sorted_emp:
            try:
                employee = employee_manager.get_employee(emp_id)
                print(f"  - {employee['name']}: {employee_workload.get(emp_id, 0)} дней")
            except:
                print(f"  - Сотрудник ID {emp_id}: {employee_workload.get(emp_id, 0)} дней")

    print(f"Всего изменено назначений: {len(balancing_changes)}")

    # Проверяем наличие дублирующихся назначений после балансировки
    duplicate_count = 0
    for subtask_key, assigned_employees in similar_subtask_assignments.items():
        if len(set(assigned_employees)) < len(assigned_employees):
            parent_id, task_name, position, timeframe = subtask_key
            print(f"ВНИМАНИЕ: Остались дублирующиеся назначения для подзадачи '{task_name}' с должностью '{position}' в группе {parent_id}")
            print(f"  Назначенные сотрудники: {assigned_employees}")
            duplicate_count += 1

    if duplicate_count == 0:
        print("Проверка успешна: дублирующихся назначений не найдено!")
    else:
        print(f"ВНИМАНИЕ: Найдено {duplicate_count} случаев дублирования назначений!")

    # Возвращаем обновленный task_dates
    return task_dates


def build_dependency_graph(tasks, task_manager):
    """
    Строит граф зависимостей между задачами

    Args:
        tasks (list): Список задач
        task_manager: Менеджер задач

    Returns:
        tuple: (graph, task_map) - граф зависимостей и словарь задач по ID
    """
    import json

    # Инициализируем граф и словарь задач
    graph = {}  # task_id -> список ID предшественников
    task_map = {}  # task_id -> task

    # Создаем словарь задач по ID для быстрого доступа
    for task in tasks:
        task_id = task['id']
        # Преобразуем ID к строковому типу для единообразия
        task_id_str = str(task_id)
        graph[task_id_str] = []
        task_map[task_id_str] = task

        # Также добавим отображение числового ID на строковый, если это разные значения
        if task_id != task_id_str:
            task_map[task_id] = task

    # Создаем словарь задач по имени для поиска предшественников, указанных по имени
    tasks_by_name = {}
    for task in tasks:
        task_name = task.get('name')
        if task_name:
            tasks_by_name[task_name] = str(task['id'])

    # Заполняем граф зависимостями
    for task in tasks:
        task_id = str(task['id'])
        task_name = task.get('name', f"Задача {task_id}")

        # Получаем зависимости из поля predecessors
        predecessors = []
        if 'predecessors' in task and task['predecessors']:
            # Обрабатываем разные форматы предшественников
            if isinstance(task['predecessors'], list):
                for pred in task['predecessors']:
                    if isinstance(pred, (int, str)):
                        predecessors.append(str(pred))
                    elif isinstance(pred, dict) and 'id' in pred:
                        predecessors.append(str(pred['id']))
                    elif isinstance(pred, str) and pred in tasks_by_name:
                        # Если это название задачи, ищем её ID
                        predecessors.append(tasks_by_name[pred])
            elif isinstance(task['predecessors'], str):
                try:
                    # Пробуем распарсить как JSON
                    pred_list = json.loads(task['predecessors'])
                    if isinstance(pred_list, list):
                        for pred in pred_list:
                            if isinstance(pred, (int, str)):
                                predecessors.append(str(pred))
                except json.JSONDecodeError:
                    # Если не JSON, пробуем разделить по запятым
                    if ',' in task['predecessors']:
                        for pred in task['predecessors'].split(','):
                            pred = pred.strip()
                            # Проверяем, это ID или имя задачи
                            if pred.isdigit():
                                predecessors.append(pred)
                            elif pred in tasks_by_name:
                                predecessors.append(tasks_by_name[pred])
                    else:
                        # Одиночное значение
                        pred = task['predecessors'].strip()
                        if pred.isdigit():
                            predecessors.append(pred)
                        elif pred in tasks_by_name:
                            predecessors.append(tasks_by_name[pred])

        # Получаем зависимости из базы данных
        try:
            db_deps = task_manager.get_task_dependencies(task['id'])
            for dep in db_deps:
                predecessor_id = str(dep['predecessor_id'])
                if predecessor_id not in predecessors:
                    predecessors.append(predecessor_id)
        except Exception as e:
            print(f"Ошибка при получении зависимостей из БД для задачи {task_id}: {e}")

        # Добавляем зависимости в граф
        for pred_id in predecessors:
            pred_id_str = str(pred_id)
            if pred_id_str not in graph:
                # Если предшественник не найден в исходном списке задач, добавляем его в граф
                graph[pred_id_str] = []
                # И пытаемся найти задачу с таким ID
                pred_task = None
                for t in tasks:
                    if str(t['id']) == pred_id_str:
                        pred_task = t
                        break

                if pred_task:
                    task_map[pred_id_str] = pred_task
                else:
                    print(f"ПРЕДУПРЕЖДЕНИЕ: Предшественник {pred_id_str} задачи {task_id} не найден в списке задач!")

            graph[task_id].append(pred_id_str)

    # Выводим отладочную информацию
    for task_id, predecessors in graph.items():
        if predecessors:
            task_name = task_map[task_id]['name'] if task_id in task_map else f"Неизвестная задача {task_id}"
            pred_names = []
            for pred_id in predecessors:
                if pred_id in task_map:
                    pred_name = task_map[pred_id].get('name', f"Задача {pred_id}")
                    pred_names.append(f"{pred_id}:{pred_name}")
                else:
                    pred_names.append(f"{pred_id}:???")

            print(f"Задача {task_id}: {task_name} - зависит от: {', '.join(pred_names)}")

    return graph, task_map


def topological_sort(graph):
    # Получаем все узлы графа
    all_nodes = set(graph.keys())

    # Обратный граф (предшественник -> список зависимых задач)
    reverse_graph = {}
    for node, predecessors in graph.items():
        for pred in predecessors:
            if pred not in reverse_graph:
                reverse_graph[pred] = []
            reverse_graph[pred].append(node)

            # Добавляем предшественников, даже если они не в исходном списке задач
            if pred not in all_nodes:
                all_nodes.add(pred)
                graph[pred] = []

    # Подсчитываем число предшественников для каждой задачи
    in_degree = {node: len(graph[node]) for node in all_nodes}

    # Создаем список задач без предшественников
    queue = [node for node in all_nodes if in_degree[node] == 0]
    result = []

    # Проверка для цикличного графа
    visited = set()

    # Выполняем топологическую сортировку
    while queue:
        # Берем задачу без предшественников
        current = queue.pop(0)

        # Добавляем в результат
        result.append(current)
        visited.add(current)

        # Обновляем зависимости для задач, зависящих от текущей
        if current in reverse_graph:
            for dependent in reverse_graph[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

    # Проверяем, все ли задачи обработаны
    if len(result) != len(all_nodes):
        print("ПРЕДУПРЕЖДЕНИЕ: В графе обнаружены циклы или недостижимые узлы!")
        print(f"Обработано {len(result)} из {len(all_nodes)} задач.")

        # Добавляем непосещенные узлы в конец
        for node in all_nodes:
            if node not in visited:
                result.append(node)
                print(f"Задача {node} добавлена в конец из-за нарушения зависимостей.")

    # Выводим результат сортировки для отладки
    print(f"Порядок обработки задач (топологическая сортировка): {result}")

    return result


def calculate_project_duration(project_start_date, task_dates):
    """
    Рассчитывает общую длительность проекта в днях

    Args:
        project_start_date (str): Дата начала проекта
        task_dates (dict): Словарь с датами задач

    Returns:
        int: Длительность проекта в днях
    """
    import datetime

    if not task_dates:
        return 0

    try:
        # Находим самую раннюю дату начала и самую позднюю дату окончания среди задач
        earliest_start = None
        latest_end = None

        for task_id, dates in task_dates.items():
            if 'start' in dates:
                start = datetime.datetime.strptime(dates['start'], '%Y-%m-%d')
                if earliest_start is None or start < earliest_start:
                    earliest_start = start

            if 'end' in dates:
                end = datetime.datetime.strptime(dates['end'], '%Y-%m-%d')
                if latest_end is None or end > latest_end:
                    latest_end = end

        # Если не нашли дат в задачах, используем дату начала проекта
        if earliest_start is None:
            earliest_start = datetime.datetime.strptime(project_start_date, '%Y-%m-%d')

        if latest_end is None:
            latest_end = earliest_start  # Минимальная длительность 0 дней

        # Вычисляем длительность в днях
        duration = (latest_end - earliest_start).days + 1  # +1 так как включаем день окончания

        print(f"Рассчитана длительность проекта: {duration} дней")
        print(f"От {earliest_start.strftime('%Y-%m-%d')} до {latest_end.strftime('%Y-%m-%d')}")

        return duration
    except Exception as e:
        print(f"Ошибка при расчете длительности проекта: {str(e)}")
        return 0


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
    import datetime
    from collections import defaultdict

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

    # Ищем пути от начальной задачи к конечной
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


def update_database_assignments(task_dates, task_manager, employee_manager=None):
    """Обновляет назначения сотрудников и даты в базе данных"""
    print(f"Обновление базы данных для {len(task_dates)} задач...")
    updated_count = 0

    # Для отладки - сколько задач обновилось
    date_updates = 0
    employee_updates = 0

    for task_id, task_data in task_dates.items():
        # Преобразуем task_id в числовой формат если это строка
        try:
            # Явно пытаемся преобразовать ID в числовой формат
            numeric_task_id = int(task_id) if isinstance(task_id, str) else task_id
        except (ValueError, TypeError):
            print(f"Ошибка конвертации ID задачи {task_id}, пропускаем обновление")
            continue

        # Получаем данные
        start_date = task_data.get('start')
        end_date = task_data.get('end')
        employee_id = task_data.get('employee_id')

        # 1. Обновляем даты в БД принудительно, даже если они кажутся неизменными
        if start_date and end_date:
            try:
                # Напрямую обновляем, без предварительной проверки
                task_manager.db.execute(
                    "UPDATE tasks SET start_date = ?, end_date = ? WHERE id = ?",
                    (start_date, end_date, numeric_task_id)
                )
                date_updates += 1
                updated_count += 1
                print(f"Обновлены даты для задачи {numeric_task_id}: {start_date} - {end_date}")
            except Exception as e:
                print(f"Ошибка при обновлении дат задачи {numeric_task_id}: {str(e)}")

        # 2. Обновляем назначение сотрудника, если указано
        if employee_id is not None:
            try:
                task_manager.db.execute(
                    "UPDATE tasks SET employee_id = ? WHERE id = ?",
                    (employee_id, numeric_task_id)
                )
                employee_updates += 1
                updated_count += 1

                # Логируем назначение сотрудника
                if employee_manager:
                    try:
                        employee = employee_manager.get_employee(employee_id)
                        print(f"Сотрудник {employee['name']} назначен на задачу {numeric_task_id}")
                    except Exception:
                        print(f"Сотрудник ID:{employee_id} назначен на задачу {numeric_task_id}")
            except Exception as e:
                print(f"Ошибка при обновлении сотрудника для задачи {numeric_task_id}: {str(e)}")

    # Печатаем итоговую статистику
    print(f"Всего обновлено {updated_count} записей")
    print(f"Обновлений дат: {date_updates}, обновлений сотрудников: {employee_updates}")

    return updated_count