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
    Главная функция для планирования проекта с исправленной обработкой

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

    # Шаг 2: Выполняем топологическую сортировку
    sorted_tasks = topological_sort(graph)
    print(f"Задачи отсортированы в порядке зависимостей, всего {len(sorted_tasks)} задач")

    # Шаг 3: Инициализируем систему отслеживания нагрузки сотрудников
    employee_workload = {}  # employee_id -> общее количество дней
    employee_schedule = {}  # employee_id -> {date: task_count}

    # Шаг 4: Рассчитываем даты задач с учетом зависимостей и балансировки нагрузки
    task_dates = calculate_tasks_with_dependencies(
        project, sorted_tasks, graph, task_map, task_manager, employee_manager,
        employee_workload, employee_schedule
    )

    # Шаг 5: Проверяем корректность назначения параллельных подзадач
    parallel_issues = validate_parallel_assignments(task_dates, task_map)
    if parallel_issues:
        print(f"❌ Найдено {len(parallel_issues)} проблем с параллельными подзадачами:")
        for issue in parallel_issues:
            print(f"  • {issue}")
    else:
        print("✅ Параллельные подзадачи назначены корректно")

    # Шаг 6: Балансировка нагрузки между сотрудниками
    task_dates = balance_workload_final(task_dates, task_map, employee_manager, employee_workload)

    # Шаг 7: Определяем критический путь
    critical_path = identify_critical_path(task_dates, graph, task_map)
    print(f"Критический путь содержит {len(critical_path)} задач")

    # Шаг 8: Рассчитываем длительность проекта
    project_duration = calculate_project_duration(project['start_date'], task_dates)
    print(f"Длительность проекта: {project_duration} дней")

    # Выводим статистику нагрузки
    print_workload_statistics(employee_workload, employee_manager)

    return {
        'task_dates': task_dates,
        'critical_path': critical_path,
        'duration': project_duration
    }


def balance_workload_final(task_dates, task_map, employee_manager, employee_workload):
    """
    Финальная балансировка нагрузки между сотрудниками
    """
    print("Выполняется балансировка нагрузки...")

    # Группируем сотрудников по должностям
    position_employees = {}
    for emp_id, workload in employee_workload.items():
        try:
            employee = employee_manager.get_employee(emp_id)
            position = employee.get('position', 'Неизвестно')

            if position not in position_employees:
                position_employees[position] = []
            position_employees[position].append(emp_id)
        except:
            continue

    # Для каждой должности балансируем нагрузку
    for position, emp_ids in position_employees.items():
        if len(emp_ids) < 2:
            continue  # Нечего балансировать

        # Вычисляем среднюю нагрузку
        total_workload = sum(employee_workload.get(emp_id, 0) for emp_id in emp_ids)
        avg_workload = total_workload / len(emp_ids)

        print(f"Должность {position}: средняя нагрузка {avg_workload:.1f} дней")

        # Находим перегруженных и недогруженных
        overloaded = [(emp_id, employee_workload.get(emp_id, 0)) for emp_id in emp_ids
                      if employee_workload.get(emp_id, 0) > avg_workload + 3]
        underloaded = [(emp_id, employee_workload.get(emp_id, 0)) for emp_id in emp_ids
                       if employee_workload.get(emp_id, 0) < avg_workload - 3]

        if overloaded and underloaded:
            print(
                f"Балансировка для должности {position}: {len(overloaded)} перегружены, {len(underloaded)} недогружены")
            # Здесь можно добавить логику перераспределения задач

    return task_dates

def calculate_tasks_with_dependencies(project, sorted_tasks, graph, task_map, task_manager,
                                      employee_manager, employee_workload, employee_schedule):
    """
    Рассчитывает даты задач с учетом зависимостей и равномерного распределения нагрузки
    """
    task_dates = {}

    for task_id in sorted_tasks:
        if task_id not in task_map:
            continue

        task = task_map[task_id]
        task_name = task.get('name', f"Задача {task_id}")

        # Пропускаем подзадачи на первом проходе
        if task.get('parent_id'):
            continue

        # Определяем дату начала на основе предшественников
        start_date = calculate_task_start_date(task_id, graph, task_dates, project['start_date'])

        # Рассчитываем задачу
        if task.get('is_group'):
            # Для групповой задачи сначала устанавливаем предварительные даты
            task_duration = task.get('duration', 1)
            end_date = start_date + datetime.timedelta(days=task_duration - 1)
            task_dates[task_id] = {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            }
            print(f"Групповая задача {task_id}: {task_name} - предварительные даты")

            # Обрабатываем подзадачи
            process_group_subtasks(task_id, task, start_date, task_dates, task_map,
                                   task_manager, employee_manager, employee_workload, employee_schedule)
        else:
            # Обычная задача
            assign_regular_task(task_id, task, start_date, task_dates, employee_manager,
                                employee_workload, employee_schedule)

    return task_dates


def validate_parallel_assignments(task_dates, task_map):
    """
    ИСПРАВЛЕННАЯ версия: Проверяет корректность назначения параллельных подзадач

    Returns:
        list: Список найденных проблем
    """
    issues = []

    print("Проверка корректности назначения параллельных подзадач...")

    # Группируем подзадачи по родительской задаче
    parent_subtasks = {}
    for task_id, task in task_map.items():
        parent_id = task.get('parent_id')
        if parent_id:
            parent_id_str = str(parent_id)
            if parent_id_str not in parent_subtasks:
                parent_subtasks[parent_id_str] = []
            parent_subtasks[parent_id_str].append(task)

    print(f"Найдено {len(parent_subtasks)} групповых задач с подзадачами")

    # Проверяем каждую группу подзадач
    for parent_id, subtasks in parent_subtasks.items():
        # Находим только ПАРАЛЛЕЛЬНЫЕ подзадачи
        parallel_subtasks = [subtask for subtask in subtasks if subtask.get('parallel')]

        if not parallel_subtasks:
            continue  # Нет параллельных подзадач в этой группе

        print(f"Группа {parent_id}: найдено {len(parallel_subtasks)} параллельных подзадач из {len(subtasks)} общих")

        # Группируем параллельные подзадачи по имени и должности
        parallel_groups = {}  # (name, position) -> [subtasks]

        for subtask in parallel_subtasks:
            name = subtask.get('name', '')
            position = subtask.get('position', '')
            group_key = (name, position)

            if group_key not in parallel_groups:
                parallel_groups[group_key] = []
            parallel_groups[group_key].append(subtask)

        # Проверяем каждую группу параллельных подзадач с одинаковым именем
        for (subtask_name, position), subtask_group in parallel_groups.items():
            if len(subtask_group) <= 1:
                continue  # Только одна подзадача с таким именем - проблем нет

            print(
                f"Проверка группы параллельных подзадач: '{subtask_name}' ({position}) - {len(subtask_group)} подзадач")

            # Проверяем назначения в этой группе
            assigned_employees = []
            task_details = []  # Для подробной диагностики

            for subtask in subtask_group:
                subtask_id = subtask['id']

                # Ищем назначение в task_dates
                if str(subtask_id) in task_dates:
                    dates_info = task_dates[str(subtask_id)]
                elif subtask_id in task_dates:
                    dates_info = task_dates[subtask_id]
                else:
                    print(f"  ⚠️ Подзадача {subtask_id} не найдена в task_dates")
                    continue

                employee_id = dates_info.get('employee_id')
                start_date = dates_info.get('start')

                if employee_id:
                    assigned_employees.append(employee_id)
                    task_details.append({
                        'subtask_id': subtask_id,
                        'employee_id': employee_id,
                        'start_date': start_date,
                        'subtask_name': subtask_name
                    })

            # Анализируем назначения
            if not assigned_employees:
                continue  # Никто не назначен

            unique_employees = len(set(assigned_employees))
            total_subtasks = len(assigned_employees)

            # Выводим детали для диагностики
            print(f"  Детали назначений для '{subtask_name}':")
            for detail in task_details:
                print(
                    f"    Подзадача {detail['subtask_id']}: сотрудник {detail['employee_id']}, дата {detail['start_date']}")

            # ОСНОВНАЯ ПРОВЕРКА: есть ли реальная проблема?
            if unique_employees < total_subtasks:
                # Есть дублирование - ищем конкретные случаи
                from collections import Counter
                employee_counts = Counter(assigned_employees)
                duplicated_employees = [emp_id for emp_id, count in employee_counts.items() if count > 1]

                if duplicated_employees:
                    # Проверяем, выполняются ли дублированные задачи в одно время
                    for dup_emp_id in duplicated_employees:
                        dup_tasks = [detail for detail in task_details if detail['employee_id'] == dup_emp_id]

                        # Группируем по датам
                        dates_tasks = {}
                        for task_detail in dup_tasks:
                            start_date = task_detail['start_date']
                            if start_date not in dates_tasks:
                                dates_tasks[start_date] = []
                            dates_tasks[start_date].append(task_detail)

                        # Проверяем, есть ли задачи на одну дату
                        for date, tasks_on_date in dates_tasks.items():
                            if len(tasks_on_date) > 1:
                                # РЕАЛЬНАЯ ПРОБЛЕМА: одному сотруднику назначено несколько параллельных подзадач на одну дату
                                issues.append(
                                    f"КРИТИЧЕСКАЯ ОШИБКА: Сотруднику {dup_emp_id} назначено {len(tasks_on_date)} "
                                    f"параллельных подзадач '{subtask_name}' на дату {date} в группе {parent_id}. "
                                    f"Это невозможно выполнить!"
                                )
                                print(
                                    f"  ❌ НАЙДЕНА РЕАЛЬНАЯ ПРОБЛЕМА: сотрудник {dup_emp_id} имеет {len(tasks_on_date)} задач на {date}")
                            else:
                                # Задачи назначены одному сотруднику, но в разные дни - это может быть нормально
                                print(
                                    f"  ✓ Сотрудник {dup_emp_id} имеет несколько подзадач '{subtask_name}', но в разные дни - ОК")
                else:
                    print(f"  ✓ Группа '{subtask_name}' в {parent_id}: назначения корректны")
            else:
                print(
                    f"  ✓ Группа '{subtask_name}' в {parent_id}: все {total_subtasks} подзадач назначены {unique_employees} разным сотрудникам")

    if not issues:
        print("✅ Проверка параллельных подзадач завершена - критических проблем не найдено!")
    else:
        print(f"❌ Найдено {len(issues)} реальных проблем с параллельными подзадачами")

    return issues

def process_group_subtasks(group_id, group_task, group_start, task_dates, task_map,
                           task_manager, employee_manager, employee_workload, employee_schedule):
    """
    Обрабатывает подзадачи групповой задачи
    """
    # Получаем все подзадачи
    subtasks = get_all_subtasks_for_group(group_id, task_map, task_manager)

    if not subtasks:
        return

    print(f"Обработка {len(subtasks)} подзадач для групповой задачи {group_id}")

    # Разделяем на параллельные и последовательные
    parallel_subtasks = [task for task in subtasks if task.get('parallel')]
    sequential_subtasks = [task for task in subtasks if not task.get('parallel')]

    # Обрабатываем параллельные подзадачи
    for subtask in parallel_subtasks:
        assign_subtask(subtask, group_start, task_dates, employee_manager, employee_workload, is_parallel=True)

    # Обрабатываем последовательные подзадачи
    current_date = group_start
    for subtask in sequential_subtasks:
        new_end_date = assign_subtask(subtask, current_date, task_dates, employee_manager, employee_workload,
                                      is_parallel=False)
        if new_end_date:
            current_date = new_end_date + datetime.timedelta(days=1)

    # Обновляем даты групповой задачи на основе подзадач
    update_group_task_dates(group_id, subtasks, task_dates)

def assign_parallel_subtask_group(subtask_group, group_start, task_dates, employee_manager,
                                  employee_workload, task_name, position):
    """
    Назначает группу одинаковых параллельных подзадач разным сотрудникам

    Args:
        subtask_group (list): Список подзадач с одинаковым именем
        group_start (datetime): Дата начала группы
        task_dates (dict): Словарь дат задач
        employee_manager: Менеджер сотрудников
        employee_workload (dict): Текущая нагрузка сотрудников
        task_name (str): Имя подзадачи
        position (str): Требуемая должность
    """
    if not position:
        print(f"⚠️ Не указана должность для параллельных подзадач '{task_name}'")
        # Назначаем без учета должности
        for subtask in subtask_group:
            assign_subtask(subtask, group_start, task_dates, employee_manager,
                           employee_workload, is_parallel=True)
        return

    # Получаем всех сотрудников с нужной должностью
    try:
        available_employees = employee_manager.get_employees_by_position(position)

        if not available_employees:
            print(f"⚠️ Не найдены сотрудники с должностью '{position}' для подзадач '{task_name}'")
            # Назначаем без учета должности
            for subtask in subtask_group:
                assign_subtask(subtask, group_start, task_dates, employee_manager,
                               employee_workload, is_parallel=True)
            return

        print(f"Найдено {len(available_employees)} сотрудников с должностью '{position}'")

        # Сортируем сотрудников по текущей нагрузке (наименее загруженные первыми)
        sorted_employees = sorted(
            available_employees,
            key=lambda e: employee_workload.get(e['id'], 0)
        )

        # Проверяем, достаточно ли сотрудников
        if len(available_employees) < len(subtask_group):
            print(f"⚠️ ВНИМАНИЕ: Сотрудников ({len(available_employees)}) меньше чем подзадач ({len(subtask_group)})")
            print(f"Некоторым сотрудникам будет назначено несколько подзадач '{task_name}'")

        # Назначаем каждую подзадачу отдельному сотруднику
        assigned_employees = set()  # Для отслеживания уже назначенных

        for i, subtask in enumerate(subtask_group):
            subtask_id = subtask['id']
            subtask_duration = subtask.get('duration', 1)

            # Выбираем сотрудника
            if i < len(available_employees):
                # Есть свободный сотрудник
                chosen_employee = sorted_employees[i]
            else:
                # Сотрудников меньше чем задач - выбираем наименее загруженного из уже назначенных
                available_for_reuse = [emp for emp in sorted_employees if emp['id'] in assigned_employees]
                if available_for_reuse:
                    chosen_employee = min(available_for_reuse, key=lambda e: employee_workload.get(e['id'], 0))
                else:
                    chosen_employee = sorted_employees[0]  # Fallback

            chosen_employee_id = chosen_employee['id']

            # Рассчитываем даты с учетом выходных дней сотрудника
            start_date_str = group_start.strftime('%Y-%m-%d')
            emp_start, emp_end, _ = get_available_dates_for_task(
                chosen_employee_id, start_date_str, subtask_duration, employee_manager
            )

            if emp_start:
                task_dates[subtask_id] = {
                    'start': emp_start,
                    'end': emp_end,
                    'employee_id': chosen_employee_id
                }

                # Обновляем нагрузку сотрудника
                employee_workload[chosen_employee_id] = employee_workload.get(chosen_employee_id, 0) + subtask_duration
                assigned_employees.add(chosen_employee_id)

                try:
                    employee_name = chosen_employee['name']
                    print(f"  ✓ Подзадача '{task_name}' (ID: {subtask_id}) назначена сотруднику {employee_name}")
                    print(
                        f"    Даты: {emp_start} - {emp_end}, нагрузка сотрудника: {employee_workload[chosen_employee_id]} дней")
                except Exception as e:
                    print(
                        f"  ✓ Подзадача '{task_name}' (ID: {subtask_id}) назначена сотруднику ID: {chosen_employee_id}")
            else:
                # Не удалось рассчитать даты с учетом выходных, используем стандартные
                end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
                task_dates[subtask_id] = {
                    'start': start_date_str,
                    'end': end_date.strftime('%Y-%m-%d'),
                    'employee_id': chosen_employee_id
                }

                employee_workload[chosen_employee_id] = employee_workload.get(chosen_employee_id, 0) + subtask_duration
                assigned_employees.add(chosen_employee_id)

                print(
                    f"  ⚠️ Подзадача '{task_name}' (ID: {subtask_id}) назначена сотруднику {chosen_employee_id} без учета выходных")

        # Проверяем результат назначения
        unique_employees = len(assigned_employees)
        total_subtasks = len(subtask_group)

        if unique_employees == total_subtasks:
            print(f"✅ Все {total_subtasks} подзадач '{task_name}' назначены разным сотрудникам")
        else:
            print(
                f"⚠️ {total_subtasks} подзадач '{task_name}' назначены {unique_employees} сотрудникам (есть дублирование)")

    except Exception as e:
        print(f"❌ Ошибка при назначении параллельных подзадач '{task_name}': {str(e)}")
        # Fallback - назначаем как обычные подзадачи
        for subtask in subtask_group:
            assign_subtask(subtask, group_start, task_dates, employee_manager,
                           employee_workload, is_parallel=True)


def assign_subtask(subtask, start_date, task_dates, employee_manager, employee_workload, is_parallel=True):
    """
    Назначает подзадачу на сотрудника
    """
    subtask_id = subtask['id']
    subtask_duration = subtask.get('duration', 1)
    position = subtask.get('position')
    employee_id = subtask.get('employee_id')

    start_date_str = start_date.strftime('%Y-%m-%d')

    if position:
        # Находим наименее загруженного сотрудника
        suitable_employees = employee_manager.get_employees_by_position(position)

        if suitable_employees:
            # Сортируем по нагрузке
            sorted_employees = sorted(
                suitable_employees,
                key=lambda e: employee_workload.get(e['id'], 0)
            )

            best_employee = sorted_employees[0]
            best_employee_id = best_employee['id']

            # Рассчитываем даты
            emp_start, emp_end, _ = get_available_dates_for_task(
                best_employee_id, start_date_str, subtask_duration, employee_manager
            )

            if emp_start:
                task_dates[subtask_id] = {
                    'start': emp_start,
                    'end': emp_end,
                    'employee_id': best_employee_id
                }
                employee_workload[best_employee_id] = employee_workload.get(best_employee_id, 0) + subtask_duration
                print(
                    f"Подзадача {subtask_id} назначена на {best_employee['name']} (нагрузка: {employee_workload[best_employee_id]})")

                return datetime.datetime.strptime(emp_end, '%Y-%m-%d')

    # Если не удалось назначить, используем стандартные даты
    end_date = start_date + datetime.timedelta(days=subtask_duration - 1)
    task_dates[subtask_id] = {
        'start': start_date_str,
        'end': end_date.strftime('%Y-%m-%d')
    }

    return end_date

def calculate_task_start_date(task_id, graph, task_dates, project_start_date):
    """
    Вычисляет дату начала задачи на основе предшественников
    """
    predecessors = graph.get(task_id, [])

    if not predecessors:
        # Нет предшественников - начинаем с даты начала проекта
        return datetime.datetime.strptime(project_start_date, '%Y-%m-%d')

    # Находим самую позднюю дату окончания среди предшественников
    latest_end_date = None

    for pred_id in predecessors:
        if pred_id in task_dates and 'end' in task_dates[pred_id]:
            pred_end = datetime.datetime.strptime(task_dates[pred_id]['end'], '%Y-%m-%d')
            pred_next_day = pred_end + datetime.timedelta(days=1)

            if latest_end_date is None or pred_next_day > latest_end_date:
                latest_end_date = pred_next_day

    if latest_end_date:
        return latest_end_date
    else:
        # Если не удалось определить даты предшественников, используем дату начала проекта
        return datetime.datetime.strptime(project_start_date, '%Y-%m-%d')


def assign_regular_task(task_id, task, start_date, task_dates, employee_manager,
                        employee_workload, employee_schedule):
    """
    Назначает обычную задачу на сотрудника с балансировкой нагрузки
    """
    task_duration = task.get('duration', 1)
    position = task.get('position')
    employee_id = task.get('employee_id')
    task_name = task.get('name', f"Задача {task_id}")

    if employee_id:
        # Уже назначен сотрудник - проверяем его доступность
        employee_start, employee_end, calendar_duration = get_available_dates_for_task(
            employee_id, start_date.strftime('%Y-%m-%d'), task_duration, employee_manager
        )
        if employee_start:
            task_dates[task_id] = {
                'start': employee_start,
                'end': employee_end,
                'employee_id': employee_id
            }
            # Обновляем нагрузку
            employee_workload[employee_id] = employee_workload.get(employee_id, 0) + task_duration
            print(f"Задача {task_id}: {task_name} - сохранен назначенный сотрудник {employee_id}")
            return

    if position:
        # Ищем наименее загруженного сотрудника с нужной должностью
        suitable_employees = employee_manager.get_employees_by_position(position)

        if suitable_employees:
            # Сортируем по текущей нагрузке
            sorted_employees = sorted(
                suitable_employees,
                key=lambda e: employee_workload.get(e['id'], 0)
            )

            # Выбираем наименее загруженного
            best_employee = sorted_employees[0]
            best_employee_id = best_employee['id']

            # Рассчитываем даты с учетом выходных
            employee_start, employee_end, calendar_duration = get_available_dates_for_task(
                best_employee_id, start_date.strftime('%Y-%m-%d'), task_duration, employee_manager
            )

            if employee_start:
                task_dates[task_id] = {
                    'start': employee_start,
                    'end': employee_end,
                    'employee_id': best_employee_id
                }
                # Обновляем нагрузку
                employee_workload[best_employee_id] = employee_workload.get(best_employee_id, 0) + task_duration
                print(
                    f"Задача {task_id}: {task_name} - назначен сотрудник {best_employee['name']} (нагрузка: {employee_workload[best_employee_id]} дней)")
                return

    # Если не удалось назначить сотрудника, используем стандартные даты
    end_date = start_date + datetime.timedelta(days=task_duration - 1)
    task_dates[task_id] = {
        'start': start_date.strftime('%Y-%m-%d'),
        'end': end_date.strftime('%Y-%m-%d')
    }
    print(f"Задача {task_id}: {task_name} - не удалось назначить сотрудника")

def calculate_main_tasks_dates(project, sorted_tasks, graph, task_map, task_manager, employee_manager):
    """
    Рассчитывает даты только для основных задач (не подзадач)
    """
    task_dates = {}

    for task_id in sorted_tasks:
        if task_id not in task_map:
            continue

        task = task_map[task_id]

        # Пропускаем подзадачи на этом этапе
        if task.get('parent_id'):
            continue

        task_name = task.get('name', f"Задача {task_id}")
        predecessors = graph.get(task_id, [])

        # Определяем дату начала задачи на основе предшественников
        start_date = None
        if not predecessors:
            # Если нет предшественников, начинаем с даты начала проекта
            start_date = datetime.datetime.strptime(project['start_date'], '%Y-%m-%d')
            print(f"Задача {task_id}: {task_name} - начало с даты начала проекта: {start_date.strftime('%Y-%m-%d')}")
        else:
            # Определяем дату начала на основе самой поздней даты окончания предшественников
            latest_end_date = None
            for pred_id in predecessors:
                if pred_id in task_dates and 'end' in task_dates[pred_id]:
                    pred_end = datetime.datetime.strptime(task_dates[pred_id]['end'], '%Y-%m-%d')
                    pred_next_day = pred_end + datetime.timedelta(days=1)
                    if latest_end_date is None or pred_next_day > latest_end_date:
                        latest_end_date = pred_next_day

            if latest_end_date:
                start_date = latest_end_date
            else:
                start_date = datetime.datetime.strptime(project['start_date'], '%Y-%m-%d')

        # Вычисляем дату окончания и сохраняем в task_dates
        task_duration = task.get('duration', 1)

        if task.get('is_group'):
            # Для групповой задачи устанавливаем предварительные даты
            end_date = start_date + datetime.timedelta(days=task_duration - 1)
            task_dates[task_id] = {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            }
            print(
                f"Групповая задача {task_id}: {task_name} - предварительные даты: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
        else:
            # Для обычной задачи назначаем сотрудника и учитываем выходные дни
            employee_id = task.get('employee_id')
            position = task.get('position')

            if employee_id:
                # Проверяем доступность сотрудника
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
            elif position:
                # Ищем подходящего сотрудника
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
                    # Используем стандартные даты
                    end_date = start_date + datetime.timedelta(days=task_duration - 1)
                    task_dates[task_id] = {
                        'start': start_date.strftime('%Y-%m-%d'),
                        'end': end_date.strftime('%Y-%m-%d')
                    }
            else:
                # Ни сотрудник, ни должность не указаны
                end_date = start_date + datetime.timedelta(days=task_duration - 1)
                task_dates[task_id] = {
                    'start': start_date.strftime('%Y-%m-%d'),
                    'end': end_date.strftime('%Y-%m-%d')
                }

    return task_dates

def process_all_subtasks_unified(task_dates, task_map, task_manager, employee_manager):
    """
    Унифицированная обработка всех подзадач с правильной синхронизацией дат
    """
    print("Унифицированная обработка подзадач...")

    # Словарь для отслеживания загрузки сотрудников
    employee_workload = {}

    # Находим все групповые задачи
    group_tasks = {}
    for task_id, task in task_map.items():
        if task.get('is_group'):
            group_tasks[str(task_id)] = task

    print(f"Найдено {len(group_tasks)} групповых задач для обработки подзадач")

    # Для каждой групповой задачи обрабатываем ее подзадачи
    for group_id, group_task in group_tasks.items():
        if group_id not in task_dates:
            continue

        group_start_str = task_dates[group_id]['start']
        group_end_str = task_dates[group_id]['end']

        group_start = datetime.datetime.strptime(group_start_str, '%Y-%m-%d')
        group_end = datetime.datetime.strptime(group_end_str, '%Y-%m-%d')

        print(
            f"Обработка групповой задачи {group_id}: {group_task.get('name', 'Без имени')} ({group_start_str} - {group_end_str})")

        # Получаем все подзадачи данной групповой задачи
        subtasks = get_all_subtasks_for_group(group_id, task_map, task_manager)

        if not subtasks:
            print(f"Не найдено подзадач для групповой задачи {group_id}")
            continue

        print(f"Найдено {len(subtasks)} подзадач для групповой задачи {group_id}")

        # Разделяем подзадачи на параллельные и последовательные
        parallel_subtasks = [task for task in subtasks if task.get('parallel')]
        sequential_subtasks = [task for task in subtasks if not task.get('parallel')]

        print(f"Параллельных подзадач: {len(parallel_subtasks)}, последовательных: {len(sequential_subtasks)}")

        # Обрабатываем параллельные подзадачи
        if parallel_subtasks:
            process_parallel_subtasks(parallel_subtasks, group_start, group_end, task_dates, employee_manager,
                                      employee_workload)

        # Обрабатываем последовательные подзадачи
        if sequential_subtasks:
            process_sequential_subtasks(sequential_subtasks, group_start, group_end, task_dates, employee_manager,
                                        employee_workload)

        # Обновляем даты групповой задачи на основе подзадач
        update_group_task_dates(group_id, subtasks, task_dates)

    return task_dates


def get_all_subtasks_for_group(group_id, task_map, task_manager):
    """
    Получает все подзадачи для групповой задачи из разных источников
    """
    subtasks = []

    # Проверяем task_map
    for task_id, task in task_map.items():
        parent_id = task.get('parent_id')
        if parent_id and (str(parent_id) == str(group_id) or parent_id == int(group_id)):
            subtasks.append(task)

    # Проверяем базу данных
    try:
        db_subtasks = task_manager.get_subtasks(int(group_id))
        for subtask in db_subtasks:
            subtask_id = subtask.get('id')
            if not any(st.get('id') == subtask_id for st in subtasks):
                subtasks.append(subtask)
                task_map[subtask_id] = subtask
    except Exception as e:
        print(f"Ошибка при получении подзадач из БД: {str(e)}")

    return subtasks

def process_parallel_subtasks(parallel_subtasks, group_start, group_end, task_dates, employee_manager,
                              employee_workload):
    """
    Обрабатывает параллельные подзадачи
    """
    print(f"Обработка {len(parallel_subtasks)} параллельных подзадач")

    for subtask in parallel_subtasks:
        subtask_id = subtask['id']
        subtask_duration = subtask.get('duration', 1)
        subtask_position = subtask.get('position')
        employee_id = subtask.get('employee_id')

        start_date_str = group_start.strftime('%Y-%m-%d')

        if employee_id:
            # Проверяем доступность назначенного сотрудника
            avail_start, avail_end, _ = get_available_dates_for_task(
                employee_id, start_date_str, subtask_duration, employee_manager
            )
            if avail_start:
                task_dates[subtask_id] = {
                    'start': avail_start,
                    'end': avail_end,
                    'employee_id': employee_id
                }
                employee_workload[employee_id] = employee_workload.get(employee_id, 0) + subtask_duration
                print(f"Параллельная подзадача {subtask_id}: сохранен назначенный сотрудник {employee_id}")
            else:
                # Сотрудник недоступен, используем стандартные даты
                end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
                task_dates[subtask_id] = {
                    'start': start_date_str,
                    'end': end_date.strftime('%Y-%m-%d'),
                    'employee_id': employee_id
                }
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
                print(f"Параллельная подзадача {subtask_id}: назначен сотрудник {new_employee_id}")
            else:
                # Не нашли сотрудника
                end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
                task_dates[subtask_id] = {
                    'start': start_date_str,
                    'end': end_date.strftime('%Y-%m-%d')
                }
        else:
            # Нет ни сотрудника, ни должности
            end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
            task_dates[subtask_id] = {
                'start': start_date_str,
                'end': end_date.strftime('%Y-%m-%d')
            }

def process_sequential_subtasks(sequential_subtasks, group_start, group_end, task_dates, employee_manager,
                                employee_workload):
    """
    Обрабатывает последовательные подзадачи
    """
    print(f"Обработка {len(sequential_subtasks)} последовательных подзадач")

    current_date = group_start

    for subtask in sequential_subtasks:
        subtask_id = subtask['id']
        subtask_duration = subtask.get('duration', 1)
        subtask_position = subtask.get('position')
        employee_id = subtask.get('employee_id')

        start_date_str = current_date.strftime('%Y-%m-%d')

        if employee_id:
            # Проверяем доступность назначенного сотрудника
            avail_start, avail_end, calendar_duration = get_available_dates_for_task(
                employee_id, start_date_str, subtask_duration, employee_manager
            )
            if avail_start:
                task_dates[subtask_id] = {
                    'start': avail_start,
                    'end': avail_end,
                    'employee_id': employee_id
                }
                employee_workload[employee_id] = employee_workload.get(employee_id, 0) + subtask_duration
                # Следующая подзадача начинается после текущей
                next_date = datetime.datetime.strptime(avail_end, '%Y-%m-%d') + datetime.timedelta(days=1)
                current_date = next_date
                print(
                    f"Последовательная подзадача {subtask_id}: сотрудник {employee_id}, даты: {avail_start} - {avail_end}")
            else:
                # Сотрудник недоступен, используем стандартные даты
                end_date = current_date + datetime.timedelta(days=subtask_duration - 1)
                task_dates[subtask_id] = {
                    'start': start_date_str,
                    'end': end_date.strftime('%Y-%m-%d'),
                    'employee_id': employee_id
                }
                current_date = end_date + datetime.timedelta(days=1)
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
                next_date = datetime.datetime.strptime(new_end, '%Y-%m-%d') + datetime.timedelta(days=1)
                current_date = next_date
                print(f"Последовательная подзадача {subtask_id}: назначен сотрудник {new_employee_id}")
            else:
                # Не нашли сотрудника
                end_date = current_date + datetime.timedelta(days=subtask_duration - 1)
                task_dates[subtask_id] = {
                    'start': start_date_str,
                    'end': end_date.strftime('%Y-%m-%d')
                }
                current_date = end_date + datetime.timedelta(days=1)
        else:
            # Нет ни сотрудника, ни должности
            end_date = current_date + datetime.timedelta(days=subtask_duration - 1)
            task_dates[subtask_id] = {
                'start': start_date_str,
                'end': end_date.strftime('%Y-%m-%d')
            }
            current_date = end_date + datetime.timedelta(days=1)

def update_group_task_dates(group_id, subtasks, task_dates):
    """
    Обновляет даты групповой задачи на основе подзадач
    """
    if not subtasks:
        return

    earliest_start = None
    latest_end = None

    for subtask in subtasks:
        subtask_id = subtask['id']

        if subtask_id in task_dates:
            subtask_start_str = task_dates[subtask_id].get('start')
            subtask_end_str = task_dates[subtask_id].get('end')

            if subtask_start_str and subtask_end_str:
                subtask_start = datetime.datetime.strptime(subtask_start_str, '%Y-%m-%d')
                subtask_end = datetime.datetime.strptime(subtask_end_str, '%Y-%m-%d')

                if earliest_start is None or subtask_start < earliest_start:
                    earliest_start = subtask_start

                if latest_end is None or subtask_end > latest_end:
                    latest_end = subtask_end

    # Обновляем даты групповой задачи
    if earliest_start and latest_end and group_id in task_dates:
        task_dates[group_id]['start'] = earliest_start.strftime('%Y-%m-%d')
        task_dates[group_id]['end'] = latest_end.strftime('%Y-%m-%d')

def final_parent_subtask_sync(task_dates, task_map, task_manager, employee_manager):
    """
    Финальная синхронизация дат родительских задач и подзадач (выполняется один раз)
    """
    print("Финальная синхронизация родителей и подзадач...")

    # Создаем мапинг родитель -> подзадачи
    parent_to_subtasks = {}
    for task_id, task in task_map.items():
        parent_id = task.get('parent_id')
        if parent_id:
            parent_id_str = str(parent_id)
            if parent_id_str not in parent_to_subtasks:
                parent_to_subtasks[parent_id_str] = []
            parent_to_subtasks[parent_id_str].append(str(task_id))

    # Обновляем каждую родительскую задачу на основе ее подзадач
    for parent_id, subtask_ids in parent_to_subtasks.items():
        if parent_id not in task_dates:
            continue

        earliest_start = None
        latest_end = None

        for subtask_id in subtask_ids:
            if subtask_id in task_dates:
                subtask_start_str = task_dates[subtask_id].get('start')
                subtask_end_str = task_dates[subtask_id].get('end')

                if subtask_start_str and subtask_end_str:
                    try:
                        subtask_start = datetime.datetime.strptime(subtask_start_str, '%Y-%m-%d')
                        subtask_end = datetime.datetime.strptime(subtask_end_str, '%Y-%m-%d')

                        if earliest_start is None or subtask_start < earliest_start:
                            earliest_start = subtask_start

                        if latest_end is None or subtask_end > latest_end:
                            latest_end = subtask_end
                    except ValueError as e:
                        print(f"Ошибка при обработке дат подзадачи {subtask_id}: {str(e)}")

        # Обновляем родительскую задачу
        if earliest_start and latest_end:
            old_start = task_dates[parent_id].get('start')
            old_end = task_dates[parent_id].get('end')

            new_start = earliest_start.strftime('%Y-%m-%d')
            new_end = latest_end.strftime('%Y-%m-%d')

            if old_start != new_start or old_end != new_end:
                task_dates[parent_id]['start'] = new_start
                task_dates[parent_id]['end'] = new_end
                print(
                    f"Финальное обновление родительской задачи {parent_id}: {old_start}-{old_end} -> {new_start}-{new_end}")

    return task_dates

def validate_dependencies_final(task_dates, graph, task_map, task_manager, employee_manager):
    """
    Финальная проверка зависимостей (выполняется один раз в конце)
    """
    print("Финальная проверка зависимостей...")

    # Создаем обратный граф (для поиска зависимых задач)
    reverse_graph = defaultdict(list)
    for task_id, predecessors in graph.items():
        for pred_id in predecessors:
            reverse_graph[pred_id].append(task_id)

    # Ищем нарушения зависимостей
    violations = []

    for task_id in reverse_graph:
        if task_id not in task_dates or 'end' not in task_dates[task_id]:
            continue

        end_date_str = task_dates[task_id]['end']
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')

        for dependent_id in reverse_graph[task_id]:
            if dependent_id not in task_dates or 'start' not in task_dates[dependent_id]:
                continue

            dependent_start_str = task_dates[dependent_id]['start']
            dependent_start = datetime.datetime.strptime(dependent_start_str, '%Y-%m-%d')

            if dependent_start <= end_date:
                violations.append((task_id, dependent_id, end_date, dependent_start))

    # Исправляем нарушения
    for pred_id, dep_id, pred_end, dep_start in violations:
        new_start = pred_end + datetime.timedelta(days=1)
        new_start_str = new_start.strftime('%Y-%m-%d')

        task = task_map.get(dep_id)
        if not task:
            continue

        task_duration = task.get('duration', 1)
        employee_id = task.get('employee_id') or task_dates.get(dep_id, {}).get('employee_id')
        position = task.get('position')

        # Пересчитываем даты с учетом выходных дней
        if employee_id:
            emp_start, emp_end, _ = get_available_dates_for_task(
                employee_id, new_start_str, task_duration, employee_manager
            )
            if emp_start:
                task_dates[dep_id]['start'] = emp_start
                task_dates[dep_id]['end'] = emp_end
                print(f"Исправлено нарушение зависимости: задача {dep_id} перенесена на {emp_start} - {emp_end}")
                continue

        # Если не удалось учесть выходные, используем стандартный расчет
        new_end = new_start + datetime.timedelta(days=task_duration - 1)
        task_dates[dep_id]['start'] = new_start_str
        task_dates[dep_id]['end'] = new_end.strftime('%Y-%m-%d')
        print(
            f"Исправлено нарушение зависимости: задача {dep_id} перенесена на {new_start_str} - {new_end.strftime('%Y-%m-%d')}")

    return task_dates

# def process_subtasks(task_dates, task_map, graph, task_manager, employee_manager):
#     """
#     Обрабатывает подзадачи групповых задач, устанавливая для них даты и назначая исполнителей,
#     со специальной обработкой параллельных подзадач с учетом их должностей.
#
#     Args:
#         task_dates (dict): Словарь с датами задач
#         task_map (dict): Словарь задач по ID
#         graph (dict): Граф зависимостей
#         task_manager: Менеджер задач
#         employee_manager: Менеджер сотрудников
#     """
#     import datetime
#     from utils.employee_availability import find_suitable_employee, get_available_dates_for_task
#
#     print("Обработка подзадач групповых задач...")
#     parent_to_subtasks = {}
#     # Словарь для отслеживания загрузки сотрудников
#     employee_workload = {}
#
#     # ИЗМЕНЕНИЕ: Словарь для отслеживания уже назначенных типов подзадач с учетом должности
#     # Ключ: (group_id, subtask_name, position, timeframe) -> Значение: список назначенных сотрудников
#     subtask_assignments = {}
#
#     # ИЗМЕНЕНИЕ: Структура для отслеживания дублирующихся подзадач с учетом должности
#     # Ключ: (group_id, subtask_name, position) -> Список объектов подзадач
#     duplicate_subtasks = {}
#
#     # Находим все групповые задачи с установленными датами
#     group_tasks = [(task_id, task) for task_id, task in task_map.items()
#                    if task.get('is_group') and task_id in task_dates]
#     parent_extreme_dates = {}  # parent_id -> (earliest_start, latest_end)
#
#     print(f"Найдено {len(group_tasks)} групповых задач для обработки подзадач")
#
#     # Для каждой групповой задачи обрабатываем ее подзадачи
#     for group_id, group_task in group_tasks:
#         group_start_str = task_dates[group_id]['start']
#         group_end_str = task_dates[group_id]['end']
#         if 'start' not in task_dates[group_id]:
#             task_dates[group_id]['start'] = group_start_str
#         else:
#             task_dates[group_id]['start'] = min(
#                 group_start_str, task_dates[group_id]['start']
#             )
#
#         if 'end' not in task_dates[group_id]:
#             task_dates[group_id]['end'] = group_end_str
#         else:
#             task_dates[group_id]['end'] = max(
#                 group_end_str, task_dates[group_id]['end']
#             )
#         group_start = datetime.datetime.strptime(group_start_str, '%Y-%m-%d')
#         group_end = datetime.datetime.strptime(group_end_str, '%Y-%m-%d')
#
#         print(
#             f"Обработка групповой задачи {group_id}: {group_task.get('name', 'Без имени')} ({group_start_str} - {group_end_str})")
#
#         # Получаем все подзадачи данной групповой задачи
#         subtasks_from_map = []
#         for task_id, task in task_map.items():
#             parent_id = task.get('parent_id')
#             # Проверяем разные типы parent_id, так как проблема может быть в несовпадении типов
#             if parent_id == group_id or str(parent_id) == str(group_id):
#                 subtasks_from_map.append(task)
#                 print(f"  Найдена подзадача в task_map: {task_id}: {task.get('name', 'Без имени')}")
#
#         # Получаем подзадачи из базы данных
#         try:
#             subtasks_from_db = task_manager.get_subtasks(group_id)
#             print(f"  Найдено {len(subtasks_from_db)} подзадач в базе данных")
#
#             # Добавляем подзадачи из базы данных, которых нет в task_map
#             for subtask in subtasks_from_db:
#                 st_id = subtask.get('id')
#                 if st_id not in [t.get('id') for t in subtasks_from_map]:
#                     subtasks_from_map.append(subtask)
#                     task_map[st_id] = subtask
#                     print(f"  Добавлена подзадача из БД: {st_id}: {subtask.get('name', 'Без имени')}")
#         except Exception as e:
#             print(f"  Ошибка при получении подзадач из БД: {str(e)}")
#
#         # Если все еще нет подзадач, проверяем поле subtasks в групповой задаче
#         if not subtasks_from_map and 'subtasks' in group_task:
#             print(f"  В групповой задаче найдено поле subtasks с {len(group_task['subtasks'])} подзадачами")
#
#             for subtask_data in group_task['subtasks']:
#                 # Преобразуем данные подзадачи из поля subtasks в формат задачи
#                 subtask = {
#                     'id': f"{group_id}_sub_{len(subtasks_from_map)}",  # Генерируем временный ID
#                     'name': subtask_data.get('name', 'Подзадача'),
#                     'duration': subtask_data.get('duration', 1),
#                     'position': subtask_data.get('position', ''),
#                     'parallel': subtask_data.get('parallel', False),
#                     'parent_id': group_id
#                 }
#                 subtasks_from_map.append(subtask)
#                 task_map[subtask['id']] = subtask
#                 print(f"  Добавлена подзадача из поля subtasks: {subtask['id']}: {subtask['name']}")
#
#         if not subtasks_from_map:
#             print(f"  ПРЕДУПРЕЖДЕНИЕ: Не найдено подзадач для групповой задачи {group_id}")
#             continue
#
#         print(f"Обработка {len(subtasks_from_map)} подзадач для групповой задачи {group_id}")
#
#         # ИЗМЕНЕНИЕ: Группируем подзадачи по имени И должности
#         subtask_groups = {}  # (имя задачи, должность) -> список задач с этим именем и должностью
#
#         for subtask in subtasks_from_map:
#             subtask_name = subtask.get('name', '')
#             subtask_position = subtask.get('position', '')  # Учитываем должность
#             group_key = (subtask_name, subtask_position)
#
#             if group_key not in subtask_groups:
#                 subtask_groups[group_key] = []
#             subtask_groups[group_key].append(subtask)
#
#             # Отслеживаем дублирующиеся подзадачи для последующего анализа
#             duplicate_key = (str(group_id), subtask_name, subtask_position)
#             if duplicate_key not in duplicate_subtasks:
#                 duplicate_subtasks[duplicate_key] = []
#             duplicate_subtasks[duplicate_key].append(subtask)
#
#         # Выводим информацию о дублирующихся подзадачах по группам должностей
#         for (subtask_name, position), tasks_list in subtask_groups.items():
#             if len(tasks_list) > 1:
#                 print(f"  Найдено {len(tasks_list)} экземпляров подзадачи '{subtask_name}' с должностью '{position}'")
#                 parallel_count = sum(1 for task in tasks_list if task.get('parallel'))
#                 print(
#                     f"    Из них параллельных: {parallel_count}, последовательных: {len(tasks_list) - parallel_count}")
#
#         # Разделяем подзадачи на параллельные и последовательные
#         parallel_subtasks = [task for task in subtasks_from_map if task.get('parallel')]
#         sequential_subtasks = [task for task in subtasks_from_map if not task.get('parallel')]
#
#         print(f"Параллельных подзадач: {len(parallel_subtasks)}, последовательных: {len(sequential_subtasks)}")
#
#         # ИЗМЕНЕНИЕ: Обрабатываем параллельные подзадачи по группам с учетом должности
#         for (subtask_name, position), similar_subtasks in subtask_groups.items():
#             # Отфильтровываем только параллельные подзадачи этого типа и должности
#             parallel_group = [task for task in similar_subtasks if task.get('parallel')]
#
#             if not parallel_group:
#                 continue  # Пропускаем, если нет параллельных подзадач этого типа
#
#             print(f"Обработка {len(parallel_group)} параллельных подзадач типа '{subtask_name}' с должностью '{position}'")
#
#             # Получаем доступных сотрудников для данной должности
#             if not position:
#                 print(f"  ПРЕДУПРЕЖДЕНИЕ: Не указана должность для подзадачи '{subtask_name}'")
#                 continue
#
#             try:
#                 available_employees = employee_manager.get_employees_by_position(position)
#                 print(f"  Найдено {len(available_employees)} сотрудников с должностью '{position}'")
#
#                 # Если сотрудников меньше, чем подзадач, выводим предупреждение
#                 if len(available_employees) < len(parallel_group):
#                     print(
#                         f"  ПРЕДУПРЕЖДЕНИЕ: Количество сотрудников ({len(available_employees)}) меньше количества подзадач ({len(parallel_group)})")
#                     print(f"  Некоторым сотрудникам придется назначить более одной подзадачи этого типа")
#
#                 # Определяем ключ для отслеживания назначений группы подзадач с учетом должности
#                 group_key = (str(group_id), subtask_name, position, group_start_str + "-" + group_end_str)
#                 if group_key not in subtask_assignments:
#                     subtask_assignments[group_key] = []
#
#                 # Инициализируем список для отслеживания назначенных сотрудников в этой группе
#                 assigned_employees = []
#
#                 # Назначаем каждую подзадачу наиболее подходящему сотруднику, избегая дублирования
#                 for idx, subtask in enumerate(parallel_group):
#                     subtask_id = subtask['id']
#                     subtask_duration = subtask.get('duration', 1)
#
#                     # Если сотрудник уже назначен, проверяем его доступность
#                     employee_id = subtask.get('employee_id')
#
#                     if employee_id and employee_id not in assigned_employees:
#                         # Проверяем доступность сотрудника
#                         avail_start, avail_end, _ = get_available_dates_for_task(
#                             employee_id, group_start_str, subtask_duration, employee_manager
#                         )
#
#                         if avail_start:
#                             # Сотрудник доступен и еще не назначен на другую подзадачу этого типа
#                             task_dates[subtask_id] = {
#                                 'start': avail_start,
#                                 'end': avail_end,
#                                 'employee_id': employee_id
#                             }
#
#                             # Добавляем сотрудника в список назначенных
#                             assigned_employees.append(employee_id)
#                             subtask_assignments[group_key].append(employee_id)
#
#                             # Обновляем загрузку сотрудника
#                             employee_workload[employee_id] = employee_workload.get(employee_id, 0) + subtask_duration
#
#                             print(f"  Подзадача {subtask_id}: сохранен назначенный сотрудник {employee_id}")
#                             continue  # Переходим к следующей подзадаче
#
#                     # Если уже назначенный сотрудник недоступен или уже назначен на другую подзадачу,
#                     # либо сотрудник еще не назначен, выбираем нового сотрудника
#
#                     # Отфильтровываем уже назначенных сотрудников
#                     available_unassigned = [e for e in available_employees if e['id'] not in assigned_employees]
#
#                     if available_unassigned:
#                         # Есть доступные ненаряженные сотрудники
#                         # Сортируем по нагрузке
#                         sorted_by_workload = sorted(
#                             available_unassigned,
#                             key=lambda e: employee_workload.get(e['id'], 0)
#                         )
#
#                         # Выбираем наименее загруженного
#                         new_employee = sorted_by_workload[0]
#                         new_employee_id = new_employee['id']
#
#                         # Проверяем его доступность
#                         avail_start, avail_end, _ = get_available_dates_for_task(
#                             new_employee_id, group_start_str, subtask_duration, employee_manager
#                         )
#
#                         if avail_start:
#                             # Назначаем сотрудника
#                             task_dates[subtask_id] = {
#                                 'start': avail_start,
#                                 'end': avail_end,
#                                 'employee_id': new_employee_id
#                             }
#
#                             # Добавляем в списки назначенных
#                             assigned_employees.append(new_employee_id)
#                             subtask_assignments[group_key].append(new_employee_id)
#
#                             # Обновляем загрузку
#                             employee_workload[new_employee_id] = employee_workload.get(new_employee_id,
#                                                                                        0) + subtask_duration
#
#                             try:
#                                 employee_name = new_employee.get('name', f"ID:{new_employee_id}")
#                                 print(f"  Подзадача {subtask_id}: назначен новый сотрудник {employee_name}")
#                             except:
#                                 print(f"  Подзадача {subtask_id}: назначен новый сотрудник {new_employee_id}")
#                         else:
#                             print(
#                                 f"  ПРЕДУПРЕЖДЕНИЕ: Выбранный сотрудник {new_employee_id} недоступен в указанный период")
#                             # Используем стандартные даты и оставляем без назначения
#                             end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
#                             task_dates[subtask_id] = {
#                                 'start': group_start_str,
#                                 'end': end_date.strftime('%Y-%m-%d')
#                             }
#                     else:
#                         # Если все сотрудники уже назначены, начинаем назначать повторно,
#                         # выбирая наименее загруженных
#                         sorted_by_workload = sorted(
#                             available_employees,
#                             key=lambda e: employee_workload.get(e['id'], 0)
#                         )
#
#                         # Берем наименее загруженного из всех
#                         best_employee = sorted_by_workload[0] if sorted_by_workload else None
#
#                         if best_employee:
#                             best_employee_id = best_employee['id']
#
#                             # Проверяем доступность
#                             avail_start, avail_end, _ = get_available_dates_for_task(
#                                 best_employee_id, group_start_str, subtask_duration, employee_manager
#                             )
#
#                             if avail_start:
#                                 # Назначаем этого сотрудника (повторно)
#                                 task_dates[subtask_id] = {
#                                     'start': avail_start,
#                                     'end': avail_end,
#                                     'employee_id': best_employee_id
#                                 }
#
#                                 # Не добавляем в assigned_employees, так как это повторное назначение
#                                 subtask_assignments[group_key].append(best_employee_id)
#
#                                 # Обновляем загрузку
#                                 employee_workload[best_employee_id] = employee_workload.get(best_employee_id,
#                                                                                             0) + subtask_duration
#
#                                 print(
#                                     f"  Подзадача {subtask_id}: назначен повторно сотрудник {best_employee_id} (нет других свободных сотрудников)")
#                             else:
#                                 # Если сотрудник недоступен, используем стандартные даты
#                                 end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
#                                 task_dates[subtask_id] = {
#                                     'start': group_start_str,
#                                     'end': end_date.strftime('%Y-%m-%d')
#                                 }
#                                 print(f"  ПРЕДУПРЕЖДЕНИЕ: Не удалось назначить сотрудника на подзадачу {subtask_id}")
#                         else:
#                             # Если вообще нет сотрудников (странная ситуация)
#                             end_date = group_start + datetime.timedelta(days=subtask_duration - 1)
#                             task_dates[subtask_id] = {
#                                 'start': group_start_str,
#                                 'end': end_date.strftime('%Y-%m-%d')
#                             }
#                             print(f"  КРИТИЧЕСКАЯ ОШИБКА: Не найдены сотрудники для должности '{position}'")
#             except Exception as e:
#                 print(f"  Ошибка при обработке подзадач типа '{subtask_name}' с должностью '{position}': {str(e)}")
#                 import traceback
#                 print(traceback.format_exc())
#
#         # Обрабатываем последовательные подзадачи - идут одна за другой
#         current_date = group_start
#
#         for subtask in sequential_subtasks:
#             subtask_id = subtask['id']
#             subtask_duration = subtask.get('duration', 1)
#             subtask_position = subtask.get('position')
#             employee_id = subtask.get('employee_id')
#
#             print(f"Обработка последовательной подзадачи {subtask_id}: {subtask.get('name', 'Без имени')}")
#
#             # Текущая дата начала подзадачи
#             start_date_str = current_date.strftime('%Y-%m-%d')
#
#             # Проверяем, что дата начала подзадачи не выходит за пределы групповой задачи
#             if current_date > group_end:
#                 print(f"  ПРЕДУПРЕЖДЕНИЕ: Подзадача {subtask_id} не помещается в рамки групповой задачи!")
#
#                 # Выбираем наиболее разумное поведение:
#                 # 1. Можно принудительно уменьшить длительность подзадачи
#                 # 2. Можно расширить рамки групповой задачи (этот вариант будем использовать)
#                 group_end = current_date + datetime.timedelta(days=subtask_duration - 1)
#                 task_dates[group_id]['end'] = group_end.strftime('%Y-%m-%d')
#                 print(f"  Расширяем групповую задачу до {group_end.strftime('%Y-%m-%d')}")
#
#             # Если у подзадачи уже назначен сотрудник
#             if employee_id:
#                 # Проверяем его доступность
#                 avail_start, avail_end, calendar_duration = get_available_dates_for_task(
#                     employee_id, start_date_str, subtask_duration, employee_manager
#                 )
#
#                 if avail_start:
#                     # Сотрудник доступен
#                     task_dates[subtask_id] = {
#                         'start': avail_start,
#                         'end': avail_end,
#                         'employee_id': employee_id
#                     }
#
#                     # Обновляем загрузку сотрудника
#                     employee_workload[employee_id] = employee_workload.get(employee_id, 0) + subtask_duration
#
#                     # Следующая подзадача начинается после текущей
#                     next_date = datetime.datetime.strptime(avail_end, '%Y-%m-%d') + datetime.timedelta(days=1)
#
#                     print(
#                         f"  Для подзадачи {subtask_id} назначен сотрудник {employee_id}, даты: {avail_start} - {avail_end}")
#
#                     # Обновляем текущую дату для следующей подзадачи
#                     current_date = next_date
#                 else:
#                     # Ищем другого подходящего сотрудника
#                     if subtask_position:
#                         new_employee_id, new_start, new_end, new_duration = find_suitable_employee(
#                             subtask_position, start_date_str, subtask_duration, employee_manager, employee_workload
#                         )
#
#                         if new_employee_id:
#                             task_dates[subtask_id] = {
#                                 'start': new_start,
#                                 'end': new_end,
#                                 'employee_id': new_employee_id
#                             }
#
#                             # Следующая подзадача начинается после текущей
#                             next_date = datetime.datetime.strptime(new_end, '%Y-%m-%d') + datetime.timedelta(days=1)
#                             current_date = next_date
#
#                             print(
#                                 f"  Для подзадачи {subtask_id} назначен новый сотрудник {new_employee_id}, даты: {new_start} - {new_end}")
#                         else:
#                             # Не нашли подходящего сотрудника, используем стандартные даты
#                             end_date = current_date + datetime.timedelta(days=subtask_duration - 1)
#                             task_dates[subtask_id] = {
#                                 'start': start_date_str,
#                                 'end': end_date.strftime('%Y-%m-%d'),
#                                 'employee_id': employee_id
#                             }
#
#                             # Следующая подзадача начинается после текущей
#                             current_date = end_date + datetime.timedelta(days=1)
#
#                             print(
#                                 f"  Для подзадачи {subtask_id} нет доступных сотрудников, используем стандартные даты")
#                     else:
#                         # Нет должности, используем стандартные даты
#                         end_date = current_date + datetime.timedelta(days=subtask_duration - 1)
#                         task_dates[subtask_id] = {
#                             'start': start_date_str,
#                             'end': end_date.strftime('%Y-%m-%d'),
#                             'employee_id': employee_id
#                         }
#
#                         # Следующая подзадача начинается после текущей
#                         current_date = end_date + datetime.timedelta(days=1)
#
#                         print(f"  Для подзадачи {subtask_id} нет должности, используем стандартные даты")
#             elif subtask_position:
#                 # Ищем подходящего сотрудника
#                 new_employee_id, new_start, new_end, new_duration = find_suitable_employee(
#                     subtask_position, start_date_str, subtask_duration, employee_manager, employee_workload
#                 )
#
#                 if new_employee_id:
#                     task_dates[subtask_id] = {
#                         'start': new_start,
#                         'end': new_end,
#                         'employee_id': new_employee_id
#                     }
#
#                     # Следующая подзадача начинается после текущей
#                     next_date = datetime.datetime.strptime(new_end, '%Y-%m-%d') + datetime.timedelta(days=1)
#                     current_date = next_date
#
#                     print(
#                         f"  Для подзадачи {subtask_id} назначен сотрудник {new_employee_id}, даты: {new_start} - {new_end}")
#                 else:
#                     # Не нашли подходящего сотрудника, используем стандартные даты
#                     end_date = current_date + datetime.timedelta(days=subtask_duration - 1)
#                     task_dates[subtask_id] = {
#                         'start': start_date_str,
#                         'end': end_date.strftime('%Y-%m-%d')
#                     }
#
#                     # Следующая подзадача начинается после текущей
#                     current_date = end_date + datetime.timedelta(days=1)
#
#                     print(f"  Для подзадачи {subtask_id} нет доступных сотрудников, используем стандартные даты")
#             else:
#                 # Нет ни сотрудника, ни должности, используем стандартные даты
#                 end_date = current_date + datetime.timedelta(days=subtask_duration - 1)
#                 task_dates[subtask_id] = {
#                     'start': start_date_str,
#                     'end': end_date.strftime('%Y-%m-%d')
#                 }
#
#                 # Следующая подзадача начинается после текущей
#                 current_date = end_date + datetime.timedelta(days=1)
#
#                 print(f"  Для подзадачи {subtask_id} нет ни сотрудника, ни должности, используем стандартные даты")
#
#         # Проверяем, что ни одна подзадача не выходит за пределы групповой задачи
#         # Если выходит, корректируем даты групповой задачи
#         latest_subtask_end = None
#         for subtask in subtasks_from_map:
#             subtask_id = subtask['id']
#
#             # Enhanced search for subtask end date in multiple possible formats
#             subtask_end_date = None
#             if subtask_id in task_dates and 'end' in task_dates[subtask_id]:
#                 subtask_end_date = task_dates[subtask_id]['end']
#             elif str(subtask_id) in task_dates and 'end' in task_dates[str(subtask_id)]:
#                 subtask_end_date = task_dates[str(subtask_id)]['end']
#
#             if subtask_end_date:
#                 subtask_end = datetime.datetime.strptime(subtask_end_date, '%Y-%m-%d')
#                 if latest_subtask_end is None or subtask_end > latest_subtask_end:
#                     latest_subtask_end = subtask_end
#                     print(f"  New latest end date from subtask {subtask_id}: {subtask_end_date}")
#
#         # Ensure we're working with datetime objects for comparison
#         if latest_subtask_end and isinstance(group_end, str):
#             group_end = datetime.datetime.strptime(group_end, '%Y-%m-%d')
#
#         # If the latest subtask ends after the group task, update the group task's end date
#         if latest_subtask_end and latest_subtask_end > group_end:
#             group_end_str = latest_subtask_end.strftime('%Y-%m-%d')
#             task_dates[group_id]['end'] = group_end_str
#             print(f"Дата окончания групповой задачи {group_id} обновлена до {group_end_str}")
#
#             # For debugging - confirm the update happened
#             print(f"ПРОВЕРКА: Новая дата окончания для группы {group_id}: {task_dates[group_id]['end']}")
#
#     # Выводим статистику по назначениям
#     print("\nСтатистика по назначениям подзадач:")
#     for group_key, assigned_employees in subtask_assignments.items():
#         group_id, subtask_name, position, timeframe = group_key
#         emp_count = len(assigned_employees)
#         unique_emp_count = len(set(assigned_employees))
#
#         if emp_count > unique_emp_count:
#             print(
#                 f"ВНИМАНИЕ: Группа '{subtask_name}' с должностью '{position}' в {group_id}: назначено {emp_count} подзадач на {unique_emp_count} сотрудников")
#             # Подсчитываем количество назначений для каждого сотрудника
#             from collections import Counter
#             employee_counts = Counter(assigned_employees)
#             duplicates = {emp_id: count for emp_id, count in employee_counts.items() if count > 1}
#             if duplicates:
#                 print(f"  Дублирующиеся назначения: {duplicates}")
#         else:
#             print(
#                 f"Группа '{subtask_name}' с должностью '{position}' в {group_id}: назначено {emp_count} подзадач на {unique_emp_count} сотрудников (OK)")
#
#     print("\nПроверка и корректировка дат родительских задач...")
#
#     # Create a mapping of parent IDs to all their subtasks
#     for task_id, task in task_map.items():
#         parent_id = task.get('parent_id')
#         if parent_id:
#             # Convert parent_id to string for consistent handling
#             parent_id_str = str(parent_id)
#             if parent_id_str not in parent_to_subtasks:
#                 parent_to_subtasks[parent_id_str] = []
#             parent_to_subtasks[parent_id_str].append(task_id)
#
#     # Process each parent task and ensure its dates encompass all subtasks
#     for parent_id, subtask_ids in parent_to_subtasks.items():
#         if parent_id not in task_dates:
#             print(f"  ПРОПУСК: Родительская задача {parent_id} не найдена в task_dates")
#             continue
#
#         parent_task = None
#         try:
#             # Try to get the parent task from task_map, handling different ID formats
#             if parent_id in task_map:
#                 parent_task = task_map[parent_id]
#             elif int(parent_id) in task_map:
#                 parent_task = task_map[int(parent_id)]
#         except (ValueError, TypeError) as e:
#             print(f"  ОШИБКА при получении родительской задачи {parent_id}: {str(e)}")
#             continue
#
#         if not parent_task:
#             print(f"  ПРОПУСК: Информация о родительской задаче {parent_id} не найдена")
#             continue
#
#         print(f"Проверка родительской задачи {parent_id}: {parent_task.get('name', 'Без имени')}")
#
#         # Find earliest start and latest end among all subtasks
#         earliest_start = None
#         latest_end = None
#
#         for subtask_id in subtask_ids:
#             subtask_start = None
#             subtask_end = None
#
#             # Check different forms of the subtask ID in task_dates
#             subtask_id_str = str(subtask_id)
#
#             # Try string ID
#             if subtask_id_str in task_dates:
#                 if 'start' in task_dates[subtask_id_str]:
#                     subtask_start = task_dates[subtask_id_str]['start']
#                 if 'end' in task_dates[subtask_id_str]:
#                     subtask_end = task_dates[subtask_id_str]['end']
#
#             # Try numeric ID if string didn't work
#             elif subtask_id in task_dates:
#                 if 'start' in task_dates[subtask_id]:
#                     subtask_start = task_dates[subtask_id]['start']
#                 if 'end' in task_dates[subtask_id]:
#                     subtask_end = task_dates[subtask_id]['end']
#
#             # Skip if we couldn't find start/end dates
#             if not subtask_start or not subtask_end:
#                 print(f"  ПРОПУСК: Не найдены даты для подзадачи {subtask_id}")
#                 continue
#
#             # Convert to datetime for comparison
#             try:
#                 start_date = datetime.datetime.strptime(subtask_start, '%Y-%m-%d')
#                 end_date = datetime.datetime.strptime(subtask_end, '%Y-%m-%d')
#
#                 # Update earliest start if needed
#                 if earliest_start is None or start_date < earliest_start:
#                     earliest_start = start_date
#
#                 # Update latest end if needed
#                 if latest_end is None or end_date > latest_end:
#                     latest_end = end_date
#                     print(f"  Обновлена дата окончания из подзадачи {subtask_id}: {subtask_end}")
#             except ValueError as e:
#                 print(f"  ОШИБКА при обработке дат подзадачи {subtask_id}: {str(e)}")
#
#         # Now update parent task dates if needed
#         if earliest_start and latest_end:
#             parent_start_str = task_dates[parent_id].get('start')
#             parent_end_str = task_dates[parent_id].get('end')
#
#             # Skip if parent doesn't have dates yet
#             if not parent_start_str or not parent_end_str:
#                 print(f"  ПРОПУСК: Родительская задача {parent_id} не имеет дат")
#                 continue
#
#             parent_start = datetime.datetime.strptime(parent_start_str, '%Y-%m-%d')
#             parent_end = datetime.datetime.strptime(parent_end_str, '%Y-%m-%d')
#
#             # Check if we need to update parent dates
#             start_changed = earliest_start < parent_start
#             end_changed = latest_end > parent_end
#
#             if start_changed or end_changed:
#                 # Update parent task dates
#                 if start_changed:
#                     task_dates[parent_id]['start'] = earliest_start.strftime('%Y-%m-%d')
#                     print(f"  Обновлена дата начала родительской задачи {parent_id}: {task_dates[parent_id]['start']}")
#
#                 if end_changed:
#                     task_dates[parent_id]['end'] = latest_end.strftime('%Y-%m-%d')
#                     print(f"  Обновлена дата окончания родительской задачи {parent_id}: {task_dates[parent_id]['end']}")
#             else:
#                 print(f"  Даты родительской задачи {parent_id} не требуют обновления")
#
#     processed_subtasks = sum(1 for tid in task_dates if tid in task_map and task_map[tid].get('parent_id'))
#     print(f"Обработка подзадач завершена. Обработано {processed_subtasks} подзадач.")
#     print(f"Проверено {len(parent_to_subtasks)} родительских задач.")
#
#     # Добавление финальной проверки дат родителей и подзадач
#     print("\nФинальная проверка согласованности дат родительских задач и подзадач...")
#
#     # Создаем словарь для отслеживания крайних дат подзадач каждой родительской задачи
#     parent_extreme_dates = {}  # parent_id -> (earliest_start, latest_end)
#
#     # Проходим по всем задачам и собираем информацию о подзадачах
#     for task_id, task in task_map.items():
#         parent_id = task.get('parent_id')
#         if parent_id:
#             # Конвертируем ID к строковому типу для единообразия
#             parent_id_str = str(parent_id)
#
#             # Получаем даты подзадачи
#             start_date = None
#             end_date = None
#
#             # Ищем даты в разных источниках
#             if task_id in task_dates:
#                 start_date = task_dates[task_id].get('start')
#                 end_date = task_dates[task_id].get('end')
#             elif str(task_id) in task_dates:
#                 start_date = task_dates[str(task_id)].get('start')
#                 end_date = task_dates[str(task_id)].get('end')
#
#             if start_date and end_date:
#                 # Инициализируем запись для родителя, если её еще нет
#                 if parent_id_str not in parent_extreme_dates:
#                     parent_extreme_dates[parent_id_str] = (start_date, end_date)
#                 else:
#                     curr_start, curr_end = parent_extreme_dates[parent_id_str]
#                     # Обновляем наиболее раннее начало
#                     if start_date < curr_start:
#                         curr_start = start_date
#                     # Обновляем наиболее позднее окончание
#                     if end_date > curr_end:
#                         curr_end = end_date
#                     parent_extreme_dates[parent_id_str] = (curr_start, curr_end)
#
#     # Теперь обновляем родительские задачи на основе крайних дат подзадач
#     for parent_id, (start_date, end_date) in parent_extreme_dates.items():
#         # Обновляем даты родительской задачи, чтобы они охватывали все подзадачи
#         if parent_id in task_dates:
#             parent_start = task_dates[parent_id].get('start')
#             parent_end = task_dates[parent_id].get('end')
#
#             updated = False
#             if parent_start and start_date < parent_start:
#                 task_dates[parent_id]['start'] = start_date
#                 updated = True
#                 print(f"  Обновлена дата начала родительской задачи {parent_id}: с {parent_start} на {start_date}")
#
#             if parent_end and end_date > parent_end:
#                 task_dates[parent_id]['end'] = end_date
#                 updated = True
#                 print(f"  Обновлена дата окончания родительской задачи {parent_id}: с {parent_end} на {end_date}")
#
#             # Проверка на выходные дни при необходимости
#             if updated and parent_id in task_map:
#                 parent_task = task_map[parent_id]
#                 employee_id = parent_task.get('employee_id')
#                 if employee_id:
#                     try:
#                         is_start_available = employee_manager.is_available(employee_id, task_dates[parent_id]['start'])
#                         is_end_available = employee_manager.is_available(employee_id, task_dates[parent_id]['end'])
#
#                         if not is_start_available or not is_end_available:
#                             print(
#                                 f"  ⚠️ Предупреждение: обновленные даты родительской задачи {parent_id} содержат выходные дни сотрудника {employee_id}")
#                     except Exception as e:
#                         print(f"  Ошибка при проверке доступности сотрудника: {str(e)}")
#
#     processed_subtasks = sum(1 for tid in task_dates if tid in task_map and task_map[tid].get('parent_id'))
#     print(f"Обработка подзадач завершена. Обработано {processed_subtasks} подзадач.")

# def revalidate_dependent_tasks(task_dates, graph, task_map, task_manager, employee_manager):
#     """
#     Проверяет и обновляет даты задач, имеющих зависимости, после обновления групповых задач
#
#     Args:
#         task_dates (dict): Словарь с датами задач
#         graph (dict): Граф зависимостей
#         task_map (dict): Словарь задач по ID
#         task_manager: Менеджер задач
#         employee_manager: Менеджер сотрудников
#     """
#     import datetime
#
#     print("Проверка и корректировка зависимостей между задачами...")
#
#     # Улучшение: создание полного графа зависимостей, включая подзадачи
#     full_dependency_graph = {}
#
#     # Копируем основные зависимости
#     for task_id, predecessors in graph.items():
#         full_dependency_graph[task_id] = list(predecessors)
#
#     # Добавляем зависимости от подзадач родительских задач
#     for task_id in task_map:
#         parent_id = task_map[task_id].get('parent_id')
#         if parent_id:
#             # Находим все задачи, зависящие от родительской задачи
#             for dep_id, dep_predecessors in graph.items():
#                 if str(parent_id) in dep_predecessors or parent_id in dep_predecessors:
#                     # Добавляем зависимость и от подзадачи тоже
#                     if dep_id not in full_dependency_graph:
#                         full_dependency_graph[dep_id] = []
#                     if task_id not in full_dependency_graph[dep_id]:
#                         full_dependency_graph[dep_id].append(task_id)
#
#     # Создаем обратный граф для поиска зависимых задач
#     reverse_graph = {}
#     for task_id, predecessors in full_dependency_graph.items():
#         for pred_id in predecessors:
#             if pred_id not in reverse_graph:
#                 reverse_graph[pred_id] = []
#             reverse_graph[pred_id].append(task_id)
#
#     # Отладочная информация
#     print(f"Построен обратный граф зависимостей с {len(reverse_graph)} вершинами")
#
#     # Проходим по всем задачам, от которых зависят другие задачи
#     updates_needed = []
#
#     for task_id in reverse_graph:
#         if task_id not in task_dates or 'end' not in task_dates[task_id]:
#             continue
#
#         # Получаем дату окончания текущей задачи
#         end_date_str = task_dates[task_id]['end']
#         end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
#         next_day_date = end_date + datetime.timedelta(days=1)
#         next_day_str = next_day_date.strftime('%Y-%m-%d')
#
#         # Проверяем все зависимые задачи
#         for dependent_id in reverse_graph[task_id]:
#             if dependent_id not in task_dates or 'start' not in task_dates[dependent_id]:
#                 continue
#
#             dependent_start_str = task_dates[dependent_id]['start']
#             dependent_start = datetime.datetime.strptime(dependent_start_str, '%Y-%m-%d')
#
#             # Если дата начала зависимой задачи раньше или равна дате окончания предшествующей задачи,
#             # нужно сдвинуть зависимую задачу на следующий день
#             if dependent_start <= end_date:
#                 # Получаем имя задачи для логирования
#                 task_name = task_map[task_id]['name'] if task_id in task_map else f"Задача {task_id}"
#                 dep_name = task_map[dependent_id]['name'] if dependent_id in task_map else f"Задача {dependent_id}"
#
#                 print(f"Обнаружен конфликт: задача {dependent_id} ({dep_name}) начинается {dependent_start_str}, "
#                       f"но её предшественник {task_id} ({task_name}) заканчивается {end_date_str}")
#
#                 # Добавляем в список задач для обновления
#                 updates_needed.append((dependent_id, next_day_str))
#
#     # Применяем обновления
#     for dependent_id, new_start_str in updates_needed:
#         task = task_map.get(dependent_id)
#         if not task:
#             continue
#
#         task_name = task.get('name', f"Задача {dependent_id}")
#         task_duration = task.get('duration', 1)
#         position = task.get('position')  # Получаем должность для задачи
#
#         print(f"Обновление даты начала для задачи {dependent_id}: {task_name} на {new_start_str}")
#
#         # Обновляем дату начала и рассчитываем новую дату окончания
#         if task.get('is_group'):
#             # Для групповой задачи устанавливаем стандартные даты
#             new_start = datetime.datetime.strptime(new_start_str, '%Y-%m-%d')
#             new_end = new_start + datetime.timedelta(days=task_duration - 1)
#
#             task_dates[dependent_id] = {
#                 'start': new_start_str,
#                 'end': new_end.strftime('%Y-%m-%d')
#             }
#
#             print(f"Групповая задача {dependent_id}: {task_name} - обновленные даты: "
#                   f"{new_start_str} - {new_end.strftime('%Y-%m-%d')}")
#         else:
#             # Для обычной задачи учитываем выходные дни
#             employee_id = task.get('employee_id')
#
#             if employee_id:
#                 # Проверяем доступность сотрудника
#                 employee_start, employee_end, calendar_duration = get_available_dates_for_task(
#                     employee_id, new_start_str, task_duration, employee_manager
#                 )
#
#                 if employee_start:
#                     task_dates[dependent_id] = {
#                         'start': employee_start,
#                         'end': employee_end,
#                         'employee_id': employee_id
#                     }
#                     print(f"Задача {dependent_id}: {task_name} - обновленные даты: {employee_start} - {employee_end}")
#                 else:
#                     # Сотрудник недоступен, используем стандартные даты
#                     new_start = datetime.datetime.strptime(new_start_str, '%Y-%m-%d')
#                     new_end = new_start + datetime.timedelta(days=task_duration - 1)
#
#                     task_dates[dependent_id] = {
#                         'start': new_start_str,
#                         'end': new_end.strftime('%Y-%m-%d'),
#                         'employee_id': employee_id
#                     }
#             elif position:  # Если не назначен сотрудник, но указана должность
#                 # Ищем подходящего сотрудника
#                 new_employee_id, new_start, new_end, new_duration = find_suitable_employee(
#                     position, new_start_str, task_duration, employee_manager
#                 )
#
#                 if new_employee_id:
#                     task_dates[dependent_id] = {
#                         'start': new_start,
#                         'end': new_end,
#                         'employee_id': new_employee_id
#                     }
#                     print(
#                         f"Задача {dependent_id}: {task_name} - назначен сотрудник {new_employee_id}, даты: {new_start} - {new_end}")
#                 else:
#                     # Используем стандартные даты
#                     new_start = datetime.datetime.strptime(new_start_str, '%Y-%m-%d')
#                     new_end = new_start + datetime.timedelta(days=task_duration - 1)
#
#                     task_dates[dependent_id] = {
#                         'start': new_start_str,
#                         'end': new_end.strftime('%Y-%m-%d')
#                     }
#                     print(f"Задача {dependent_id}: {task_name} - не найден подходящий сотрудник")
#             else:
#                 # Используем стандартные даты
#                 new_start = datetime.datetime.strptime(new_start_str, '%Y-%m-%d')
#                 new_end = new_start + datetime.timedelta(days=task_duration - 1)
#
#                 task_dates[dependent_id] = {
#                     'start': new_start_str,
#                     'end': new_end.strftime('%Y-%m-%d')
#                 }
#
#         # Рекурсивно обновляем задачи, зависящие от текущей
#         if dependent_id in reverse_graph:
#             for next_dependent_id in reverse_graph[dependent_id]:
#                 # Если задача уже в списке обновлений, пропускаем
#                 if not any(next_dependent_id == task_id for task_id, _ in updates_needed):
#                     next_start = datetime.datetime.strptime(task_dates[dependent_id]['end'],
#                                                             '%Y-%m-%d') + datetime.timedelta(days=1)
#                     updates_needed.append((next_dependent_id, next_start.strftime('%Y-%m-%d')))

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
        task_id_str = str(task_id)
        graph[task_id_str] = []
        task_map[task_id_str] = task
        # Также добавим отображение числового ID на строковый
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
                graph[pred_id_str] = []
                # Пытаемся найти задачу с таким ID
                pred_task = None
                for t in tasks:
                    if str(t['id']) == pred_id_str:
                        pred_task = t
                        break
                if pred_task:
                    task_map[pred_id_str] = pred_task

            graph[task_id].append(pred_id_str)

    return graph, task_map

def topological_sort(graph):
    """
    Выполняет топологическую сортировку задач
    """
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

    # Выполняем топологическую сортировку
    while queue:
        current = queue.pop(0)
        result.append(current)

        # Обновляем зависимости для задач, зависящих от текущей
        if current in reverse_graph:
            for dependent in reverse_graph[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

    # Проверяем, все ли задачи обработаны
    if len(result) != len(all_nodes):
        print("ПРЕДУПРЕЖДЕНИЕ: В графе обнаружены циклы!")
        # Добавляем непосещенные узлы в конец
        for node in all_nodes:
            if node not in result:
                result.append(node)

    return result


def calculate_project_duration(project_start_date, task_dates):
    """
    Рассчитывает общую длительность проекта в днях
    """
    if not task_dates:
        return 0

    try:
        project_start = datetime.datetime.strptime(project_start_date, '%Y-%m-%d')

        # Находим самую позднюю дату окончания
        latest_end_date = None

        for task_id, dates in task_dates.items():
            if 'end' in dates:
                try:
                    end_date = datetime.datetime.strptime(dates['end'], '%Y-%m-%d')
                    if latest_end_date is None or end_date > latest_end_date:
                        latest_end_date = end_date
                except (ValueError, TypeError):
                    continue

        if latest_end_date is None:
            return 0

        # Рассчитываем длительность в днях
        duration = (latest_end_date - project_start).days + 1
        return duration

    except Exception as e:
        print(f"Ошибка при расчете длительности проекта: {str(e)}")
        return 0


def identify_critical_path(task_dates, graph, task_map):
    """
    Определяет критический путь проекта
    """
    if not task_dates:
        return []

    # Находим задачу с самой поздней датой окончания
    latest_end_date = None
    latest_task_id = None

    for task_id, dates in task_dates.items():
        if 'end' in dates:
            try:
                end_date = datetime.datetime.strptime(dates['end'], '%Y-%m-%d')
                if latest_end_date is None or end_date > latest_end_date:
                    latest_end_date = end_date
                    latest_task_id = task_id
            except (ValueError, TypeError):
                continue

    if latest_task_id is None:
        return []

    # Строим критический путь от конца к началу
    critical_path = []
    current_task_id = latest_task_id

    while current_task_id is not None:
        critical_path.append(current_task_id)

        # Находим предшественника с самой поздней датой окончания
        predecessors = graph.get(current_task_id, [])
        if not predecessors:
            break

        latest_predecessor_id = None
        latest_predecessor_end = None

        for pred_id in predecessors:
            if pred_id in task_dates and 'end' in task_dates[pred_id]:
                try:
                    end_date = datetime.datetime.strptime(task_dates[pred_id]['end'], '%Y-%m-%d')
                    if latest_predecessor_end is None or end_date > latest_predecessor_end:
                        latest_predecessor_end = end_date
                        latest_predecessor_id = pred_id
                except (ValueError, TypeError):
                    continue

        current_task_id = latest_predecessor_id

    # Возвращаем критический путь в правильном порядке (от начала к концу)
    return list(reversed(critical_path))

def update_database_assignments(task_dates, task_manager, employee_manager=None):
    """
    Обновляет назначения сотрудников и даты в базе данных с пакетной обработкой

    Args:
        task_dates (dict): Словарь с рассчитанными датами и назначениями задач
        task_manager: Менеджер задач
        employee_manager: Менеджер сотрудников (опционально)

    Returns:
        int: Количество обновленных задач
    """
    print(f"Пакетное обновление базы данных для {len(task_dates)} задач...")

    # Подготавливаем пакеты для обновления
    date_updates = []
    employee_updates = []

    for task_id, task_data in task_dates.items():
        try:
            # Преобразуем task_id в числовой формат
            numeric_task_id = int(task_id) if isinstance(task_id, str) and task_id.isdigit() else task_id
        except (ValueError, TypeError):
            print(f"Ошибка конвертации ID задачи {task_id}")
            continue

        # Получаем данные для обновления
        start_date = task_data.get('start')
        end_date = task_data.get('end')
        employee_id = task_data.get('employee_id')

        # Добавляем в пакеты обновлений
        if start_date and end_date:
            date_updates.append((start_date, end_date, numeric_task_id))

        if employee_id is not None:
            employee_updates.append((employee_id, numeric_task_id))

    # Выполняем пакетное обновление дат
    updated_dates = 0
    if date_updates:
        try:
            task_manager.db.connect()
            task_manager.db.cursor.executemany(
                "UPDATE tasks SET start_date = ?, end_date = ? WHERE id = ?",
                date_updates
            )
            task_manager.db.connection.commit()
            task_manager.db.close()
            updated_dates = len(date_updates)
            print(f"Обновлены даты для {updated_dates} задач")
        except Exception as e:
            print(f"Ошибка при пакетном обновлении дат: {str(e)}")
            task_manager.db.close()

    # Выполняем пакетное обновление назначений сотрудников
    updated_employees = 0
    if employee_updates:
        try:
            task_manager.db.connect()
            task_manager.db.cursor.executemany(
                "UPDATE tasks SET employee_id = ? WHERE id = ?",
                employee_updates
            )
            task_manager.db.connection.commit()
            task_manager.db.close()
            updated_employees = len(employee_updates)
            print(f"Обновлены назначения для {updated_employees} задач")
        except Exception as e:
            print(f"Ошибка при пакетном обновлении назначений: {str(e)}")
            task_manager.db.close()

    total_updated = updated_dates + updated_employees
    print(f"Общее количество обновлений: {total_updated}")

    return total_updated

def print_workload_statistics(employee_workload, employee_manager):
    """
    Выводит статистику загрузки сотрудников
    """
    print("\nСтатистика загрузки сотрудников:")

    for emp_id, workload in employee_workload.items():
        try:
            employee = employee_manager.get_employee(emp_id)
            print(f"  {employee['name']} ({employee['position']}): {workload} дней")
        except:
            print(f"  Сотрудник ID {emp_id}: {workload} дней")

    if employee_workload:
        avg_workload = sum(employee_workload.values()) / len(employee_workload)
        print(f"Средняя нагрузка: {avg_workload:.1f} дней")


def validate_project_schedule(task_dates, task_map, graph=None):
    """
    Комплексная проверка корректности календарного плана

    Args:
        task_dates (dict): Словарь с датами задач
        task_map (dict): Словарь задач по ID
        graph (dict): Граф зависимостей между задачами

    Returns:
        tuple: (is_valid, warnings) - валидность плана и список предупреждений
    """
    warnings = []
    critical_errors = []

    print("Проверка корректности календарного плана...")

    # 1. Проверяем основные данные
    for task_id, dates in task_dates.items():
        start_date = dates.get('start')
        end_date = dates.get('end')

        if not start_date or not end_date:
            critical_errors.append(f"Задача {task_id}: отсутствуют даты")
            continue

        try:
            start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.datetime.strptime(end_date, '%Y-%m-%d')

            if start > end:
                critical_errors.append(
                    f"Задача {task_id}: дата начала ({start_date}) позже даты окончания ({end_date})")

        except ValueError as e:
            critical_errors.append(f"Задача {task_id}: некорректный формат дат - {str(e)}")

    # 2. Проверяем согласованность родительских задач и подзадач
    parent_to_subtasks = {}
    for task_id, task in task_map.items():
        parent_id = task.get('parent_id')
        if parent_id:
            parent_id_str = str(parent_id)
            if parent_id_str not in parent_to_subtasks:
                parent_to_subtasks[parent_id_str] = []
            parent_to_subtasks[parent_id_str].append(str(task_id))

    for parent_id, subtask_ids in parent_to_subtasks.items():
        if parent_id not in task_dates:
            warnings.append(f"Родительская задача {parent_id} не найдена в расписании")
            continue

        parent_dates = task_dates[parent_id]
        parent_start = parent_dates.get('start')
        parent_end = parent_dates.get('end')

        if not parent_start or not parent_end:
            continue

        try:
            parent_start_date = datetime.datetime.strptime(parent_start, '%Y-%m-%d')
            parent_end_date = datetime.datetime.strptime(parent_end, '%Y-%m-%d')

            # Находим крайние даты подзадач
            earliest_subtask = None
            latest_subtask = None

            for subtask_id in subtask_ids:
                if subtask_id not in task_dates:
                    warnings.append(f"Подзадача {subtask_id} не найдена в расписании")
                    continue

                subtask_dates = task_dates[subtask_id]
                subtask_start = subtask_dates.get('start')
                subtask_end = subtask_dates.get('end')

                if not subtask_start or not subtask_end:
                    continue

                try:
                    subtask_start_date = datetime.datetime.strptime(subtask_start, '%Y-%m-%d')
                    subtask_end_date = datetime.datetime.strptime(subtask_end, '%Y-%m-%d')

                    if earliest_subtask is None or subtask_start_date < earliest_subtask:
                        earliest_subtask = subtask_start_date

                    if latest_subtask is None or subtask_end_date > latest_subtask:
                        latest_subtask = subtask_end_date

                except ValueError:
                    continue

            # Проверяем согласованность
            if earliest_subtask and earliest_subtask < parent_start_date:
                warnings.append(
                    f"Подзадачи родительской задачи {parent_id} начинаются раньше ({earliest_subtask.strftime('%Y-%m-%d')}) чем родительская задача ({parent_start})")

            if latest_subtask and latest_subtask > parent_end_date:
                warnings.append(
                    f"Подзадачи родительской задачи {parent_id} заканчиваются позже ({latest_subtask.strftime('%Y-%m-%d')}) чем родительская задача ({parent_end})")

        except ValueError as e:
            warnings.append(f"Ошибка при проверке дат родительской задачи {parent_id}: {str(e)}")

    # 3. Проверяем зависимости между задачами
    if graph:
        dependency_violations = 0
        for task_id, predecessors in graph.items():
            if not predecessors or task_id not in task_dates:
                continue

            task_start = task_dates[task_id].get('start')
            if not task_start:
                continue

            try:
                task_start_date = datetime.datetime.strptime(task_start, '%Y-%m-%d')

                for pred_id in predecessors:
                    if pred_id not in task_dates:
                        warnings.append(f"Предшественник {pred_id} задачи {task_id} не найден в расписании")
                        continue

                    pred_end = task_dates[pred_id].get('end')
                    if not pred_end:
                        continue

                    try:
                        pred_end_date = datetime.datetime.strptime(pred_end, '%Y-%m-%d')

                        if task_start_date <= pred_end_date:
                            dependency_violations += 1
                            task_name = task_map.get(task_id, {}).get('name', f'Задача {task_id}')
                            pred_name = task_map.get(pred_id, {}).get('name', f'Задача {pred_id}')
                            critical_errors.append(
                                f"Нарушение зависимости: '{task_name}' начинается {task_start}, "
                                f"но её предшественник '{pred_name}' заканчивается {pred_end}"
                            )

                    except ValueError:
                        continue

            except ValueError:
                continue

        if dependency_violations > 0:
            print(f"❌ Найдено {dependency_violations} нарушений зависимостей!")

    # 4. Проверяем разумность длительности задач
    duration_warnings = 0
    for task_id, dates in task_dates.items():
        start_date = dates.get('start')
        end_date = dates.get('end')

        if start_date and end_date:
            try:
                start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
                end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
                duration = (end - start).days + 1

                # Проверяем слишком длинные задачи
                if duration > 90:  # Более 3 месяцев
                    task_name = task_map.get(task_id, {}).get('name', f'Задача {task_id}')
                    warnings.append(f"Задача '{task_name}' имеет очень большую длительность: {duration} дней")
                    duration_warnings += 1

            except ValueError:
                continue

    # 5. Выводим сводку
    total_issues = len(critical_errors) + len(warnings)

    if critical_errors:
        print(f"❌ Найдено {len(critical_errors)} критических ошибок:")
        for error in critical_errors[:5]:  # Показываем первые 5
            print(f"  • {error}")
        if len(critical_errors) > 5:
            print(f"  ... и еще {len(critical_errors) - 5} ошибок")

    if warnings:
        print(f"⚠️ Найдено {len(warnings)} предупреждений:")
        for warning in warnings[:3]:  # Показываем первые 3
            print(f"  • {warning}")
        if len(warnings) > 3:
            print(f"  ... и еще {len(warnings) - 3} предупреждений")

    if not critical_errors and not warnings:
        print("✅ Календарный план прошел проверку - критических проблем не найдено!")

    is_valid = len(critical_errors) == 0
    all_issues = critical_errors + warnings

    return is_valid, all_issues

# def debug_check_parent_subtask_dates(task_dates, task_map, task_manager):
#     """
#     Performs a final verification of parent-subtask date relationships
#     and fixes any inconsistencies before database update
#     """
#     print("\n=== FINAL VERIFICATION AND CORRECTION OF PARENT-SUBTASK DATES ===")
#     import datetime
#
#     # Get all parent tasks
#     parent_tasks = {}
#     for task_id, task in task_map.items():
#         if task.get('is_group'):
#             parent_tasks[str(task_id)] = task
#
#     print(f"Found {len(parent_tasks)} parent tasks to verify")
#
#     # For each parent, verify all its subtasks
#     for parent_id, parent_task in parent_tasks.items():
#         try:
#             print(f"\nVerifying parent task {parent_id}: {parent_task.get('name', 'Unknown')}")
#
#             # Skip if parent not in task_dates
#             if parent_id not in task_dates:
#                 print(f"  WARNING: Parent task {parent_id} not in task_dates, skipping")
#                 continue
#
#             # Get parent end date
#             parent_start_str = task_dates[parent_id].get('start')
#             parent_end_str = task_dates[parent_id].get('end')
#
#             if not parent_start_str or not parent_end_str:
#                 print(f"  WARNING: Parent task {parent_id} has no dates, skipping")
#                 continue
#
#             parent_start = datetime.datetime.strptime(parent_start_str, '%Y-%m-%d')
#             parent_end = datetime.datetime.strptime(parent_end_str, '%Y-%m-%d')
#             print(f"  Parent dates: {parent_start_str} - {parent_end_str}")
#
#             # Get all subtasks of this parent
#             # First check using task_manager
#             subtasks = []
#             try:
#                 # Try to get subtasks from database
#                 db_subtasks = task_manager.get_subtasks(int(parent_id))
#                 if db_subtasks:
#                     subtasks.extend(db_subtasks)
#                     print(f"  Found {len(db_subtasks)} subtasks in database")
#             except Exception as e:
#                 print(f"  ERROR getting subtasks from database: {str(e)}")
#
#             # Also check parent_id in task_map
#             for task_id, task in task_map.items():
#                 task_parent_id = task.get('parent_id')
#                 if not task_parent_id:
#                     continue
#
#                 # Check if this task's parent matches our current parent
#                 if str(task_parent_id) == parent_id or task_parent_id == int(parent_id):
#                     subtask_already_added = False
#                     for existing_subtask in subtasks:
#                         if str(existing_subtask.get('id', '')) == str(task_id):
#                             subtask_already_added = True
#                             break
#
#                     if not subtask_already_added:
#                         subtasks.append(task)
#                         print(f"  Found additional subtask in task_map: {task_id} - {task.get('name', 'Unknown')}")
#
#             if not subtasks:
#                 print(f"  WARNING: No subtasks found for parent {parent_id}")
#                 continue
#
#             # Check end date of each subtask
#             earliest_start = parent_start
#             latest_end = parent_end
#             earliest_subtask_id = None
#             latest_subtask_id = None
#             earliest_start_str = parent_start_str
#             latest_end_str = parent_end_str
#
#             dates_mismatch = False
#
#             for subtask in subtasks:
#                 subtask_id = str(subtask.get('id', ''))
#                 if not subtask_id:
#                     continue
#
#                 # Get subtask end date from task_dates
#                 subtask_start_str = None
#                 subtask_end_str = None
#                 if subtask_id in task_dates and 'start' in task_dates[subtask_id] and 'end' in task_dates[subtask_id]:
#                     subtask_start_str = task_dates[subtask_id]['start']
#                     subtask_end_str = task_dates[subtask_id]['end']
#                 else:
#                     # Try numeric ID
#                     try:
#                         numeric_id = int(subtask_id)
#                         if numeric_id in task_dates and 'start' in task_dates[numeric_id] and 'end' in task_dates[
#                             numeric_id]:
#                             subtask_start_str = task_dates[numeric_id]['start']
#                             subtask_end_str = task_dates[numeric_id]['end']
#                     except (ValueError, TypeError):
#                         pass
#
#                 # If still not found, try getting from the subtask directly
#                 if not subtask_start_str and subtask.get('start_date'):
#                     subtask_start_str = subtask['start_date']
#                 if not subtask_end_str and subtask.get('end_date'):
#                     subtask_end_str = subtask['end_date']
#
#                 # Skip if no dates found
#                 if not subtask_start_str or not subtask_end_str:
#                     print(f"  WARNING: Subtask {subtask_id} has no dates")
#                     continue
#
#                 print(
#                     f"  Subtask {subtask_id}: {subtask.get('name', 'Unknown')}, dates: {subtask_start_str} - {subtask_end_str}")
#
#                 # Compare with parent dates
#                 try:
#                     subtask_start = datetime.datetime.strptime(subtask_start_str, '%Y-%m-%d')
#                     subtask_end = datetime.datetime.strptime(subtask_end_str, '%Y-%m-%d')
#
#                     # Check if this subtask starts earlier than current earliest
#                     if subtask_start < earliest_start:
#                         earliest_start = subtask_start
#                         earliest_subtask_id = subtask_id
#                         earliest_start_str = subtask_start_str
#                         dates_mismatch = True
#                         print(f"  Found earlier start date: {earliest_start_str} from subtask {earliest_subtask_id}")
#
#                     # Check if this subtask ends later than current latest
#                     if subtask_end > latest_end:
#                         latest_end = subtask_end
#                         latest_subtask_id = subtask_id
#                         latest_end_str = subtask_end_str
#                         dates_mismatch = True
#                         print(f"  Found later end date: {latest_end_str} from subtask {latest_subtask_id}")
#                 except ValueError as e:
#                     print(f"  ERROR parsing subtask dates: {str(e)}")
#
#             # Update parent dates if needed
#             if dates_mismatch:
#                 print(f"  ⚠️ MISMATCH DETECTED: Parent dates {parent_start_str} - {parent_end_str}")
#
#                 if earliest_start < parent_start:
#                     print(
#                         f"  ⚠️ Subtask {earliest_subtask_id} starts at {earliest_start_str}, which is before parent start {parent_start_str}")
#
#                 if latest_end > parent_end:
#                     print(
#                         f"  ⚠️ Subtask {latest_subtask_id} ends at {latest_end_str}, which is after parent end {parent_end_str}")
#
#                 # Update parent dates in task_dates
#                 task_dates[parent_id]['start'] = earliest_start_str
#                 task_dates[parent_id]['end'] = latest_end_str
#                 print(f"  ✅ CORRECTED: Updated parent {parent_id} dates to {earliest_start_str} - {latest_end_str}")
#
#                 # Directly update in database as well for extra certainty
#                 try:
#                     task_manager.db.execute(
#                         "UPDATE tasks SET start_date = ?, end_date = ? WHERE id = ?",
#                         (earliest_start_str, latest_end_str, int(parent_id))
#                     )
#                     print(f"  ✅ COMMITTED: Direct database update for parent {parent_id}")
#                 except Exception as e:
#                     print(f"  ERROR updating database: {str(e)}")
#             else:
#                 print(
#                     f"  ✓ VERIFIED: Parent dates {parent_start_str} - {parent_end_str} correctly encompass all subtasks")
#
#         except Exception as e:
#             print(f"ERROR verifying parent {parent_id}: {str(e)}")
#             import traceback
#             print(traceback.format_exc())
#
#     # Verify task dependencies - check for cases where a task starts before its predecessor ends
#     print("\n=== VERIFYING TASK DEPENDENCIES ===")
#
#     dependencies_fixed = 0
#
#     # Build a dependency dictionary
#     dependency_map = {}  # task_id -> list of predecessor IDs
#
#     # Collect dependencies from task_map
#     for task_id, task in task_map.items():
#         predecessors = task.get('predecessors', [])
#         if predecessors:
#             if isinstance(predecessors, str):
#                 try:
#                     # Try to parse as JSON if it's a string
#                     import json
#                     predecessors = json.loads(predecessors)
#                 except:
#                     # If not valid JSON, try comma-separated
#                     predecessors = [p.strip() for p in predecessors.split(',') if p.strip()]
#
#             # Convert to list of strings
#             predecessors = [str(p) for p in predecessors]
#             dependency_map[str(task_id)] = predecessors
#
#     # Check each dependency
#     for task_id, predecessors in dependency_map.items():
#         if task_id not in task_dates or 'start' not in task_dates[task_id]:
#             continue
#
#         task_start_str = task_dates[task_id]['start']
#         task_start = datetime.datetime.strptime(task_start_str, '%Y-%m-%d')
#
#         for pred_id in predecessors:
#             if pred_id not in task_dates or 'end' not in task_dates[pred_id]:
#                 continue
#
#             pred_end_str = task_dates[pred_id]['end']
#             pred_end = datetime.datetime.strptime(pred_end_str, '%Y-%m-%d')
#
#             # Check if task starts before predecessor ends
#             if task_start <= pred_end:
#                 # Fix dates
#                 new_start = pred_end + datetime.timedelta(days=1)
#                 new_start_str = new_start.strftime('%Y-%m-%d')
#                 task_duration = task_map[task_id].get('duration', 1) if task_id in task_map else 1
#                 new_end = new_start + datetime.timedelta(days=task_duration - 1)
#                 new_end_str = new_end.strftime('%Y-%m-%d')
#
#                 # Get task names for logging
#                 task_name = task_map[task_id]['name'] if task_id in task_map else f"Task {task_id}"
#                 pred_name = task_map[pred_id]['name'] if pred_id in task_map else f"Task {pred_id}"
#
#                 print(
#                     f"  ⚠️ Task {task_id} ({task_name}) starts at {task_start_str}, which is before/same day as predecessor {pred_id} ({pred_name}) ends at {pred_end_str}")
#
#                 # Update in task_dates
#                 task_dates[task_id]['start'] = new_start_str
#                 task_dates[task_id]['end'] = new_end_str
#
#                 print(f"  ✅ CORRECTED: Updated task {task_id} dates to {new_start_str} - {new_end_str}")
#
#                 # Update in database
#                 try:
#                     task_manager.db.execute(
#                         "UPDATE tasks SET start_date = ?, end_date = ? WHERE id = ?",
#                         (new_start_str, new_end_str, int(task_id))
#                     )
#                     print(f"  ✅ COMMITTED: Direct database update for task {task_id}")
#                     dependencies_fixed += 1
#                 except Exception as e:
#                     print(f"  ERROR updating database: {str(e)}")
#
#     print(f"Fixed {dependencies_fixed} dependency issues")
#     print("=== END VERIFICATION ===\n")

def simple_final_validation(task_dates, task_map):
    """
    Простая финальная проверка корректности дат без пересчетов

    Args:
        task_dates (dict): Словарь с датами задач
        task_map (dict): Словарь задач по ID

    Returns:
        list: Список предупреждений о найденных проблемах
    """
    warnings = []

    print("Финальная проверка корректности дат...")

    # Проверяем согласованность дат родителей и подзадач
    parent_to_subtasks = {}
    for task_id, task in task_map.items():
        parent_id = task.get('parent_id')
        if parent_id:
            parent_id_str = str(parent_id)
            if parent_id_str not in parent_to_subtasks:
                parent_to_subtasks[parent_id_str] = []
            parent_to_subtasks[parent_id_str].append(str(task_id))

    for parent_id, subtask_ids in parent_to_subtasks.items():
        if parent_id not in task_dates:
            continue

        parent_start = task_dates[parent_id].get('start')
        parent_end = task_dates[parent_id].get('end')

        if not parent_start or not parent_end:
            continue

        try:
            parent_start_date = datetime.datetime.strptime(parent_start, '%Y-%m-%d')
            parent_end_date = datetime.datetime.strptime(parent_end, '%Y-%m-%d')

            for subtask_id in subtask_ids:
                if subtask_id not in task_dates:
                    continue

                subtask_start = task_dates[subtask_id].get('start')
                subtask_end = task_dates[subtask_id].get('end')

                if not subtask_start or not subtask_end:
                    continue

                subtask_start_date = datetime.datetime.strptime(subtask_start, '%Y-%m-%d')
                subtask_end_date = datetime.datetime.strptime(subtask_end, '%Y-%m-%d')

                # Проверяем, что подзадача помещается в рамки родительской задачи
                if subtask_start_date < parent_start_date:
                    warning = f"Подзадача {subtask_id} начинается раньше родительской задачи {parent_id}"
                    warnings.append(warning)
                    print(f"⚠️ {warning}")

                if subtask_end_date > parent_end_date:
                    warning = f"Подзадача {subtask_id} заканчивается позже родительской задачи {parent_id}"
                    warnings.append(warning)
                    print(f"⚠️ {warning}")

        except ValueError as e:
            warning = f"Ошибка при проверке дат для родительской задачи {parent_id}: {str(e)}"
            warnings.append(warning)
            print(f"⚠️ {warning}")

    # Проверяем логичность дат (начало <= конец)
    for task_id, dates in task_dates.items():
        start_date = dates.get('start')
        end_date = dates.get('end')

        if start_date and end_date:
            try:
                start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
                end = datetime.datetime.strptime(end_date, '%Y-%m-%d')

                if start > end:
                    warning = f"Задача {task_id}: дата начала ({start_date}) позже даты окончания ({end_date})"
                    warnings.append(warning)
                    print(f"⚠️ {warning}")

            except ValueError as e:
                warning = f"Ошибка формата дат для задачи {task_id}: {str(e)}"
                warnings.append(warning)
                print(f"⚠️ {warning}")

    if not warnings:
        print("✅ Финальная проверка прошла успешно - проблем не найдено")
    else:
        print(f"⚠️ Найдено {len(warnings)} предупреждений при финальной проверке")

    return warnings