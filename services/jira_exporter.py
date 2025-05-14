import csv
import tempfile
import os
import json
import datetime
import logging
from jira import JIRA
# Настраиваем логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JiraExporter:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()
        self.jira_url = os.getenv("JIRA_URL")
        self.jira_username = os.getenv("JIRA_USERNAME")
        self.jira_api_token = os.getenv("JIRA_API_TOKEN")
        self.jira_project = os.getenv("JIRA_PROJECT", "TEC")
        self.START_DATE_FIELD_ID = 'customfield_10015'
        self.CATEGORY_FIELD_ID = 'customfield_10035'
        self.employee_manager = None

    def export(self, project, tasks):
        """Создает CSV файл для импорта в Jira (резервный вариант)"""
        export_file = os.path.join(self.temp_dir, f"{project['name']}_jira_export.csv")

        # Определяем поля для экспорта
        fieldnames = [
            'Summary', 'Description', 'Issue Type', 'Priority',
            'Assignee', 'Reporter', 'Original Estimate',
            'Due Date', 'Start Date', 'Parent', 'Predecessors', 'Project'
        ]

        # Создаем CSV-файл
        with open(export_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for task in tasks:
                row = {
                    'Summary': task.get('name', ''),
                    'Description': f"Длительность: {task.get('duration', 0)} дн.",
                    'Issue Type': 'Task',
                    'Priority': 'Medium',
                    'Project': self.jira_project
                }
                writer.writerow(row)

        return export_file

    def import_to_jira(self, project, tasks, employee_manager=None):
        """Экспортирует задачи в Jira - с фокусом на создание подзадач"""
        if employee_manager:
            self.employee_manager = employee_manager
        try:
            # Подключаемся к Jira
            jira = JIRA(
                server=self.jira_url,
                basic_auth=(self.jira_username, self.jira_api_token)
            )
            start_date_field_id = self.START_DATE_FIELD_ID
            print(f"Используем поле для даты начала: {start_date_field_id}")
            # Выводим список всех кастомных полей для анализа
            print("Список всех доступных полей:")
            for field in jira.fields():
                if 'customfield_' in field['id']:
                    print(
                        f"ID: {field['id']}, Имя: {field['name']}, Тип: {field.get('schema', {}).get('type', 'unknown')}")
            # Получаем доступные типы связей
            available_link_types = {}
            try:
                for link_type in jira.issue_link_types():
                    # Сохраняем информацию о каждом типе связи
                    available_link_types[link_type.name.lower()] = {
                        'name': link_type.name,  # Сохраняем оригинальное имя с учетом регистра
                        'inward': link_type.inward,
                        'outward': link_type.outward
                    }
                    print(
                        f"Доступный тип связи: {link_type.name} (inward: {link_type.inward}, outward: {link_type.outward})")
            except Exception as e:
                print(f"Ошибка при получении типов связей: {str(e)}")
                available_link_types = {}

            # Выбираем тип связи для зависимостей
            dependency_link_type = None

            # Пробуем найти тип связи "Blocks" с учетом регистра
            if 'blocks' in available_link_types:
                dependency_link_type = available_link_types['blocks']['name']
                print(f"Будем использовать тип связи: {dependency_link_type}")
            elif 'block' in available_link_types:
                dependency_link_type = available_link_types['block']['name']
                print(f"Будем использовать тип связи: {dependency_link_type}")
            else:
                # Если нет "Blocks", используем "Relates"
                dependency_link_type = 'Relates'
                print(f"Тип связи 'Blocks' не найден, используем: {dependency_link_type}")

            print(f"Успешное подключение к Jira API")

            # Получаем доступные типы задач для проекта
            meta = jira.createmeta(
                projectKeys=self.jira_project,
                expand='projects.issuetypes'
            )

            project_meta = meta['projects'][0]
            project_issue_types = project_meta.get('issuetypes', [])

            # Печатаем доступные типы задач
            print(f"Доступные типы задач для проекта {self.jira_project}:")
            for itype in project_issue_types:
                print(f"- {itype['name']} (ID: {itype['id']})")

            # Ищем нужные типы задач
            task_type = None
            subtask_type = None
            epic_type = None

            for itype in project_issue_types:
                if itype['name'] == 'Задача' or itype['name'] == 'Task':
                    task_type = itype
                    print(f"Тип задачи: {task_type['name']} (ID: {task_type['id']})")
                elif itype['name'] == 'Подзадача' or itype['name'] == 'Sub-task':
                    subtask_type = itype
                    print(f"Тип подзадачи: {subtask_type['name']} (ID: {subtask_type['id']})")
                elif itype['name'] == 'Эпик' or itype['name'] == 'Epic':
                    epic_type = itype
                    print(f"Тип эпика: {epic_type['name']} (ID: {epic_type['id']})")

            # Если не нашли нужные типы, используем первый доступный
            if not task_type and project_issue_types:
                task_type = next((t for t in project_issue_types if not t.get('subtask')), project_issue_types[0])
                print(f"Используем тип по умолчанию: {task_type['name']} (ID: {task_type['id']})")

            # Проверка, что у нас есть хотя бы тип задачи
            if not task_type:
                raise ValueError("Не удалось найти подходящий тип задачи в проекте")

            # Создаем главную задачу проекта
            project_name = project.get('name', 'Неизвестный проект')

            main_issue_type = epic_type if epic_type else task_type

            epic_issue = jira.create_issue(
                fields={
                    'project': {'key': self.jira_project},
                    'summary': f"Проект: {project_name}",
                    'description': f"Календарный план проекта '{project_name}'",
                    'issuetype': {'id': main_issue_type['id']}
                }
            )

            print(f"Создана родительская задача проекта: {epic_issue.key}")

            # Словарь для отслеживания созданных задач
            created_issues = [{'key': epic_issue.key, 'name': f"Проект: {project_name}"}]
            task_keys = {}  # id задачи -> ключ в Jira

            # Шаг 1: Идентифицируем все групповые задачи и подзадачи
            group_tasks = {}  # id -> task
            child_tasks = {}  # parent_id -> [tasks]

            for task in tasks:
                # Если это групповая задача
                if task.get('is_group') == 1:
                    try:
                        task_id = int(task['id'])
                    except (ValueError, TypeError):
                        task_id = task['id']  # fallback
                    group_tasks[task_id] = task
                    print(f"Найдена групповая задача: {task['name']} (ID: {task_id})")

                parent_id_raw = task.get('parent_id')
                try:
                    parent_id = int(parent_id_raw) if parent_id_raw is not None else None
                except (ValueError, TypeError):
                    parent_id = None
                print(
                    f"[DEBUG] Обработка задачи id={task['id']}, name={task['name']}, parent_id_raw={task.get('parent_id')}")

                # Если это подзадача (имеет parent_id)
                if parent_id is not None:
                    if parent_id not in child_tasks:
                        child_tasks[parent_id] = []
                    child_tasks[parent_id].append(task)
                    print(f"Найдена подзадача: {task['name']} для родителя ID={parent_id}")

            # Проверяем, что нашли подзадачи
            print(
                f"Всего найдено {len(group_tasks)} групповых задач и {sum(len(tasks) for tasks in child_tasks.values())} подзадач")
            # Шаг 2: Создаем групповые задачи и их подзадачи
            for task_id, task in group_tasks.items():
                try:
                    # Определяем категорию для каждой задачи индивидуально
                    category_value = None
                    if self.employee_manager and task.get('position'):
                        category = self.employee_manager.get_category_by_position(task.get('position'))
                        if category:
                            category_value = {"value": category}

                    # Определяем исполнителя
                    assignee = self._get_assignee_for_task(jira, task_id, task.get('employee_id'))

                    # Создаем групповую задачу
                    fields={
                        'project': {'key': self.jira_project},
                        'summary': task['name'],
                        'description': f"Длительность: {task.get('duration', 0)} дн.",
                        'issuetype': {'id': task_type['id']},
                        'duedate': task.get('end_date') if task.get('end_date') else None,
                        start_date_field_id: task.get('start_date') if task.get('start_date') else None,
                        self.CATEGORY_FIELD_ID: category_value
                    }

                    if assignee:
                        fields['assignee'] = assignee

                    task_issue = jira.create_issue(fields=fields)

                    task_keys[task_id] = task_issue.key
                    created_issues.append({'key': task_issue.key, 'name': task['name']})
                    print(f"Создана групповая задача: {task_issue.key} - {task['name']}")

                    # Если у этой групповой задачи есть подзадачи, создаем их
                    if task_id in child_tasks and child_tasks[task_id]:
                        print(f"У задачи {task['name']} (ID={task_id}) найдено {len(child_tasks[task_id])} подзадач")

                        for subtask in child_tasks[task_id]:

                            # Определяем категорию для подзадачи
                            subtask_category_value = None
                            if self.employee_manager and subtask.get('position'):
                                subtask_category = self.employee_manager.get_category_by_position(
                                    subtask.get('position'))
                                if subtask_category:
                                    subtask_category_value = {"value": subtask_category}

                            subtask_assignee = self._get_assignee_for_task(jira, subtask['id'],
                                                                           subtask.get('employee_id'))
                            # Проверка типа подзадачи и правильное создание подзадачи
                            if subtask_type:
                                try:
                                    # Создаем подзадачу с правильным типом
                                    subtask_fields = {
                                        'project': {'key': self.jira_project},
                                        'summary': subtask['name'],
                                        'description': f"Длительность: {subtask.get('duration', 0)} дн.\nДолжность: {subtask.get('position', 'Не указана')}",
                                        'issuetype': {'id': subtask_type['id']},
                                        'parent': {'key': task_issue.key},
                                        'duedate': subtask.get('end_date') if subtask.get('end_date') else None,
                                        # Добавляем дату начала
                                        start_date_field_id: subtask.get('start_date') if subtask.get(
                                            'start_date') else None,
                                        self.CATEGORY_FIELD_ID: subtask_category_value
                                    }
                                    if subtask_assignee:
                                        subtask_fields['assignee'] = subtask_assignee
                                    subtask_issue = jira.create_issue(fields=subtask_fields)

                                    task_keys[subtask['id']] = subtask_issue.key
                                    created_issues.append({'key': subtask_issue.key, 'name': subtask['name']})
                                    print(f"  Создана подзадача: {subtask_issue.key} - {subtask['name']}")
                                except Exception as e:
                                    print(f"  Ошибка при создании подзадачи {subtask['name']}: {str(e)}")

                                    # План Б: создаем обычную задачу
                                    try:
                                        subtask_task = jira.create_issue(
                                            fields={
                                                'project': {'key': self.jira_project},
                                                'summary': f"{task['name']} - {subtask['name']}",
                                                'description': f"Длительность: {subtask.get('duration', 0)} дн.\nДолжность: {subtask.get('position', 'Не указана')}",
                                                'issuetype': {'id': task_type['id']},
                                                'duedate': subtask.get('end_date') if subtask.get('end_date') else None,
                                                # Добавляем дату начала
                                                start_date_field_id: subtask.get('start_date') if subtask.get(
                                                    'start_date') else None
                                            }
                                        )

                                        # Связываем с родительской задачей
                                        jira.create_issue_link(
                                            type='Relates',
                                            inwardIssue=subtask_task.key,
                                            outwardIssue=task_issue.key
                                        )

                                        task_keys[subtask['id']] = subtask_task.key
                                        created_issues.append(
                                            {'key': subtask_task.key, 'name': f"{task['name']} - {subtask['name']}"})
                                        print(f"  Создана обычная задача вместо подзадачи: {subtask_task.key}")
                                    except Exception as e2:
                                        print(f"  Не удалось создать даже обычную задачу: {str(e2)}")
                            else:
                                # Если тип подзадачи недоступен, создаем обычную задачу
                                try:
                                    subtask_task = jira.create_issue(
                                        fields={
                                            'project': {'key': self.jira_project},
                                            'summary': f"{task['name']} - {subtask['name']}",
                                            'description': f"Длительность: {subtask.get('duration', 0)} дн.\nДолжность: {subtask.get('position', 'Не указана')}",
                                            'issuetype': {'id': task_type['id']}
                                        }
                                    )

                                    # Связываем с родительской задачей
                                    jira.create_issue_link(
                                        type='Relates',
                                        inwardIssue=subtask_task.key,
                                        outwardIssue=task_issue.key
                                    )

                                    task_keys[subtask['id']] = subtask_task.key
                                    created_issues.append(
                                        {'key': subtask_task.key, 'name': f"{task['name']} - {subtask['name']}"})
                                    print(f"  Создана связанная задача (нет типа подзадачи): {subtask_task.key}")
                                except Exception as e:
                                    print(f"  Ошибка при создании связанной задачи: {str(e)}")
                    else:
                        print(f"У задачи {task['name']} (ID={task_id}) нет подзадач")
                except Exception as e:
                    print(f"Ошибка при создании групповой задачи {task['name']}: {str(e)}")

            # Шаг 3: Создаем обычные задачи (не групповые и не подзадачи)
            for task in tasks:
                # Пропускаем групповые задачи (они уже созданы)
                if task['id'] in group_tasks:
                    continue

                # Пропускаем подзадачи (они уже созданы)
                parent_id = task.get('parent_id')
                if parent_id and parent_id in group_tasks:
                    continue

                category_value = None
                if self.employee_manager and task.get('position'):
                    category = self.employee_manager.get_category_by_position(task.get('position'))
                    if category:
                        category_value = {"value": category}

                task_assignee = self._get_assignee_for_task(jira, task['id'], task.get('employee_id'))

                # Создаем обычную задачу
                try:
                    fields={
                        'project': {'key': self.jira_project},
                        'summary': task['name'],
                        'description': f"Длительность: {task.get('duration', 0)} дн.\nДолжность: {task.get('position', 'Не указана')}",
                        'issuetype': {'id': task_type['id']},
                        'duedate': task.get('end_date') if task.get('end_date') else None,
                        start_date_field_id: task.get('start_date') if task.get('start_date') else None,
                        self.CATEGORY_FIELD_ID: category_value
                    }

                    # Добавляем исполнителя, если найден
                    if task_assignee:
                        fields['assignee'] = task_assignee

                    task_issue = jira.create_issue(fields=fields)

                    # Связываем с эпиком
                    jira.create_issue_link(
                        type='Relates',
                        inwardIssue=task_issue.key,
                        outwardIssue=epic_issue.key
                    )

                    task_keys[task['id']] = task_issue.key
                    created_issues.append({'key': task_issue.key, 'name': task['name']})
                    print(f"Создана задача: {task_issue.key} - {task['name']}")
                except Exception as e:
                    print(f"Ошибка при создании задачи {task['name']}: {str(e)}")

            # Шаг 4: Создаем зависимости между задачами
            print("Создание зависимостей между задачами")
            for task in tasks:
                # Получаем предшественников
                predecessors_str = task.get('predecessors')
                if not predecessors_str or task['id'] not in task_keys:
                    continue

                task_key = task_keys[task['id']]
                task_name = task['name']

                # Парсим предшественников
                predecessors = []
                try:
                    if isinstance(predecessors_str, str):
                        if predecessors_str.strip() == "NULL" or not predecessors_str.strip():
                            continue

                        # Пытаемся распарсить строку
                        if predecessors_str.strip().startswith('[') and predecessors_str.strip().endswith(']'):
                            pred_str = predecessors_str.strip().strip('[]')
                            predecessors = [int(p.strip()) for p in pred_str.split(',') if p.strip()]
                        else:
                            predecessors = [int(predecessors_str.strip())]
                    elif isinstance(predecessors_str, list):
                        predecessors = predecessors_str
                except Exception as e:
                    print(f"Ошибка при парсинге предшественников '{predecessors_str}': {str(e)}")

                # Создаем связи
                for pred_id in predecessors:
                    if pred_id in task_keys:
                        pred_key = task_keys[pred_id]
                        pred_task = next((t for t in tasks if t['id'] == pred_id), None)
                        pred_name = pred_task['name'] if pred_task else f"Задача {pred_id}"

                        try:
                            if dependency_link_type == 'Blocks':
                                # Для типа Blocks используем правильное направление:
                                # Предшественник (pred_key) БЛОКИРУЕТ текущую задачу (task_key)
                                jira.create_issue_link(
                                    type=dependency_link_type,
                                    outwardIssue=task_key,  # Кто блокирует
                                    inwardIssue=pred_key  # Кого блокируют
                                )
                                print(f"Создана связь: '{pred_name}' блокирует '{task_name}'")
                            else:
                                # Для других типов используем обычную связь
                                jira.create_issue_link(
                                    type=dependency_link_type,
                                    inwardIssue=task_key,
                                    outwardIssue=pred_key
                                )
                                print(f"Создана связь '{dependency_link_type}' между '{task_name}' и '{pred_name}'")
                        except Exception as e:
                            print(f"Ошибка при создании связи: {str(e)}")
                            try:
                                jira.create_issue_link(
                                    type='Relates',
                                    inwardIssue=task_key,
                                    outwardIssue=pred_key
                                )
                                print(f"Создана альтернативная связь 'Relates' между '{task_name}' и '{pred_name}'")
                            except Exception as e2:
                                print(f"Не удалось создать даже связь 'Relates': {str(e2)}")
            try:
                print(f"Добавляем задачи в эпик {epic_issue.key} через поле parent...")

                for task_id, jira_key in task_keys.items():
                    # Проверяем, что это не сам эпик
                    if jira_key != epic_issue.key:
                        try:
                            # Получаем задачу
                            issue = jira.issue(jira_key)

                            # Обновляем задачу, указывая parent
                            issue.update(fields={"parent": {"key": epic_issue.key}})
                            print(f"Успешно добавлена задача {jira_key} в эпик {epic_issue.key}")
                        except Exception as e_inner:
                            print(f"Не удалось добавить задачу {jira_key} в эпик: {str(e_inner)}")
            except Exception as e:
                print(f"Ошибка при добавлении задач в эпик: {str(e)}")
                print("Задачи будут связаны с эпиком через обычные связи.")
            return {
                'success': True,
                'epic_key': epic_issue.key,
                'created_issues': created_issues,
                'count': len(created_issues),
                'jira_project_url': f"{self.jira_url}/projects/{self.jira_project}"
            }

        except Exception as e:
            import traceback
            traceback.print_exc()

            print(f"Критическая ошибка при экспорте в Jira: {str(e)}")

            # Создаем CSV-файл как резервный вариант
            csv_export_file = self.export(project, tasks)

            return {
                'success': False,
                'message': f"Ошибка при экспорте в Jira: {str(e)}. Создан CSV-файл для ручного импорта.",
                'csv_export_file': csv_export_file,
                'error': str(e)
            }

    def _find_jira_user_by_name(self, jira, employee_name):
        """
        Находит пользователя Jira по имени сотрудника с учетом GDPR-режима

        Args:
            jira: JIRA client
            employee_name (str): Имя сотрудника (например, "Иванов И.И.")

        Returns:
            dict: Словарь с данными исполнителя для Jira или None
        """
        if not employee_name:
            return None

        try:
            # Извлекаем фамилию для поиска
            last_name = employee_name.split()[0] if " " in employee_name else employee_name

            # Для GDPR-совместимого API Jira Cloud
            try:
                # Попытка поиска через query вместо username
                users = jira.search_users(query=last_name, maxResults=10)

                if not users:
                    # Альтернативный поиск через picker API
                    users = jira.search_users_for_picker(query=last_name, maxResults=10)

                if users:
                    print(
                        f"Найден пользователь в Jira для {employee_name}: {users[0].displayName if hasattr(users[0], 'displayName') else users[0]}")
                    return {"accountId": users[0].accountId}
                else:
                    print(f"Пользователь с именем {employee_name} не найден в Jira")
            except Exception as picker_error:
                print(f"Ошибка при поиске пользователя через GDPR-совместимые методы: {str(picker_error)}")

                # Если все методы не сработали, можно использовать жестко закодированное сопоставление
                employee_map = {
                    "Иванов И.И.": "637f0d8ae7fb394fe88d67e7",  # accountId пользователя в Jira
                    "Петров П.П.": "64b0e4fbe7fb394fe88d67e8",
                    "Сидоров С.С.": "64b0e4fbe7fb394fe88d67e9",
                    # Добавьте других сотрудников по мере необходимости
                }

                if employee_name in employee_map:
                    print(f"Использую жестко закодированное сопоставление для {employee_name}")
                    return {"accountId": employee_map[employee_name]}

        except Exception as e:
            print(f"Критическая ошибка при поиске пользователя: {str(e)}")

        return None

    def _get_assignee_for_task(self, jira, task_id, employee_id):
        """
        Возвращает данные исполнителя для задачи

        Args:
            jira: JIRA client
            task_id: ID задачи
            employee_id: ID сотрудника

        Returns:
            dict: Словарь с данными исполнителя для Jira или None
        """
        if not employee_id:
            return None

        if not self.employee_manager:
            return None

        try:
            # Получаем информацию о сотруднике
            employee = self.employee_manager.get_employee(employee_id)

            if employee and 'name' in employee:
                # Ищем пользователя Jira по имени сотрудника
                return self._find_jira_user_by_name(jira, employee['name'])
        except Exception as e:
            print(f"Ошибка при получении исполнителя для задачи {task_id}: {str(e)}")

        return None
