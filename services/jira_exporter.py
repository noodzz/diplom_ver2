import csv
import tempfile
import os
import json
import datetime

from jira import JIRA


class JiraExporter:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()
        self.jira_url = os.getenv("JIRA_URL")
        self.jira_username = os.getenv("JIRA_USERNAME")
        self.jira_api_token = os.getenv("JIRA_API_TOKEN")
        self.jira_project = os.getenv("JIRA_PROJECT", "TEC")  # Project key, по умолчанию TEC

        # Жестко закодированные ID типов задач для проекта
        self.task_type_id = "10001"  # ID для типа "Задача"
        self.subtask_type_id = "10002"  # ID для типа "Подзадача"
        self.epic_type_id = "10006"  # ID для типа "Эпик"

    def export(self, project, tasks):
        """
        Создает CSV файл для импорта в Jira (старая функциональность)

        Args:
            project (dict): Информация о проекте
            tasks (list): Список задач проекта

        Returns:
            str: Путь к созданному файлу экспорта
        """
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

            # Создаем словарь для отслеживания идентификаторов задач в Jira
            jira_keys = {}

            # Используем существующий проект TEC вместо создания нового эпика
            project_key = "TEC"

            # Добавляем задачи
            for task in tasks:
                # Если это групповая задача, добавляем ее как историю
                if task['is_group']:
                    task_row = {
                        'Summary': task['name'],
                        'Description': f"Длительность: {task['duration']} дн.",
                        'Issue Type': 'Story',
                        'Priority': 'Medium',
                        'Assignee': '',
                        'Reporter': '',
                        'Original Estimate': f"{task['duration']}d",
                        'Due Date': task.get('end_date', ''),
                        'Start Date': task.get('start_date', ''),
                        'Parent': '',  # Не указываем родителя, так как задачи будут в проекте TEC
                        'Predecessors': self._get_predecessors_keys(task, jira_keys),
                        'Project': project_key
                    }
                    writer.writerow(task_row)

                    # Запоминаем ключ задачи в Jira
                    jira_keys[task['id']] = task['name']

                    # Добавляем подзадачи
                    subtasks = self._get_subtasks(task['id'], tasks)
                    for subtask in subtasks:
                        subtask_row = {
                            'Summary': subtask['name'],
                            'Description': f"Длительность: {subtask['duration']} дн.\nДолжность: {subtask['position']}",
                            'Issue Type': 'Sub-task',
                            'Priority': 'Medium',
                            'Assignee': self._get_employee_name(subtask),
                            'Reporter': '',
                            'Original Estimate': f"{subtask['duration']}d",
                            'Due Date': subtask.get('end_date', ''),
                            'Start Date': subtask.get('start_date', ''),
                            'Parent': task['name'],  # Указываем родительскую задачу для подзадачи
                            'Predecessors': '',
                            'Project': project_key
                        }
                        writer.writerow(subtask_row)

                        # Запоминаем ключ подзадачи в Jira
                        jira_keys[subtask['id']] = subtask['name']
                else:
                    # Обычная задача - добавляем как задачу
                    task_row = {
                        'Summary': task['name'],
                        'Description': f"Длительность: {task['duration']} дн.\nДолжность: {task['position']}",
                        'Issue Type': 'Task',
                        'Priority': 'Medium',
                        'Assignee': self._get_employee_name(task),
                        'Reporter': '',
                        'Original Estimate': f"{task['duration']}d",
                        'Due Date': task.get('end_date', ''),
                        'Start Date': task.get('start_date', ''),
                        'Parent': '',  # Не указываем родителя, так как задачи будут в проекте TEC
                        'Predecessors': self._get_predecessors_keys(task, jira_keys),
                        'Project': project_key
                    }
                    writer.writerow(task_row)

                    # Запоминаем ключ задачи в Jira
                    jira_keys[task['id']] = task['name']

        return export_file

    def import_to_jira(self, project, tasks):
        """Экспортирует задачи в Jira согласно проверенному подходу"""
        try:
            # Подключаемся к Jira
            jira = JIRA(
                server=self.jira_url,
                basic_auth=(self.jira_username, self.jira_api_token)
            )

            # Проверяем подключение
            myself = jira.myself()
            print(f"Успешное подключение к Jira как: {myself['displayName']}")

            # Получаем доступные типы связей для диагностики
            link_types = jira.issue_link_types()
            print("Доступные типы связей в Jira:")
            for link_type in link_types:
                print(f"- {link_type.name}: inward={link_type.inward}, outward={link_type.outward}")

            # Выбираем подходящий тип связи
            blocks_link_type = None
            for link_type in link_types:
                if ('блок' in link_type.outward.lower() or 'block' in link_type.outward.lower()):
                    blocks_link_type = link_type.name
                    print(f"Найден тип связи для блокировки: {blocks_link_type}")
                    break

            # Если специальный тип не найден, используем 'Relates'
            link_type_name = blocks_link_type or 'Relates'
            print(f"Будет использоваться тип связи: {link_type_name}")

            # Создаем эпик для проекта
            project_name = project.get('name', 'Неизвестный проект')
            epic_fields = {
                'project': {'key': self.jira_project},
                'summary': f"Проект: {project_name}",
                'description': f"Календарный план проекта '{project_name}'",
                'issuetype': {'id': self.epic_type_id}
            }

            epic = jira.create_issue(fields=epic_fields)
            print(f"Создан эпик проекта: {epic.key}")

            # Словарь для отслеживания созданных задач
            created_issues = [{'key': epic.key, 'name': f"Проект: {project_name}"}]
            task_keys = {}  # id задачи -> ключ в Jira

            # АНАЛИЗ СТРУКТУРЫ: Выявление групповых задач и подзадач
            group_tasks = {}  # id групповой задачи -> задача
            child_tasks = {}  # id подзадачи -> родительская задача id

            print("Анализ структуры задач и зависимостей:")
            for task in tasks:
                # Если это групповая задача
                if task.get('is_group'):
                    group_tasks[task['id']] = task
                    print(f"Найдена групповая задача: {task['name']} (ID: {task['id']})")

            # Находим все подзадачи
            for task in tasks:
                parent_id = task.get('parent_id')
                if parent_id:
                    child_tasks[task['id']] = parent_id
                    parent_task = next((t for t in tasks if t['id'] == parent_id), None)
                    if parent_task:
                        print(
                            f"Подзадача: {task['name']} (ID: {task['id']}) для родителя: {parent_task['name']} (ID: {parent_id})")
                    else:
                        print(
                            f"Подзадача: {task['name']} (ID: {task['id']}) имеет несуществующего родителя: {parent_id}")

            # Создаем обычные и групповые задачи
            print("\nСоздание задач:")
            for task in tasks:
                # Пропускаем подзадачи - они будут созданы с родительскими задачами
                if task['id'] in child_tasks:
                    continue

                task_id = task['id']
                task_name = task['name']
                task_duration = task.get('duration', 0)
                task_description = f"Длительность: {task_duration} дн."

                # Все задачи создаем как обычные задачи (Task), не как эпики
                task_fields = {
                    'project': {'key': self.jira_project},
                    'summary': task_name,
                    'description': task_description,
                    'issuetype': {'id': self.task_type_id}  # Всегда создаем как Task, не Epic
                }

                try:
                    issue = jira.create_issue(fields=task_fields)
                    task_keys[task_id] = issue.key
                    created_issues.append({'key': issue.key, 'name': task_name})
                    print(f"Создана задача: {issue.key} - {task_name}")

                    # Связываем с эпиком проекта
                    jira.create_issue_link(
                        type='Relates',
                        inwardIssue=issue.key,
                        outwardIssue=epic.key
                    )

                    # Если это групповая задача, сразу создаем подзадачи
                    if task['id'] in group_tasks:
                        # Найдем все подзадачи для этой групповой задачи
                        subtasks = [t for t in tasks if t.get('parent_id') == task['id']]
                        print(f"  У групповой задачи {task_name} (ID: {task['id']}) найдено {len(subtasks)} подзадач")

                        for subtask in subtasks:
                            subtask_id = subtask['id']
                            subtask_name = subtask['name']
                            subtask_duration = subtask.get('duration', 0)
                            subtask_description = f"Длительность: {subtask_duration} дн."

                            # ВАЖНО: Создаем подзадачу с правильными параметрами
                            subtask_fields = {
                                'project': {'key': self.jira_project},
                                'summary': subtask_name,
                                'description': subtask_description,
                                'issuetype': {'id': self.subtask_type_id},  # ID для Подзадачи
                                'parent': {'key': issue.key}  # Указываем родительскую задачу
                            }

                            try:
                                sub_issue = jira.create_issue(fields=subtask_fields)
                                task_keys[subtask_id] = sub_issue.key
                                created_issues.append({'key': sub_issue.key, 'name': subtask_name})
                                print(f"    Создана подзадача: {sub_issue.key} - {subtask_name}")
                            except Exception as e:
                                print(f"    Ошибка при создании подзадачи {subtask_name}: {str(e)}")
                                # Если не удалось создать подзадачу, создаем обычную задачу
                                try:
                                    alt_fields = {
                                        'project': {'key': self.jira_project},
                                        'summary': f"{task_name} - {subtask_name}",
                                        'description': subtask_description,
                                        'issuetype': {'id': self.task_type_id}  # Обычная задача
                                    }
                                    alt_issue = jira.create_issue(fields=alt_fields)
                                    task_keys[subtask_id] = alt_issue.key
                                    created_issues.append(
                                        {'key': alt_issue.key, 'name': f"{task_name} - {subtask_name}"})

                                    # Связываем с родительской задачей
                                    jira.create_issue_link(
                                        type='Relates',
                                        inwardIssue=alt_issue.key,
                                        outwardIssue=issue.key
                                    )
                                    print(
                                        f"    Создана альтернативная задача: {alt_issue.key} - {task_name} - {subtask_name}")
                                except Exception as e2:
                                    print(f"    Не удалось создать альтернативную задачу: {str(e2)}")

                except Exception as e:
                    print(f"Ошибка при создании задачи {task_name}: {str(e)}")

            # Создаем зависимости между задачами
            print("\nСоздание зависимостей между задачами:")
            for task in tasks:
                if 'predecessors' not in task or not task['predecessors'] or task['id'] not in task_keys:
                    continue

                task_key = task_keys[task['id']]
                task_name = task['name']

                for pred_id in task['predecessors']:
                    if pred_id in task_keys:
                        pred_key = task_keys[pred_id]
                        pred_task = next((t for t in tasks if t['id'] == pred_id), None)
                        pred_name = pred_task['name'] if pred_task else f"Задача ID {pred_id}"

                        try:
                            # ВАЖНО: Создаем правильную связь
                            # Предшественник (pred_key) блокирует текущую задачу (task_key)
                            jira.create_issue_link(
                                type=link_type_name,
                                outwardIssue=pred_key,  # Задача-предшественник блокирует
                                inwardIssue=task_key  # Текущая задача блокируется
                            )
                            print(f"Создана связь: '{pred_name}' блокирует '{task_name}'")
                        except Exception as e:
                            print(f"Ошибка при создании связи между '{pred_name}' и '{task_name}': {str(e)}")
                            # Пробуем создать связь типа Relates
                            try:
                                jira.create_issue_link(
                                    type='Relates',
                                    inwardIssue=task_key,
                                    outwardIssue=pred_key
                                )
                                print(f"Создана связь типа 'Relates' между '{task_name}' и '{pred_name}'")
                            except Exception as e2:
                                print(f"Не удалось создать даже связь типа 'Relates': {str(e2)}")

            return {
                'success': True,
                'epic_key': epic.key,
                'created_issues': created_issues,
                'count': len(created_issues),
                'jira_project_url': f"{self.jira_url}/projects/{self.jira_project}"
            }

        except Exception as e:
            print(f"Критическая ошибка при экспорте в Jira: {str(e)}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}

    def _create_epic(self, jira, project):
        """Создает эпик для проекта"""
        try:
            # Безопасное получение данных проекта с использованием .get()
            project_name = project.get('name', 'Неизвестный проект')
            project_start = project.get('start_date', 'Не указано')
            project_status = project.get('status', 'Не указано')

            description = f"Проект: {project_name}\n"
            if project_start != 'Не указано':
                description += f"Дата начала: {project_start}\n"
            if project_status != 'Не указано':
                description += f"Статус: {project_status}\n"

            epic_dict = {
                'project': {'key': self.jira_project},
                'summary': f"Проект: {project_name}",
                'description': description,
                'issuetype': {'name': 'Эпик'}  # Используем русское название
            }

            epic = jira.create_issue(fields=epic_dict)
            return epic.key

        except Exception as e:
            print(f"Ошибка при создании эпика: {str(e)}")
            # В случае ошибки создаем простую задачу
            try:
                task_dict = {
                    'project': {'key': self.jira_project},
                    'summary': f"Проект: {project.get('name', 'Неизвестный проект')}",
                    'description': "Главная задача проекта",
                    'issuetype': {'name': 'Задача'}  # Используем русское название
                }
                task = jira.create_issue(fields=task_dict)
                return task.key
            except Exception as task_error:
                print(f"Ошибка при создании задачи: {str(task_error)}")
                return None

    def _create_task(self, jira, task, parent_key=None):
        """Создает задачу в Jira"""
        try:
            # Безопасное получение данных
            task_name = task.get('name', 'Неизвестная задача')
            task_duration = task.get('duration', 'Не указано')
            task_position = task.get('position', 'Не указана')

            description = f"Длительность: {task_duration} дн.\n"
            if task_position != 'Не указана':
                description += f"Должность: {task_position}\n"

            issue_dict = {
                'project': {'key': self.jira_project},
                'summary': task_name,
                'description': description,
                'issuetype': {'name': 'Задача'}  # Используем русское название
            }

            issue = jira.create_issue(fields=issue_dict)

            # Если есть родительская задача, создаем ссылку
            if parent_key:
                jira.create_issue_link(
                    type='Relates',
                    inwardIssue=issue.key,
                    outwardIssue=parent_key
                )

            return issue

        except Exception as e:
            print(f"Ошибка при создании задачи '{task.get('name', 'Неизвестная')}': {str(e)}")
            return None

    def _create_subtask(self, jira, task, parent_key):
        """Создает подзадачу в Jira"""
        try:
            # Безопасное получение данных
            task_name = task.get('name', 'Неизвестная подзадача')
            task_duration = task.get('duration', 'Не указано')
            task_position = task.get('position', 'Не указана')

            description = f"Длительность: {task_duration} дн.\n"
            if task_position != 'Не указана':
                description += f"Должность: {task_position}\n"

            issue_dict = {
                'project': {'key': self.jira_project},
                'summary': task_name,
                'description': description,
                'issuetype': {'name': 'Подзадача'},  # Используем русское название
                'parent': {'key': parent_key}
            }

            issue = jira.create_issue(fields=issue_dict)
            return issue

        except Exception as e:
            print(f"Ошибка при создании подзадачи '{task.get('name', 'Неизвестная')}': {str(e)}")
            # Если не удалось создать подзадачу, создаем обычную задачу
            try:
                return self._create_task(jira, task, parent_key)
            except:
                return None

    def _set_dependencies(self, jira, tasks, task_to_issue):
        """Устанавливает зависимости между задачами"""
        for task in tasks:
            if task.get('predecessors') and task['id'] in task_to_issue:
                issue_key = task_to_issue[task['id']]

                for pred_id in task['predecessors']:
                    if pred_id in task_to_issue:
                        pred_key = task_to_issue[pred_id]
                        # Создаем связь "блокируется" (Blocked by)
                        jira.create_issue_link(
                            type='Blocks',
                            inwardIssue=issue_key,
                            outwardIssue=pred_key
                        )

    def _get_subtasks(self, parent_id, all_tasks):
        """
        Возвращает список подзадач для групповой задачи

        Args:
            parent_id (int): Идентификатор родительской задачи
            all_tasks (list): Список всех задач проекта

        Returns:
            list: Список подзадач
        """
        subtasks = []
        for task in all_tasks:
            if task.get('parent_id') == parent_id:
                subtasks.append(task)
        return subtasks

    def _get_predecessors_keys(self, task, jira_keys):
        """
        Возвращает строку с ключами задач-предшественников для Jira

        Args:
            task (dict): Информация о задаче
            jira_keys (dict): Словарь соответствия ID задач и их ключей в Jira

        Returns:
            str: Строка с ключами предшественников
        """
        if 'predecessors' not in task or not task['predecessors']:
            return ''

        predecessors = []
        for pred_id in task['predecessors']:
            if pred_id in jira_keys:
                predecessors.append(jira_keys[pred_id])

        return ', '.join(predecessors)

    def _get_employee_name(self, task):
        """
        Возвращает имя сотрудника, назначенного на задачу

        Args:
            task (dict): Информация о задаче

        Returns:
            str: Имя сотрудника или пустая строка
        """
        if 'employee_name' in task and task['employee_name']:
            return task['employee_name']

        if 'employee_id' in task and task['employee_id']:
            # В реальном проекте здесь нужно получить имя сотрудника из базы данных
            return f"Сотрудник #{task['employee_id']}"

        return ''