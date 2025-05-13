import csv
import tempfile
import os
import json
import datetime


class JiraExporter:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()

    def export(self, project, tasks):
        """
        Экспортирует проект в формат CSV для импорта в Jira

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