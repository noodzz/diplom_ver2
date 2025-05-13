import datetime
import json

from data.config import Config


class ProjectManager:
    def __init__(self, db_manager):
        self.db = db_manager

    def create_empty(self, name, start_date):
        """Создает пустой проект"""
        try:
            # Валидация даты
            datetime.datetime.strptime(start_date, '%Y-%m-%d')

            # Создаем проект
            project_id = self.db.create_project(name, start_date)
            return project_id
        except ValueError:
            raise ValueError("Некорректный формат даты. Используйте YYYY-MM-DD")

    def create_from_template(self, name, start_date, template_id):
        """Создает проект из шаблона"""
        try:
            # Валидация даты
            datetime.datetime.strptime(start_date, '%Y-%m-%d')

            # Проверяем существование шаблона
            if template_id not in Config.PROJECT_TEMPLATES:
                raise ValueError(f"Шаблон с ID {template_id} не найден")

            template = Config.PROJECT_TEMPLATES[template_id]

            # Создаем проект
            project_id = self.db.create_project(name, start_date)

            # Создаем задачи из шаблона
            task_mapping = {}  # Для сопоставления имен задач с их ID

            # Сначала создаем все задачи без зависимостей
            for task_data in template["tasks"]:
                is_group = task_data.get("is_group", False)

                task_id = self.db.create_task(
                    project_id=project_id,
                    name=task_data["name"],
                    duration=task_data["duration"],
                    is_group=is_group,
                    position=task_data.get("position")
                )

                task_mapping[task_data["name"]] = task_id

                # Если это групповая задача, создаем подзадачи
                if is_group and "subtasks" in task_data:
                    for subtask in task_data["subtasks"]:
                        self.db.create_task(
                            project_id=project_id,
                            parent_id=task_id,
                            name=subtask["name"],
                            duration=subtask["duration"],
                            position=subtask["position"],
                            parallel=subtask.get("parallel", False)
                        )

            # Затем устанавливаем зависимости
            for task_data in template["tasks"]:
                if "predecessors" in task_data and task_data["predecessors"]:
                    task_id = task_mapping[task_data["name"]]

                    # Получаем текущую задачу
                    task = self.db.get_task(task_id)
                    if task:
                        # Создаем список ID предшественников
                        predecessors = []
                        for predecessor_name in task_data["predecessors"]:
                            if predecessor_name in task_mapping:
                                predecessor_id = task_mapping[predecessor_name]
                                predecessors.append(predecessor_id)
                                # Добавляем зависимость в базу данных
                                self.db.add_dependency(task_id, predecessor_id)

                        # Обновляем задачу в базе с информацией о предшественниках
                        self.db.execute(
                            "UPDATE tasks SET predecessors = ? WHERE id = ?",
                            (json.dumps(predecessors), task_id)
                        )

            return project_id

        except ValueError as e:
            raise ValueError(f"Ошибка при создании проекта из шаблона: {str(e)}")

    def create_from_csv(self, name, start_date, csv_data):
        """Создает проект из данных CSV"""
        try:
            # Валидация даты
            datetime.datetime.strptime(start_date, '%Y-%m-%d')

            # Создаем проект
            project_id = self.db.create_project(name, start_date)

            # Создаем задачи из CSV
            task_mapping = {}  # Для сопоставления имен задач с их ID

            # Сначала создаем все задачи без зависимостей
            for task_data in csv_data:
                is_group = task_data.get("is_group", False)

                task_id = self.db.create_task(
                    project_id=project_id,
                    name=task_data["name"],
                    duration=task_data["duration"],
                    is_group=is_group,
                    position=task_data.get("position")
                )

                task_mapping[task_data["name"]] = task_id

                # Если это групповая задача, создаем подзадачи
                if is_group and "subtasks" in task_data:
                    for subtask in task_data["subtasks"]:
                        self.db.create_task(
                            project_id=project_id,
                            parent_id=task_id,
                            name=subtask["name"],
                            duration=subtask["duration"],
                            position=subtask["position"],
                            parallel=subtask.get("parallel", False)
                        )

            # Затем устанавливаем зависимости
            for task_data in csv_data:
                if "predecessors" in task_data and task_data["predecessors"]:
                    task_id = task_mapping[task_data["name"]]

                    # Получаем текущую задачу
                    task = self.db.get_task(task_id)
                    if task:
                        # Создаем список ID предшественников
                        predecessors = []
                        for predecessor_name in task_data["predecessors"]:
                            if predecessor_name in task_mapping:
                                predecessor_id = task_mapping[predecessor_name]
                                predecessors.append(predecessor_id)
                                # Добавляем зависимость в базу данных
                                self.db.add_dependency(task_id, predecessor_id)

                        # Обновляем задачу в базе с информацией о предшественниках
                        self.db.execute(
                            "UPDATE tasks SET predecessors = ? WHERE id = ?",
                            (json.dumps(predecessors), task_id)
                        )

            return project_id

        except ValueError as e:
            raise ValueError(f"Ошибка при создании проекта из CSV: {str(e)}")

    def get_all_projects(self):
        """Возвращает список всех проектов"""
        projects = self.db.get_projects()
        return [dict(project) for project in projects]

    def get_project_details(self, project_id):
        """Возвращает детальную информацию о проекте"""
        project = self.db.get_project(project_id)
        if not project:
            raise ValueError(f"Проект с ID {project_id} не найден")

        return dict(project)

    def get_templates(self):
        """Возвращает список доступных шаблонов"""
        templates = []
        for template_id, template_data in Config.PROJECT_TEMPLATES.items():
            templates.append({
                "id": template_id,
                "name": template_data["name"]
            })
        return templates

    def add_task(self, project_id, task_data):
        """Добавляет задачу в проект"""
        # Проверяем существование проекта
        project = self.db.get_project(project_id)
        if not project:
            raise ValueError(f"Проект с ID {project_id} не найден")

        # Создаем задачу
        is_group = task_data.get("is_group", False)

        task_id = self.db.create_task(
            project_id=project_id,
            name=task_data["name"],
            duration=task_data["duration"],
            is_group=is_group,
            position=task_data.get("position")
        )

        # Если это групповая задача, создаем подзадачи
        if is_group and "subtasks" in task_data:
            for subtask in task_data["subtasks"]:
                self.db.create_task(
                    project_id=project_id,
                    parent_id=task_id,
                    name=subtask["name"],
                    duration=subtask["duration"],
                    position=subtask["position"],
                    parallel=subtask.get("parallel", False)
                )

        # Устанавливаем зависимости
        if "predecessors" in task_data and task_data["predecessors"]:
            predecessors = []
            for predecessor_id in task_data["predecessors"]:
                # Проверяем существование предшественника
                predecessor = self.db.get_task(predecessor_id)
                if predecessor:
                    self.db.add_dependency(task_id, predecessor_id)
                    predecessors.append(predecessor_id)

            # Обновляем задачу с информацией о предшественниках
            if predecessors:
                self.db.execute(
                    "UPDATE tasks SET predecessors = ? WHERE id = ?",
                    (json.dumps(predecessors), task_id)
                )

        return task_id

    def delete_project(self, project_id):
        """Удаляет проект и все связанные с ним задачи"""
        # Проверяем существование проекта
        project = self.db.get_project(project_id)
        if not project:
            raise ValueError(f"Проект с ID {project_id} не найден")

        # Получаем список задач проекта
        tasks = self.db.get_tasks(project_id)

        # Удаляем зависимости для всех задач
        for task in tasks:
            task_id = task["id"]
            dependencies = self.db.get_task_dependencies(task_id)
            for dep in dependencies:
                self.db.execute("DELETE FROM dependencies WHERE id = ?", (dep["id"],))

            dependents = self.db.get_dependents(task_id)
            for dep in dependents:
                self.db.execute("DELETE FROM dependencies WHERE id = ?", (dep["id"],))

            # Удаляем подзадачи
            subtasks = self.db.get_subtasks(task_id)
            for subtask in subtasks:
                self.db.execute("DELETE FROM tasks WHERE id = ?", (subtask["id"],))

            # Удаляем задачу
            self.db.execute("DELETE FROM tasks WHERE id = ?", (task_id,))

        # Удаляем проект
        self.db.execute("DELETE FROM projects WHERE id = ?", (project_id,))