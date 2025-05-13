import datetime
import json


class TaskManager:
    def __init__(self, db_manager):
        self.db = db_manager

    def get_tasks_by_project(self, project_id):
        """Возвращает список задач проекта"""
        tasks = self.db.get_tasks(project_id)
        result = []

        for task in tasks:
            task_dict = dict(task)

            # Добавляем информацию о предшественниках
            if 'predecessors' in task_dict and task_dict['predecessors']:
                try:
                    task_dict['predecessors'] = json.loads(task_dict['predecessors'])
                except (json.JSONDecodeError, TypeError):
                    task_dict['predecessors'] = []
            else:
                # Если в задаче нет информации о предшественниках, получаем ее из таблицы зависимостей
                dependencies = self.db.get_task_dependencies(task_dict['id'])
                task_dict['predecessors'] = [dep['predecessor_id'] for dep in dependencies]

            result.append(task_dict)

        return result

    def get_subtasks(self, task_id):
        """Возвращает список подзадач для групповой задачи"""
        subtasks = self.db.get_subtasks(task_id)
        return [dict(subtask) for subtask in subtasks]

    def get_task(self, task_id):
        """Возвращает информацию о задаче"""
        task = self.db.get_task(task_id)
        if not task:
            raise ValueError(f"Задача с ID {task_id} не найдена")

        task_dict = dict(task)

        # Добавляем информацию о предшественниках
        if 'predecessors' in task_dict and task_dict['predecessors']:
            try:
                task_dict['predecessors'] = json.loads(task_dict['predecessors'])
            except (json.JSONDecodeError, TypeError):
                task_dict['predecessors'] = []
        else:
            # Если в задаче нет информации о предшественниках, получаем ее из таблицы зависимостей
            dependencies = self.db.get_task_dependencies(task_id)
            task_dict['predecessors'] = [dep['predecessor_id'] for dep in dependencies]

        return task_dict

    def create_task(self, project_id, task_data):
        """Создает новую задачу в проекте"""
        try:
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
                for predecessor_id in task_data["predecessors"]:
                    self.db.add_dependency(task_id, predecessor_id)

            return task_id

        except ValueError as e:
            raise ValueError(f"Ошибка при создании задачи: {str(e)}")

    def create_subtask(self, project_id, parent_id, subtask_data):
        """Создает подзадачу для групповой задачи"""
        try:
            # Проверяем существование родительской задачи
            parent_task = self.db.get_task(parent_id)
            if not parent_task:
                raise ValueError(f"Родительская задача с ID {parent_id} не найдена")

            # Проверяем, что родительская задача является групповой
            if not parent_task["is_group"]:
                raise ValueError("Подзадачи можно создавать только для групповых задач")

            # Создаем подзадачу
            subtask_id = self.db.create_task(
                project_id=project_id,
                parent_id=parent_id,
                name=subtask_data["name"],
                duration=subtask_data["duration"],
                position=subtask_data["position"],
                parallel=subtask_data.get("parallel", False)
            )

            return subtask_id

        except ValueError as e:
            raise ValueError(f"Ошибка при создании подзадачи: {str(e)}")

    def assign_employee(self, task_id, employee_id):
        """Назначает сотрудника на задачу"""
        try:
            # Проверяем существование задачи
            task = self.db.get_task(task_id)
            if not task:
                raise ValueError(f"Задача с ID {task_id} не найдена")

            # Проверяем, что задача не является групповой
            if task["is_group"]:
                raise ValueError("Нельзя назначить сотрудника на групповую задачу. Назначьте сотрудников на подзадачи.")

            # Проверяем существование сотрудника
            employee = self.db.get_employee(employee_id)
            if not employee:
                raise ValueError(f"Сотрудник с ID {employee_id} не найден")

            # Проверяем, что должность сотрудника соответствует требуемой для задачи
            if task["position"] and task["position"] != employee["position"]:
                raise ValueError(
                    f"Должность сотрудника ({employee['position']}) не соответствует требуемой для задачи ({task['position']})")

            # Назначаем сотрудника на задачу
            self.db.assign_employee(task_id, employee_id)

            return True

        except ValueError as e:
            raise ValueError(f"Ошибка при назначении сотрудника: {str(e)}")

    def get_task_dependencies(self, task_id):
        """Возвращает список предшественников задачи"""
        dependencies = self.db.get_task_dependencies(task_id)
        return [dict(dep) for dep in dependencies]

    def get_task_dependents(self, task_id):
        """Возвращает список задач, зависящих от указанной"""
        dependents = self.db.get_dependents(task_id)
        return [dict(dep) for dep in dependents]

    def add_dependency(self, task_id, predecessor_id):
        """Добавляет зависимость между задачами"""
        try:
            # Проверяем существование задачи
            task = self.db.get_task(task_id)
            if not task:
                raise ValueError(f"Задача с ID {task_id} не найдена")

            # Проверяем существование предшественника
            predecessor = self.db.get_task(predecessor_id)
            if not predecessor:
                raise ValueError(f"Предшественник с ID {predecessor_id} не найден")

            # Проверяем, что не создается циклическая зависимость
            if self._is_cyclic_dependency(task_id, predecessor_id):
                raise ValueError("Нельзя создать циклическую зависимость между задачами")

            # Добавляем зависимость
            self.db.add_dependency(task_id, predecessor_id)

            return True

        except ValueError as e:
            raise ValueError(f"Ошибка при добавлении зависимости: {str(e)}")

    def _is_cyclic_dependency(self, task_id, predecessor_id):
        """Проверяет, не создается ли циклическая зависимость"""
        # Если задача и предшественник совпадают, это циклическая зависимость
        if task_id == predecessor_id:
            return True

        # Проверяем, не является ли задача уже предшественником для предшественника
        predecessors_of_predecessor = self.get_task_dependencies(predecessor_id)
        for dep in predecessors_of_predecessor:
            if dep["predecessor_id"] == task_id:
                return True

            # Рекурсивно проверяем предшественников
            if self._is_cyclic_dependency(task_id, dep["predecessor_id"]):
                return True

        return False

    def update_task_dates(self, task_dates):
        """Обновляет даты начала и окончания задач"""
        for task_id, dates in task_dates.items():
            # Проверяем наличие необходимых данных
            if 'start' in dates and 'end' in dates:
                try:
                    # Проверяем, существует ли задача
                    task = self.get_task(task_id)
                    if task:
                        self.db.update_task_dates(task_id, dates['start'], dates['end'])
                except ValueError:
                    # Задача не найдена, пропускаем
                    continue