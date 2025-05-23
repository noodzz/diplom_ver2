import json


class EmployeeManager:
    def __init__(self, db_manager):
        self.db = db_manager

    def get_all_employees(self):
        """Возвращает список всех сотрудников"""
        employees = self.db.get_employees()
        return [self._parse_employee(employee) for employee in employees]

    def get_employees_by_position(self, position):
        """Возвращает список сотрудников определенной должности"""
        employees = self.db.get_employees_by_position(position)
        return [self._parse_employee(employee) for employee in employees]

    def get_employee(self, employee_id):
        """Возвращает информацию о сотруднике"""
        employee = self.db.get_employee(employee_id)
        if not employee:
            raise ValueError(f"Сотрудник с ID {employee_id} не найден")

        return self._parse_employee(employee)

    def is_available(self, employee_id, date):
        """Проверяет, доступен ли сотрудник в указанную дату"""
        try:
            employee = self.get_employee(employee_id)

            # Получаем день недели (0 - понедельник, 6 - воскресенье)
            # Преобразуем формат даты 'YYYY-MM-DD' в объект datetime
            from datetime import datetime
            dt = datetime.strptime(date, '%Y-%m-%d')
            weekday = dt.weekday() + 1  # +1 чтобы привести к формату 1=пн, 7=вс

            # Проверяем, не выходной ли это день
            is_day_off = weekday in employee['days_off']

            # Отладочная информация
            print(f"Проверка доступности {employee['name']} на {date}: "
                  f"день недели {weekday}, выходные дни {employee['days_off']}, "
                  f"результат: {'недоступен' if is_day_off else 'доступен'}")

            return not is_day_off
        except Exception as e:
            raise ValueError(f"Ошибка при проверке доступности сотрудника: {str(e)}")

    def get_available_employees(self, position, date):
        """Возвращает список доступных сотрудников определенной должности на указанную дату"""
        try:
            employees = self.get_employees_by_position(position)
            available_employees = []

            for employee in employees:
                if self.is_available(employee['id'], date):
                    available_employees.append(employee)

            return available_employees

        except Exception as e:
            raise ValueError(f"Ошибка при получении списка доступных сотрудников: {str(e)}")

    def _parse_employee(self, employee):
        """Преобразует данные сотрудника из базы данных в словарь"""
        employee_dict = dict(employee)

        # Преобразуем дни выходных из JSON-строки в список
        if 'days_off' in employee_dict and employee_dict['days_off']:
            try:
                employee_dict['days_off'] = json.loads(employee_dict['days_off'])
            except json.JSONDecodeError:
                employee_dict['days_off'] = []
        else:
            employee_dict['days_off'] = []

        return employee_dict

    def get_employee_workload(self, project_id):
        """
        Возвращает распределение задач по сотрудникам для проекта с учетом одноименных подзадач

        Args:
            project_id (int): ID проекта

        Returns:
            dict: Словарь, где ключ - сотрудник, значение - список задач
        """
        try:
            # Получаем всех сотрудников
            employees = self.get_all_employees()

            # Создаем словарь для хранения задач сотрудников
            employee_tasks = {}

            # Сначала загрузим все задачи проекта для получения информации о родительских задачах
            all_tasks = self.db.execute(
                """SELECT id, name, parent_id FROM tasks WHERE project_id = ?""",
                (project_id,)
            )

            # Создаем словарь для получения имени родительской задачи
            parent_task_names = {}
            for task in all_tasks:
                task_dict = dict(task)
                if task_dict.get('parent_id'):
                    # Найдем родительскую задачу
                    parent = next((t for t in all_tasks if dict(t)['id'] == task_dict['parent_id']), None)
                    if parent:
                        parent_dict = dict(parent)
                        parent_task_names[task_dict['id']] = parent_dict['name']

            # Получаем все задачи проекта с назначенными сотрудниками
            assigned_tasks = self.db.execute(
                """SELECT t.*, p.name as project_name 
                FROM tasks t 
                JOIN projects p ON t.project_id = p.id
                WHERE t.project_id = ? AND t.employee_id IS NOT NULL""",
                (project_id,)
            )

            # Создаем словарь для отслеживания уже добавленных задач для каждого сотрудника
            processed_task_ids = {}  # employee_id -> set of task IDs

            # Группируем задачи по сотрудникам
            for task in assigned_tasks:
                task_dict = dict(task)
                employee_id = task_dict['employee_id']

                # Инициализируем структуры, если это первая задача для сотрудника
                if employee_id not in employee_tasks:
                    employee_tasks[employee_id] = {
                        'name': '',
                        'position': '',
                        'tasks': []
                    }
                    processed_task_ids[employee_id] = set()

                # Находим сотрудника
                employee = next((e for e in employees if e['id'] == employee_id), None)
                if employee:
                    employee_tasks[employee_id]['name'] = employee['name']
                    employee_tasks[employee_id]['position'] = employee['position']

                    # Используем ID задачи для уникальной идентификации
                    task_id = task_dict['id']

                    # Проверяем, не добавляли ли мы уже эту задачу для этого сотрудника
                    if task_id not in processed_task_ids[employee_id]:
                        processed_task_ids[employee_id].add(task_id)

                        # Получаем название задачи с учетом родительской задачи
                        task_name = task_dict['name']
                        if task_dict.get('parent_id') and task_dict['id'] in parent_task_names:
                            # Для подзадач формируем название "Родительская задача - Подзадача"
                            display_name = f"{parent_task_names[task_dict['id']]} - {task_name}"
                        else:
                            display_name = task_name

                        employee_tasks[employee_id]['tasks'].append({
                            'id': task_id,
                            'name': display_name,  # Используем расширенное имя для отображения
                            'start_date': task_dict.get('start_date'),
                            'end_date': task_dict.get('end_date'),
                            'duration': task_dict['duration'],
                            'project_name': task_dict['project_name'],
                            'parallel': task_dict.get('parallel', 0) == 1  # Флаг parallel
                        })

            return employee_tasks

        except Exception as e:
            raise ValueError(f"Ошибка при получении распределения задач: {str(e)}")

    """
    Обновленная функция generate_workload_report для корректного отображения дат задач
    """

    def generate_workload_report(self, project_id):
        """
        Генерирует отчет о распределении задач по сотрудникам с учетом параллельных задач

        Args:
            project_id (int): ID проекта

        Returns:
            str: Текстовый отчет
        """
        try:
            # Получаем данные о проекте
            project = self.db.get_project(project_id)
            if not project:
                raise ValueError(f"Проект с ID {project_id} не найден")

            project_dict = dict(project)

            # Получаем распределение задач - обеспечиваем свежие данные из базы
            # Форсируем обновление из базы данных, чтобы избежать проблем с устаревшими данными
            self.db.connect()
            workload = self.get_employee_workload(project_id)
            self.db.close()

            # Создаем отчет
            report = f"Отчет о распределении задач для проекта '{project_dict['name']}'\n\n"

            if not workload:
                report += "Ни одной задачи не назначено на сотрудников.\n"
                return report

            # Группируем сотрудников по должностям
            positions = {}
            for employee_id, data in workload.items():
                position = data['position']
                if position not in positions:
                    positions[position] = []

                positions[position].append(employee_id)

            # Формируем отчет по должностям
            for position, employee_ids in positions.items():
                report += f"\n== {position} ==\n"

                for employee_id in employee_ids:
                    data = workload[employee_id]

                    # Пересчитываем загрузку сотрудника на основе актуальных данных о задачах
                    total_duration = sum(task.get('duration', 0) for task in data['tasks'])

                    report += f"\n{data['name']} - {total_duration} дней загрузки\n"

                    # Задачи сотрудника
                    for task in data['tasks']:
                        date_range = ""
                        if task.get('start_date') and task.get('end_date'):
                            # Используем актуальные даты из базы
                            # Форматируем даты, не модифицируя их
                            start_date = task['start_date']
                            end_date = task['end_date']
                            date_range = f" ({start_date} - {end_date})"

                        report += f"• {task['name']} - {task['duration']} дн.{date_range}\n"

            return report

        except Exception as e:
            raise ValueError(f"Ошибка при создании отчета: {str(e)}")

    def check_employee_workload(self, employee_id, start_date, end_date):
        """
        Проверяет загрузку сотрудника в указанном диапазоне дат

        Args:
            employee_id (int): ID сотрудника
            start_date (str): Дата начала в формате 'YYYY-MM-DD'
            end_date (str): Дата окончания в формате 'YYYY-MM-DD'

        Returns:
            list: Список задач, назначенных на сотрудника в указанном диапазоне
        """
        try:
            # Получаем все задачи, назначенные на сотрудника
            tasks = self.db.execute(
                """SELECT t.*, p.name as project_name, p.start_date as project_start_date 
                FROM tasks t 
                JOIN projects p ON t.project_id = p.id
                WHERE t.employee_id = ?""",
                (employee_id,)
            )

            # Фильтруем задачи по диапазону дат
            filtered_tasks = []
            for task in tasks:
                task_dict = dict(task)

                # Если у задачи есть даты начала и окончания, проверяем пересечение с указанным диапазоном
                if task_dict.get('start_date') and task_dict.get('end_date'):
                    task_start = task_dict['start_date']
                    task_end = task_dict['end_date']

                    # Проверяем, пересекаются ли диапазоны дат
                    if not (task_end < start_date or task_start > end_date):
                        filtered_tasks.append(task_dict)

            return filtered_tasks

        except Exception as e:
            raise ValueError(f"Ошибка при проверке загрузки сотрудника: {str(e)}")

    def get_category_by_position(self, position):
        """
        Определяет категорию сотрудника по его должности

        Args:
            position (str): Должность сотрудника

        Returns:
            str: Название категории ("ПМы", "Настройка", "Контент") или None
        """
        if not position:
            return None

        position = position.lower()

        # ПМы
        if "проектный менеджер" in position or "пм" in position or "менеджер" in position:
            return "ПМы"

        # Настройка (проверяем первым, так как имеет приоритет над "специалист")
        if "настройка" in position or "технический" in position or "руководитель настройки" in position:
            return "Настройка"

        # Контент
        if "контент" in position or "специалист" in position:
            return "Контент"

        return None