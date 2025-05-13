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
            weekday = dt.weekday()  # 0 - понедельник, 6 - воскресенье

            # Проверяем, не выходной ли это день для сотрудника
            return weekday + 1 not in employee['days_off']  # +1 потому что в базе 1 - понедельник, 7 - воскресенье

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
        Возвращает распределение задач по сотрудникам для проекта

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

            # Получаем все задачи проекта с назначенными сотрудниками
            tasks = self.db.execute(
                """SELECT t.*, p.name as project_name 
                FROM tasks t 
                JOIN projects p ON t.project_id = p.id
                WHERE t.project_id = ? AND t.employee_id IS NOT NULL""",
                (project_id,)
            )

            # Группируем задачи по сотрудникам
            for task in tasks:
                task_dict = dict(task)
                employee_id = task_dict['employee_id']

                # Находим сотрудника
                employee = next((e for e in employees if e['id'] == employee_id), None)
                if employee:
                    # Создаем ключ для сотрудника, если его еще нет
                    if employee_id not in employee_tasks:
                        employee_tasks[employee_id] = {
                            'name': employee['name'],
                            'position': employee['position'],
                            'tasks': []
                        }

                    # Добавляем задачу
                    employee_tasks[employee_id]['tasks'].append({
                        'id': task_dict['id'],
                        'name': task_dict['name'],
                        'start_date': task_dict.get('start_date'),
                        'end_date': task_dict.get('end_date'),
                        'duration': task_dict['duration'],
                        'project_name': task_dict['project_name']
                    })

            return employee_tasks

        except Exception as e:
            raise ValueError(f"Ошибка при получении распределения задач: {str(e)}")

    def generate_workload_report(self, project_id):
        """
        Генерирует отчет о распределении задач по сотрудникам

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

            # Получаем распределение задач
            workload = self.get_employee_workload(project_id)

            # Создаем отчет
            report = f"Отчет о распределении задач для проекта '{project_dict['name']}'\n\n"

            if not workload:
                report += "Ни одной задачи не назначено на сотрудников.\n"
                return report

            # Подсчитываем общую загрузку каждого сотрудника (в днях)
            employee_load = {}
            for employee_id, data in workload.items():
                total_duration = sum(task['duration'] for task in data['tasks'])
                employee_load[employee_id] = total_duration

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
                    report += f"\n{data['name']} - {employee_load[employee_id]} дней загрузки\n"

                    # Задачи сотрудника
                    for task in data['tasks']:
                        date_range = ""
                        if task['start_date'] and task['end_date']:
                            date_range = f" ({task['start_date']} - {task['end_date']})"

                        report += f"• {task['name']} - {task['duration']} дн.{date_range}\n"

            return report

        except Exception as e:
            raise ValueError(f"Ошибка при создании отчета: {str(e)}")
