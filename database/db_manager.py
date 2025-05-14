import sqlite3
import os
import datetime
import json
from data.config import Config


class DatabaseManager:
    def __init__(self):
        self.db_path = Config.DB_NAME
        self.connection = None
        self.cursor = None

    def init_db(self):
        """Инициализирует базу данных и создает таблицы, если их нет"""
        self.connect()

        # Создаем таблицы
        self.cursor.executescript('''
         -- Таблица пользователей
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            is_admin BOOLEAN DEFAULT 0,
            is_active BOOLEAN DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Таблица сотрудников
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            position TEXT NOT NULL,
            days_off TEXT NOT NULL
        );

        -- Таблица проектов
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            start_date TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            user_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );

        -- Таблица задач
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            parent_id INTEGER DEFAULT NULL,
            name TEXT NOT NULL,
            duration INTEGER NOT NULL,
            is_group BOOLEAN DEFAULT 0,
            parallel BOOLEAN DEFAULT 0,
            start_date TEXT DEFAULT NULL,
            end_date TEXT DEFAULT NULL,
            employee_id INTEGER DEFAULT NULL,
            position TEXT DEFAULT NULL,
            predecessors TEXT DEFAULT NULL,
            working_duration INTEGER DEFAULT NULL,
            FOREIGN KEY (project_id) REFERENCES projects (id),
            FOREIGN KEY (parent_id) REFERENCES tasks (id),
            FOREIGN KEY (employee_id) REFERENCES employees (id)
        );

        -- Таблица зависимостей между задачами
        CREATE TABLE IF NOT EXISTS dependencies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            predecessor_id INTEGER NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks (id),
            FOREIGN KEY (predecessor_id) REFERENCES tasks (id)
        );
        ''')

        # Загружаем начальные данные о сотрудниках, если таблица пуста
        self.cursor.execute("SELECT COUNT(*) FROM employees")
        count = self.cursor.fetchone()[0]

        if count == 0:
            for employee in Config.EMPLOYEES:
                self.cursor.execute(
                    "INSERT INTO employees (id, name, position, days_off) VALUES (?, ?, ?, ?)",
                    (employee['id'], employee['name'], employee['position'], json.dumps(employee['days_off']))
                )

        self.connection.commit()
        self.close()

    def connect(self):
        """Устанавливает соединение с базой данных"""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

    def close(self):
        """Закрывает соединение с базой данных"""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.cursor = None

    def execute(self, query, params=None):
        """Выполняет SQL-запрос"""
        self.connect()
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        self.connection.commit()
        result = self.cursor.fetchall()
        self.close()
        return result

    def execute_many(self, query, params_list):
        """Выполняет множество SQL-запросов"""
        self.connect()
        self.cursor.executemany(query, params_list)
        self.connection.commit()
        self.close()

    def get_last_id(self):
        """Возвращает ID последней вставленной записи"""
        return self.cursor.lastrowid

    # Методы для работы с проектами
    def create_project(self, name, start_date, user_id=None):
        """Создает новый проект"""
        self.connect()

        # Отладочная печать
        print(f"Создание проекта: name={name}, start_date={start_date}, user_id={user_id}")

        # Проверяем существование поля в таблице
        self.cursor.execute("PRAGMA table_info(projects)")
        columns = self.cursor.fetchall()
        column_names = [column[1] for column in columns]

        if 'user_id' in column_names:
            # Если поле user_id существует в таблице
            self.cursor.execute(
                "INSERT INTO projects (name, start_date, user_id) VALUES (?, ?, ?)",
                (name, start_date, user_id)
            )
        else:
            # Если поля нет, выполняем миграцию
            print("ВНИМАНИЕ: Поле user_id отсутствует в таблице projects!")
            self.cursor.execute(
                "INSERT INTO projects (name, start_date) VALUES (?, ?)",
                (name, start_date)
            )

        project_id = self.cursor.lastrowid
        # Проверяем, сохранился ли user_id
        self.cursor.execute("SELECT user_id FROM projects WHERE id = ?", (project_id,))
        saved_user_id = self.cursor.fetchone()[0]
        print(f"Проект создан с ID: {project_id}, сохраненный user_id: {saved_user_id}")

        self.connection.commit()
        self.close()
        return project_id

    def get_projects(self, user_id=None):
        """Возвращает список всех проектов"""
        if user_id is not None:
            return self.execute("SELECT * FROM projects WHERE user_id = ? ORDER BY created_at DESC", (user_id,))
        else:
            return self.execute("SELECT * FROM projects ORDER BY created_at DESC")

    def get_project(self, project_id):
        """Возвращает информацию о проекте"""
        result = self.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
        return result[0] if result else None

    # Методы для работы с задачами
    def create_task(self, project_id, name, duration, is_group=False, parent_id=None, position=None, parallel=False,
                    working_duration=None):
        """Создает новую задачу"""
        self.connect()
        working_duration = working_duration or duration  # Если не указано, используем duration
        self.cursor.execute(
            """INSERT INTO tasks 
            (project_id, parent_id, name, duration, working_duration, is_group, position, parallel) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (project_id, parent_id, name, duration, working_duration, is_group, position, parallel)
        )
        task_id = self.cursor.lastrowid
        self.connection.commit()
        self.close()
        return task_id

    def get_tasks(self, project_id):
        """Возвращает список задач проекта"""
        return self.execute(
            """SELECT * FROM tasks 
            WHERE project_id = ? AND parent_id IS NULL 
            ORDER BY id""",
            (project_id,)
        )

    def get_subtasks(self, parent_id):
        """Возвращает список подзадач для групповой задачи"""
        return self.execute(
            "SELECT * FROM tasks WHERE parent_id = ? ORDER BY id",
            (parent_id,)
        )

    def get_task(self, task_id):
        """Возвращает информацию о задаче"""
        result = self.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        return result[0] if result else None

    def assign_employee(self, task_id, employee_id):
        """Назначает сотрудника на задачу"""
        self.execute(
            "UPDATE tasks SET employee_id = ? WHERE id = ?",
            (employee_id, task_id)
        )

    def update_task_dates(self, task_id, start_date, end_date):
        """Обновляет даты начала и окончания задачи"""
        self.execute(
            "UPDATE tasks SET start_date = ?, end_date = ? WHERE id = ?",
            (start_date, end_date, task_id)
        )

    # Методы для работы с зависимостями
    def add_dependency(self, task_id, predecessor_id):
        """Добавляет зависимость между задачами"""
        self.execute(
            "INSERT INTO dependencies (task_id, predecessor_id) VALUES (?, ?)",
            (task_id, predecessor_id)
        )

    def get_task_dependencies(self, task_id):
        """Возвращает список зависимостей для задачи"""
        return self.execute(
            """SELECT d.*, t.name as predecessor_name 
            FROM dependencies d 
            JOIN tasks t ON d.predecessor_id = t.id 
            WHERE d.task_id = ?""",
            (task_id,)
        )

    def get_dependents(self, task_id):
        """Возвращает список задач, зависящих от указанной"""
        return self.execute(
            """SELECT d.*, t.name as dependent_name 
            FROM dependencies d 
            JOIN tasks t ON d.task_id = t.id 
            WHERE d.predecessor_id = ?""",
            (task_id,)
        )

    # Методы для работы с сотрудниками
    def get_employees(self):
        """Возвращает список всех сотрудников"""
        return self.execute("SELECT * FROM employees ORDER BY position, name")

    def get_employees_by_position(self, position):
        """Возвращает список сотрудников определенной должности"""
        return self.execute(
            "SELECT * FROM employees WHERE position = ? ORDER BY name",
            (position,)
        )

    def get_employee(self, employee_id):
        """Возвращает информацию о сотруднике"""
        result = self.execute("SELECT * FROM employees WHERE id = ?", (employee_id,))
        return result[0] if result else None

    def get_all_project_tasks(self, project_id):
        """Возвращает список ВСЕХ задач проекта, включая подзадачи"""
        return self.execute(
            """SELECT * FROM tasks 
            WHERE project_id = ?
            ORDER BY id""",
            (project_id,)
        )

    def get_user(self, user_id):
        """Возвращает информацию о пользователе"""
        result = self.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        return result[0] if result else None

    def get_all_users(self):
        """Возвращает список всех пользователей"""
        return self.execute("SELECT * FROM users ORDER BY created_at DESC")

    def add_user(self, user_id, name=None, is_admin=0):
        """Добавляет нового пользователя"""
        self.execute(
            "INSERT OR IGNORE INTO users (id, name, is_admin) VALUES (?, ?, ?)",
            (user_id, name or f"User_{user_id}", is_admin)
        )

    def update_user(self, user_id, is_active=True, is_admin=None):
        """Обновляет статус пользователя"""
        if is_admin is not None:
            self.execute(
                "UPDATE users SET is_active = ?, is_admin = ? WHERE id = ?",
                (1 if is_active else 0, 1 if is_admin else 0, user_id)
            )
        else:
            self.execute(
                "UPDATE users SET is_active = ? WHERE id = ?",
                (1 if is_active else 0, user_id)
            )

    def delete_user(self, user_id):
        """Удаляет пользователя"""
        self.execute("DELETE FROM users WHERE id = ?", (user_id,))