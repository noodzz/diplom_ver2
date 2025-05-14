import os
import logging
import asyncio
import json
import tempfile

from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    KeyboardButton, Message, ReplyKeyboardMarkup, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
)
from dotenv import load_dotenv
from data.config import Config
from database.db_manager import DatabaseManager
from services.project_manager import ProjectManager
from services.task_manager import TaskManager
from services.employee_manager import EmployeeManager
from services.jira_exporter import JiraExporter
from services.network_model import NetworkModel
from services.gantt_chart import GanttChart
from services.workload_chart import WorkloadChart
from utils.helpers import parse_csv, format_date, is_authorized

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=os.getenv("BOT_TOKEN"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Инициализация менеджеров
db_manager = DatabaseManager()
project_manager = ProjectManager(db_manager)
task_manager = TaskManager(db_manager)
employee_manager = EmployeeManager(db_manager)
jira_exporter = JiraExporter()
network_model = NetworkModel()
gantt_chart = GanttChart()
workload_chart = WorkloadChart()

# Создание роутера
router = Router()
dp.include_router(router)


# Определение состояний для конечного автомата
class ProjectState(StatesGroup):
    waiting_for_name = State()
    waiting_for_start_date = State()
    waiting_for_choice = State()
    waiting_for_csv = State()
    waiting_for_template = State()


class TaskState(StatesGroup):
    waiting_for_name = State()
    waiting_for_duration = State()
    waiting_for_predecessors = State()
    waiting_for_employee_type = State()
    waiting_for_employee = State()


# -----------------------------------------------------------------------------
# Обработчики команд
# -----------------------------------------------------------------------------

@router.message(CommandStart())
async def cmd_start(message: Message):
    if not is_authorized(message.from_user.id):
        await message.answer("Извините, у вас нет доступа к этому боту.")
        return

    await message.answer(
        "Добро пожаловать в бот для управления проектами!\n"
        "Используйте /help для просмотра доступных команд."
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    if not is_authorized(message.from_user.id):
        return

    help_text = (
        "Доступные команды:\n"
        "/create_project - Создать новый проект\n"
        "/list_projects - Список всех проектов\n"
        "/view_project - Просмотр деталей проекта\n"
        "/add_task - Добавить задачу в проект\n"
        "/assign_employee - Назначить сотрудника на задачу\n"
        "/calculate_schedule - Рассчитать календарный план\n"
        "/employee_workload - Распределение задач по сотрудникам\n"
        "/export_to_jira - Экспортировать проект в Jira\n"
        "/help - Показать эту справку"
    )
    await message.answer(help_text)


# -----------------------------------------------------------------------------
# Управление проектами
# -----------------------------------------------------------------------------

@router.message(Command("create_project"))
async def cmd_create_project(message: Message, state: FSMContext):
    if not is_authorized(message.from_user.id):
        return

    await message.answer("Введите название проекта:")
    await state.set_state(ProjectState.waiting_for_name)


@router.message(ProjectState.waiting_for_name)
async def process_project_name(message: Message, state: FSMContext):
    await state.update_data(project_name=message.text)
    await message.answer("Введите дату начала проекта (формат YYYY-MM-DD):")
    await state.set_state(ProjectState.waiting_for_start_date)


@router.message(ProjectState.waiting_for_start_date)
async def process_start_date(message: Message, state: FSMContext):
    await state.update_data(start_date=message.text)

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Использовать шаблон", callback_data="use_template")],
        [InlineKeyboardButton(text="Загрузить из CSV", callback_data="upload_csv")],
        [InlineKeyboardButton(text="Создать пустой проект", callback_data="empty_project")]
    ])

    await message.answer("Как вы хотите создать проект?", reply_markup=markup)
    await state.set_state(ProjectState.waiting_for_choice)


@router.callback_query(F.data == "use_template", ProjectState.waiting_for_choice)
async def process_template_choice(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Выберите шаблон:")

    templates = project_manager.get_templates()
    buttons = []
    for template in templates:
        buttons.append([InlineKeyboardButton(text=template['name'], callback_data=f"template_{template['id']}")])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.answer("Доступные шаблоны:", reply_markup=markup)
    await state.set_state(ProjectState.waiting_for_template)


@router.callback_query(ProjectState.waiting_for_template)
async def process_template_selection(callback: CallbackQuery, state: FSMContext):
    template_id = int(callback.data.split('_')[1])
    user_data = await state.get_data()

    try:
        project_id = project_manager.create_from_template(
            user_data['project_name'],
            user_data['start_date'],
            template_id
        )

        await callback.message.edit_text(
            f"Проект '{user_data['project_name']}' успешно создан из шаблона!\n"
            f"ID проекта: {project_id}"
        )
    except Exception as e:
        await callback.message.edit_text(f"Ошибка при создании проекта: {str(e)}")

    await state.clear()


@router.callback_query(F.data == "upload_csv", ProjectState.waiting_for_choice)
async def process_csv_choice(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Пожалуйста, загрузите CSV-файл с данными проекта.\n"
        "Файл должен содержать следующие столбцы:\n"
        "- Задача - Название задачи\n"
        "- Длительность - Длительность задачи в днях\n"
        "- Тип - Тип задачи (обычная или групповая)\n"
        "- Должность - Требуемая должность для выполнения задачи\n"
        "- Предшественники - Список предшествующих задач через запятую\n"
        "- Родительская задача - Для подзадач указывается название родительской задачи\n"
        "- Параллельная - Для подзадач указывается, могут ли они выполняться параллельно (да/нет)"
    )
    await state.set_state(ProjectState.waiting_for_csv)


@router.message(ProjectState.waiting_for_csv)
async def process_csv_file(message: Message, state: FSMContext):
    if not message.document:
        await message.answer("Пожалуйста, отправьте CSV-файл.")
        return

    try:
        file = await bot.get_file(message.document.file_id)
        file_path = file.file_path
        downloaded_file = await bot.download_file(file_path)

        user_data = await state.get_data()
        csv_content = downloaded_file.read().decode('utf-8')
        project_data = parse_csv(csv_content)

        project_id = project_manager.create_from_csv(
            user_data['project_name'],
            user_data['start_date'],
            project_data
        )

        await message.answer(
            f"Проект '{user_data['project_name']}' успешно создан из CSV!\n"
            f"ID проекта: {project_id}"
        )
    except Exception as e:
        await message.answer(f"Ошибка при обработке CSV: {str(e)}")

    await state.clear()


@router.callback_query(F.data == "empty_project", ProjectState.waiting_for_choice)
async def process_empty_project(callback: CallbackQuery, state: FSMContext):
    user_data = await state.get_data()

    try:
        project_id = project_manager.create_empty(
            user_data['project_name'],
            user_data['start_date']
        )

        await callback.message.edit_text(
            f"Пустой проект '{user_data['project_name']}' успешно создан!\n"
            f"ID проекта: {project_id}\n\n"
            f"Теперь вы можете добавить задачи с помощью команды /add_task"
        )
    except Exception as e:
        await callback.message.edit_text(f"Ошибка при создании проекта: {str(e)}")

    await state.clear()


@router.message(Command("list_projects"))
async def cmd_list_projects(message: Message):
    if not is_authorized(message.from_user.id):
        return

    projects = project_manager.get_all_projects()

    if not projects:
        await message.answer("Проектов пока нет. Создайте новый с помощью команды /create_project")
        return

    buttons = []
    for project in projects:
        buttons.append([InlineKeyboardButton(
            text=f"{project['name']} (начало: {format_date(project['start_date'])})",
            callback_data=f"view_project_{project['id']}"
        )])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("Выберите проект для просмотра:", reply_markup=markup)


@router.callback_query(lambda c: c.data.startswith("view_project_"))
async def view_project_callback(callback: CallbackQuery):
    import os
    import tempfile

    project_id = int(callback.data.split("_")[2])

    try:
        project_info = project_manager.get_project_details(project_id)
        tasks = task_manager.get_tasks_by_project(project_id)

        text = f"Проект: {project_info['name']}\n"
        text += f"Дата начала: {format_date(project_info['start_date'])}\n"
        text += f"Статус: {project_info['status']}\n\n"

        if tasks:
            text += "Задачи:\n"
            for task in tasks:
                text += f"• {task['name']} "
                text += f"({task['duration']} дн.) "

                if task['is_group']:
                    text += "[Групповая задача]\n"
                    subtasks = task_manager.get_subtasks(task['id'])
                    for subtask in subtasks:
                        employee = None
                        if subtask.get('employee_id'):
                            try:
                                employee = employee_manager.get_employee(subtask['employee_id'])
                            except ValueError:
                                pass

                        employee_name = f"{employee['name']} ({employee['position']})" if employee else "Не назначен"
                        text += f"  ↳ {subtask['name']} - {employee_name}\n"
                else:
                    employee = None
                    if task.get('employee_id'):
                        try:
                            employee = employee_manager.get_employee(task['employee_id'])
                        except ValueError:
                            pass

                    text += f"- {employee['name']} ({employee['position']})" if employee else "- Не назначен\n"
        else:
            text += "Задач в проекте нет"

        # Проверяем длину текста и отправляем соответствующим образом
        if len(text) > 3500:  # Лимит Telegram с запасом
            # Создаем временный файл
            temp_dir = tempfile.mkdtemp()
            safe_project_name = "".join(
                c if c.isalnum() or c in [' ', '.', '_', '-'] else '_' for c in project_info['name'])
            file_path = os.path.join(temp_dir, f"{safe_project_name}_details.txt")

            # Записываем текст в файл
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(text)

            # Отправляем краткую информацию
            await callback.message.edit_text(
                f"Проект: {project_info['name']}\n"
                f"Дата начала: {format_date(project_info['start_date'])}\n"
                f"Статус: {project_info['status']}\n\n"
                f"Проект содержит много задач, полные детали в файле:"
            )

            # Отправляем файл
            file = FSInputFile(file_path)
            await bot.send_document(
                callback.from_user.id,
                file,
                caption=f"Детали проекта '{project_info['name']}'"
            )

            # Отправляем кнопки
            buttons = [
                [InlineKeyboardButton(text="Добавить задачу", callback_data=f"add_task_{project_id}")],
                [InlineKeyboardButton(text="Рассчитать календарный план", callback_data=f"calculate_{project_id}")],
                [InlineKeyboardButton(text="Распределение по сотрудникам", callback_data=f"workload_{project_id}")],
                [InlineKeyboardButton(text="Экспорт в Jira", callback_data=f"export_jira_{project_id}")],
                [InlineKeyboardButton(text="Назад к списку проектов", callback_data="back_to_projects")]
            ]

            markup = InlineKeyboardMarkup(inline_keyboard=buttons)
            await callback.message.reply("Выберите действие:", reply_markup=markup)

            # Удаляем временный файл
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Ошибка при удалении временного файла: {str(e)}")
        else:
            # Если текст не слишком длинный, отправляем обычным сообщением
            buttons = [
                [InlineKeyboardButton(text="Добавить задачу", callback_data=f"add_task_{project_id}")],
                [InlineKeyboardButton(text="Рассчитать календарный план", callback_data=f"calculate_{project_id}")],
                [InlineKeyboardButton(text="Распределение по сотрудникам", callback_data=f"workload_{project_id}")],
                [InlineKeyboardButton(text="Экспорт в Jira", callback_data=f"export_jira_{project_id}")],
                [InlineKeyboardButton(text="Назад к списку проектов", callback_data="back_to_projects")]
            ]

            markup = InlineKeyboardMarkup(inline_keyboard=buttons)
            await callback.message.edit_text(text, reply_markup=markup)

    except Exception as e:
        await callback.message.edit_text(f"Ошибка при получении данных проекта: {str(e)}")

@router.callback_query(F.data == "back_to_projects")
async def back_to_projects(callback: CallbackQuery):
    projects = project_manager.get_all_projects()

    buttons = []
    for project in projects:
        buttons.append([InlineKeyboardButton(
            text=f"{project['name']} (начало: {format_date(project['start_date'])})",
            callback_data=f"view_project_{project['id']}"
        )])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text("Выберите проект для просмотра:", reply_markup=markup)


# -----------------------------------------------------------------------------
# Календарное планирование
# -----------------------------------------------------------------------------

@router.callback_query(lambda c: c.data.startswith("calculate_"))
async def calculate_schedule(callback: CallbackQuery):
    project_id = int(callback.data.split("_")[1])

    await callback.message.edit_text("Выполняется расчет календарного плана и распределение задач...")

    try:
        project = project_manager.get_project_details(project_id)
        tasks = task_manager.get_tasks_by_project(project_id)

        # ШАГ 1: Назначаем сотрудников на задачи (без привязки к датам)
        assignments = {}  # Словарь для хранения назначений

        # Получаем список сотрудников
        all_employees = employee_manager.get_all_employees()

        # Группируем сотрудников по должностям
        employee_by_position = {}
        for employee in all_employees:
            position = employee['position']
            if position not in employee_by_position:
                employee_by_position[position] = []
            employee_by_position[position].append(employee)

        # Словарь для отслеживания загрузки сотрудников (в днях)
        employee_workload = {}
        for employee in all_employees:
            employee_workload[employee['id']] = 0

        # Назначаем сотрудников на основные задачи
        print("Назначаем сотрудников на основные задачи...")
        for task in tasks:
            if not task['is_group'] and not task.get('parent_id'):
                position = task.get('position')
                if not position:
                    print(f"Пропускаем задачу {task['name']} (ID: {task['id']}): не указана должность")
                    continue

                suitable_employees = employee_by_position.get(position, [])
                if not suitable_employees:
                    print(f"Пропускаем задачу {task['name']} (ID: {task['id']}): нет сотрудников должности {position}")
                    continue

                # Находим наименее загруженного сотрудника
                best_employee = None
                min_load = float('inf')

                for employee in suitable_employees:
                    load = employee_workload[employee['id']]
                    if load < min_load:
                        min_load = load
                        best_employee = employee

                # Назначаем сотрудника на задачу
                if best_employee:
                    task_manager.assign_employee(task['id'], best_employee['id'])
                    assignments[task['id']] = best_employee['id']

                    # Обновляем загрузку сотрудника
                    employee_workload[best_employee['id']] += task['duration']
                    print(f"Сотрудник {best_employee['name']} назначен на задачу {task['name']}")

        # Получаем подзадачи для групповых задач напрямую из БД
        print("Назначаем сотрудников на подзадачи...")
        all_subtasks = {}
        for task in tasks:
            if task['is_group']:
                db_subtasks = task_manager.db.execute(
                    "SELECT * FROM tasks WHERE parent_id = ?",
                    (task['id'],)
                )
                if db_subtasks:
                    all_subtasks[task['id']] = [dict(subtask) for subtask in db_subtasks]

        # Назначаем сотрудников на подзадачи
        for task_id, subtasks in all_subtasks.items():
            for subtask in subtasks:
                position = subtask.get('position')
                if not position:
                    print(f"Пропускаем подзадачу {subtask['name']} (ID: {subtask['id']}): не указана должность")
                    continue

                suitable_employees = employee_by_position.get(position, [])
                if not suitable_employees:
                    print(
                        f"Пропускаем подзадачу {subtask['name']} (ID: {subtask['id']}): нет сотрудников должности {position}")
                    continue

                # Находим наименее загруженного сотрудника
                best_employee = None
                min_load = float('inf')

                for employee in suitable_employees:
                    load = employee_workload[employee['id']]
                    if load < min_load:
                        min_load = load
                        best_employee = employee

                # Назначаем сотрудника на подзадачу
                if best_employee:
                    task_manager.assign_employee(subtask['id'], best_employee['id'])
                    assignments[subtask['id']] = best_employee['id']

                    # Обновляем загрузку сотрудника
                    employee_workload[best_employee['id']] += subtask['duration']
                    print(f"Сотрудник {best_employee['name']} назначен на подзадачу {subtask['name']}")

        # ШАГ 2: Корректируем длительности задач с учетом выходных дней сотрудников
        print("Корректируем длительности задач с учетом выходных дней...")
        for task in tasks:
            if not task['is_group'] and task.get('employee_id'):
                adjust_task_duration_for_days_off(task, employee_manager)

        for task_id, subtasks in all_subtasks.items():
            for subtask in subtasks:
                if subtask.get('employee_id'):
                    adjust_task_duration_for_days_off(subtask, employee_manager)

        # ШАГ 3: Рассчитываем календарный план с учетом скорректированных длительностей
        print("Рассчитываем календарный план...")
        result = network_model.calculate(project, tasks)

        # ШАГ 4: Обновляем даты задач в базе данных
        print("Обновляем даты задач...")
        task_manager.update_task_dates(result['task_dates'])

        # ШАГ 5: Генерируем отчет о результатах
        print("Генерируем отчет...")
        text = f"Календарный план для проекта '{project['name']}'\n\n"

        # Вычисляем фактическую длительность проекта
        from datetime import datetime, timedelta
        if result['task_dates']:
            start_dates = [datetime.strptime(dates['start'], '%Y-%m-%d') for dates in
                           result['task_dates'].values()]
            end_dates = [datetime.strptime(dates['end'], '%Y-%m-%d') for dates in
                         result['task_dates'].values()]

            if start_dates and end_dates:
                project_start = min(start_dates)
                project_end = max(end_dates)
                project_duration = (project_end - project_start).days + 1
                text += f"Длительность проекта: {project_duration} дней\n"
                text += f"Дата начала: {project_start.strftime('%d.%m.%Y')}\n"
                text += f"Дата завершения: {project_end.strftime('%d.%m.%Y')}\n\n"
            else:
                text += f"Длительность проекта: {result['duration']} дней\n\n"
        else:
            text += f"Длительность проекта: {result['duration']} дней\n\n"

        # Критический путь
        text += "Критический путь:\n"
        for task_id in result['critical_path']:
            task = task_manager.get_task(task_id)
            text += f"• {task['name']} ({task['duration']} дн.)\n"

        # Информация о распределении задач
        if assignments:
            text += "\nАвтоматически распределены задачи:\n"
            for task_id, employee_id in assignments.items():
                task = task_manager.get_task(task_id)
                employee = employee_manager.get_employee(employee_id)

                # Определяем, это основная задача или подзадача
                if task.get('parent_id'):
                    parent_task = task_manager.get_task(task['parent_id'])
                    text += f"• {parent_task['name']} → {task['name']} → {employee['name']}\n"
                else:
                    text += f"• {task['name']} → {employee['name']}\n"
        else:
            text += "\nНе удалось автоматически распределить задачи."

        # Генерируем диаграмму Ганта
        gantt_image = gantt_chart.generate(project, tasks, result['task_dates'], result['critical_path'])

        # Всегда создаем файл для отчета, чтобы избежать ошибки MESSAGE_TOO_LONG
        # Создаем безопасное имя файла
        safe_project_name = "".join(c if c.isalnum() or c in [' ', '.', '_', '-'] else '_' for c in project['name'])

        temp_dir = tempfile.mkdtemp()
        report_file_path = os.path.join(temp_dir, f"{safe_project_name}_report.txt")

        # Записываем отчет в файл
        with open(report_file_path, 'w', encoding='utf-8') as file:
            file.write(text)

        # Отправляем краткое сообщение и файл
        await callback.message.edit_text(
            f"Расчет календарного плана для проекта '{project['name']}' завершен.\n"
            f"Длительность проекта: {result.get('duration', 'Не определена')} дней.\n"
            f"Полный отчет прилагается в файле."
        )

        # Отправляем файл с отчетом
        report_file = FSInputFile(report_file_path)
        await bot.send_document(
            callback.from_user.id,
            report_file,
            caption=f"Отчет по проекту '{project['name']}'"
        )

        # Отправляем диаграмму Ганта
        gantt_file = FSInputFile(gantt_image)
        await bot.send_photo(
            callback.from_user.id,
            gantt_file,
            caption=f"Диаграмма Ганта для проекта '{project['name']}'",
        )

        # Если есть распределение задач, отправляем также диаграмму загрузки
        if assignments:
            workload_data = employee_manager.get_employee_workload(project_id)
            if workload_data:
                workload_image = workload_chart.generate(project, workload_data)
                workload_file = FSInputFile(workload_image)
                await bot.send_photo(
                    callback.from_user.id,
                    workload_file,
                    caption=f"Диаграмма загрузки сотрудников для проекта '{project['name']}'",
                )

        # Отправляем кнопки для дальнейших действий
        buttons = [
            [InlineKeyboardButton(text="Просмотреть/изменить распределение", callback_data=f"workload_{project_id}")],
            [InlineKeyboardButton(text="Назад к проекту", callback_data=f"view_project_{project_id}")]
        ]

        markup = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.reply("Расчет календарного плана и распределение задач завершены", reply_markup=markup)

        # Очистка временных файлов
        try:
            if os.path.exists(report_file_path):
                os.remove(report_file_path)
            if os.path.exists(gantt_image):
                os.remove(gantt_image)
        except Exception as e:
            print(f"Ошибка при очистке временных файлов: {str(e)}")

    except Exception as e:
        await callback.message.edit_text(f"Ошибка при расчете календарного плана: {str(e)}")


def adjust_task_duration_for_days_off(task, employee_manager):
    """
    Корректирует длительность задачи с учетом выходных дней сотрудника

    Args:
        task (dict): Задача
        employee_manager: Менеджер сотрудников
    """
    try:
        employee_id = task['employee_id']
        original_duration = task['duration']

        # Предполагаем начало с даты проекта
        from datetime import datetime, timedelta
        project_start_date = datetime.strptime(task.get('project_start_date', '2025-01-01'), '%Y-%m-%d')

        # Рассчитываем, сколько календарных дней потребуется для выполнения задачи
        calendar_days = 0
        working_days = 0
        current_date = project_start_date

        while working_days < original_duration:
            calendar_days += 1
            date_str = current_date.strftime('%Y-%m-%d')

            if employee_manager.is_available(employee_id, date_str):
                working_days += 1

            current_date += timedelta(days=1)

            # Защита от бесконечного цикла
            if calendar_days > 100:
                break

        # Обновляем длительность задачи
        if calendar_days > original_duration:
            print(
                f"Задача '{task['name']}' (ID: {task['id']}): длительность скорректирована с {original_duration} до {calendar_days} дней")
            task['adjusted_duration'] = calendar_days

            # Обновляем в базе данных
            task_manager.db.execute(
                "UPDATE tasks SET duration = ? WHERE id = ?",
                (calendar_days, task['id'])
            )

    except Exception as e:
        print(f"Ошибка при корректировке длительности задачи {task.get('name', 'Unknown')}: {str(e)}")


# -----------------------------------------------------------------------------
# Экспорт в Jira
# -----------------------------------------------------------------------------

@router.callback_query(lambda c: c.data.startswith("export_jira_"))
async def export_to_jira(callback: CallbackQuery):
    project_id = int(callback.data.split("_")[2])

    await callback.message.edit_text("Выполняется экспорт в Jira...")

    try:
        project = project_manager.get_project_details(project_id)
        tasks = task_manager.get_tasks_by_project(project_id)

        # Пробуем прямую интеграцию с Jira API
        result = jira_exporter.import_to_jira(project, tasks)

        if result['success']:
            # API-интеграция успешна
            message_text = (
                f"Проект '{project['name']}' успешно экспортирован в Jira!\n\n"
                f"Эпик: {result['epic_key']}\n"
                f"Создано задач: {len(result['created_issues'])}\n\n"
                f"Ссылка на проект в Jira: {result['jira_project_url']}"
            )
            await callback.message.edit_text(message_text)
        else:
            # Если API не сработал, отправляем файл
            file = FSInputFile(result['csv_export_file'])
            await bot.send_document(
                callback.from_user.id,
                file,
                caption=f"Файл для импорта в Jira (проект '{project['name']}')\n\n{result['message']}"
            )

            await callback.message.edit_text(
                "Экспорт в Jira через API не удался. Отправлен CSV-файл для ручного импорта."
            )

        buttons = [
            [InlineKeyboardButton(text="Назад к проекту", callback_data=f"view_project_{project_id}")]
        ]

        markup = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.reply("Экспорт завершен", reply_markup=markup)

    except Exception as e:
        await callback.message.edit_text(f"Ошибка при экспорте в Jira: {str(e)}")


# -----------------------------------------------------------------------------
# Распределение задач по сотрудникам
# -----------------------------------------------------------------------------

@router.message(Command("employee_workload"))
async def cmd_employee_workload(message: Message):
    """Показывает распределение задач по сотрудникам"""
    if not is_authorized(message.from_user.id):
        return

    projects = project_manager.get_all_projects()

    if not projects:
        await message.answer("Проектов пока нет. Создайте новый с помощью команды /create_project")
        return

    buttons = []
    for project in projects:
        buttons.append([InlineKeyboardButton(
            text=f"{project['name']} (начало: {format_date(project['start_date'])})",
            callback_data=f"workload_{project['id']}"
        )])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("Выберите проект для просмотра распределения задач по сотрудникам:", reply_markup=markup)


@router.callback_query(lambda c: c.data.startswith("workload_"))
async def show_employee_workload(callback: CallbackQuery):
    """Показывает распределение задач по сотрудникам для выбранного проекта"""
    try:
        project_id = int(callback.data.split("_")[1])
    except (ValueError, IndexError):
        await callback.message.edit_text("Ошибка: некорректный идентификатор проекта")
        return

    try:
        # Получаем данные о проекте
        project = project_manager.get_project_details(project_id)

        # Получаем распределение задач по сотрудникам
        workload_data = employee_manager.get_employee_workload(project_id)

        # Генерируем отчет о распределении задач
        report = employee_manager.generate_workload_report(project_id)

        # Создаем кнопку для возврата к просмотру проекта
        buttons = [
            [InlineKeyboardButton(text="Назначить сотрудников", callback_data=f"assign_to_project_{project_id}")],
            [InlineKeyboardButton(text="Назад к проекту", callback_data=f"view_project_{project_id}")]
        ]

        markup = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.edit_text(report, reply_markup=markup)

        # Генерируем и отправляем диаграмму загрузки сотрудников
        workload_image = workload_chart.generate(project, workload_data)

        workload_file = FSInputFile(workload_image)
        await bot.send_photo(
            callback.from_user.id,
            workload_file,
            caption=f"Диаграмма загрузки сотрудников для проекта '{project['name']}'",
        )

    except Exception as e:
        await callback.message.edit_text(f"Ошибка при получении распределения задач: {str(e)}")


@router.callback_query(lambda c: c.data.startswith("assign_to_project_"))
async def assign_to_project(callback: CallbackQuery):
    """Показывает список задач проекта для назначения сотрудников"""
    try:
        parts = callback.data.split("_")
        if len(parts) < 3:
            await callback.message.edit_text(
                "Ошибка: неверный формат данных. Пожалуйста, вернитесь в список проектов.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Вернуться к списку проектов", callback_data="back_to_projects")]
                ])
            )
            return

        project_id = int(parts[2])
    except ValueError:
        await callback.message.edit_text(
            "Ошибка: некорректный идентификатор проекта. Пожалуйста, вернитесь в список проектов.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Вернуться к списку проектов", callback_data="back_to_projects")]
            ])
        )
        return

    try:
        # Получаем данные о проекте
        project = project_manager.get_project_details(project_id)

        # Получаем список задач, на которые можно назначить сотрудников
        tasks = task_manager.get_tasks_by_project(project_id)
        assignable_tasks = [task for task in tasks if not task['is_group'] and not task.get('parent_id')]

        if not assignable_tasks:
            await callback.message.edit_text(
                f"В проекте '{project['name']}' нет задач, на которые можно назначить сотрудников.\n"
                f"Сначала добавьте задачи с помощью команды /add_task.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Назад к проекту", callback_data=f"view_project_{project_id}")]
                ])
            )
            return

        # Создаем кнопки для задач
        buttons = []
        for task in assignable_tasks:
            # Определяем текущий статус назначения
            status = ""
            if task.get('employee_id'):
                employee = employee_manager.get_employee(task['employee_id'])
                status = f" - {employee['name']}"

            buttons.append([InlineKeyboardButton(
                text=f"{task['name']}{status}",
                callback_data=f"assign_task_{task['id']}"
            )])

        # Добавляем кнопку возврата
        buttons.append([InlineKeyboardButton(
            text="Назад к распределению",
            callback_data=f"workload_{project_id}"
        )])

        markup = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.edit_text(
            f"Выберите задачу для назначения сотрудника в проекте '{project['name']}':",
            reply_markup=markup
        )

    except Exception as e:
        await callback.message.edit_text(f"Ошибка при загрузке задач: {str(e)}")


@router.callback_query(lambda c: c.data.startswith("assign_task_"))
async def assign_employee_to_task(task, start_date, end_date, suitable_employees, employee_daily_load, task_manager,
                                  employee_manager):
    """
    Назначает сотрудника на задачу с учетом выходных дней

    Args:
        task (dict): Задача
        start_date (str): Дата начала в формате YYYY-MM-DD
        end_date (str): Дата окончания в формате YYYY-MM-DD
        suitable_employees (list): Список подходящих сотрудников
        employee_daily_load (dict): Словарь с загрузкой сотрудников по дням
        task_manager: Менеджер задач
        employee_manager: Менеджер сотрудников

    Returns:
        int: ID назначенного сотрудника или None, если не удалось назначить
    """
    try:
        print(f"Попытка назначить сотрудника на задачу: {task['name']} (ID: {task['id']})")

        # Преобразуем даты в объекты datetime
        from datetime import datetime, timedelta
        if isinstance(start_date, datetime):
            original_start = start_date
        else:
            original_start = datetime.strptime(start_date, '%Y-%m-%d')

        if isinstance(end_date, datetime):
            original_end = end_date
        else:
            original_end = datetime.strptime(end_date, '%Y-%m-%d')

        # Продолжительность задачи в днях
        original_duration = (original_end - original_start).days + 1

        # Находим наиболее подходящего сотрудника
        best_employee = None
        min_load = float('inf')
        best_start_date = None
        best_end_date = None

        for employee in suitable_employees:
            # Проверяем доступность сотрудника с учетом выходных
            # Стратегия 1: Пробуем начать задачу в указанную дату и учитываем выходные
            start = original_start
            end = original_start
            working_days = 0
            current_date = start

            # Проверяем до 10 дней после изначальной даты окончания
            max_end_date = original_end + timedelta(days=10)

            while working_days < original_duration and current_date <= max_end_date:
                date_str = current_date.strftime('%Y-%m-%d')

                # Если текущий день - рабочий для сотрудника
                if employee_manager.is_available(employee['id'], date_str):
                    working_days += 1
                    end = current_date

                current_date += timedelta(days=1)

            # Если не смогли набрать нужное количество рабочих дней
            if working_days < original_duration:
                print(f"Сотрудник {employee['name']} не может выполнить задачу в срок с учетом выходных")
                continue

            # Теперь проверяем загрузку сотрудника
            try:
                adjusted_start_str = start.strftime('%Y-%m-%d')
                adjusted_end_str = end.strftime('%Y-%m-%d')

                employee_tasks = employee_manager.check_employee_workload(employee['id'], adjusted_start_str,
                                                                          adjusted_end_str)
                existing_load = sum(t.get('duration', 0) for t in employee_tasks)

                # Учитываем загрузку из текущего распределения
                daily_load = 0
                current = start
                while current <= end:
                    date_str = current.strftime('%Y-%m-%d')
                    daily_load += employee_daily_load.get(employee['id'], {}).get(date_str, 0)
                    current += timedelta(days=1)

                total_load = existing_load + daily_load

                # Если это лучший вариант, запоминаем
                if total_load < min_load:
                    min_load = total_load
                    best_employee = employee
                    best_start_date = start
                    best_end_date = end

            except Exception as e:
                print(f"Ошибка при подсчете загрузки сотрудника {employee['name']}: {str(e)}")
                continue

        # Если нашли подходящего сотрудника, назначаем его
        if best_employee:
            try:
                task_manager.assign_employee(task['id'], best_employee['id'])

                # Обновляем загрузку сотрудника
                current = best_start_date
                while current <= best_end_date:
                    date_str = current.strftime('%Y-%m-%d')
                    if date_str not in employee_daily_load[best_employee['id']]:
                        employee_daily_load[best_employee['id']][date_str] = 0
                    employee_daily_load[best_employee['id']][date_str] += 1
                    current += timedelta(days=1)

                # Обновляем даты задачи с учетом выходных
                adjusted_start_str = best_start_date.strftime('%Y-%m-%d')
                adjusted_end_str = best_end_date.strftime('%Y-%m-%d')

                # Если даты изменились, выводим информацию
                if adjusted_start_str != start_date or adjusted_end_str != end_date:
                    print(
                        f"Даты задачи '{task['name']}' скорректированы с учетом выходных сотрудника {best_employee['name']}:")
                    print(f"  Исходные даты: {start_date} - {end_date}")
                    print(f"  Новые даты: {adjusted_start_str} - {adjusted_end_str}")

                # Обновляем даты задачи в базе данных
                task_manager.update_task_dates({
                    task['id']: {
                        'start': adjusted_start_str,
                        'end': adjusted_end_str
                    }
                })

                print(f"Сотрудник {best_employee['name']} назначен на задачу {task['name']}")
                return best_employee['id']
            except Exception as e:
                print(f"Ошибка при назначении сотрудника {best_employee['name']} на задачу {task['name']}: {str(e)}")
                return None
        else:
            print(f"Не найдено подходящих сотрудников для задачи {task['name']}")
            return None

    except Exception as e:
        print(f"Общая ошибка при назначении сотрудника на задачу {task['name']}: {str(e)}")
        return None

@router.callback_query(lambda c: c.data.startswith("set_employee_"))
async def set_employee(callback: CallbackQuery):
    """Назначает сотрудника на задачу"""
    try:
        parts = callback.data.split("_")
        if len(parts) < 4:
            await callback.message.edit_text("Ошибка: некорректный формат данных.")
            return

        task_id = int(parts[2])
        employee_id = int(parts[3])

        # Получаем информацию о задаче
        task = task_manager.get_task(task_id)

        # Получаем информацию о сотруднике
        employee = employee_manager.get_employee(employee_id)

        # Назначаем сотрудника на задачу
        task_manager.assign_employee(task_id, employee_id)

        await callback.message.edit_text(
            f"Сотрудник {employee['name']} назначен на задачу '{task['name']}'.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="Вернуться к распределению",
                    callback_data=f"workload_{task['project_id']}"
                )]
            ])
        )

    except Exception as e:
        await callback.message.edit_text(f"Ошибка при назначении сотрудника: {str(e)}")


@router.callback_query(lambda c: c.data.startswith("unassign_employee_"))
async def unassign_employee(callback: CallbackQuery):
    """Снимает назначение сотрудника с задачи"""
    try:
        parts = callback.data.split("_")
        if len(parts) < 3:
            await callback.message.edit_text("Ошибка: некорректный формат данных.")
            return

        task_id = int(parts[2])

        # Получаем информацию о задаче
        task = task_manager.get_task(task_id)

        # Снимаем назначение
        task_manager.db.execute("UPDATE tasks SET employee_id = NULL WHERE id = ?", (task_id,))

        await callback.message.edit_text(
            f"Назначение на задачу '{task['name']}' снято.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="Вернуться к распределению",
                    callback_data=f"workload_{task['project_id']}"
                )]
            ])
        )

    except Exception as e:
        await callback.message.edit_text(f"Ошибка при снятии назначения: {str(e)}")


# -----------------------------------------------------------------------------
# Запуск бота
# -----------------------------------------------------------------------------

async def main():
    # Убедимся, что таблицы созданы
    db_manager.init_db()

    # Запускаем бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())