import os
import logging
import asyncio
import json
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
                        employee = employee_manager.get_employee(subtask['employee_id']) if subtask.get(
                            'employee_id') else None
                        employee_name = f"{employee['name']} ({employee['position']})" if employee else "Не назначен"
                        text += f"  ↳ {subtask['name']} - {employee_name}\n"
                else:
                    employee = employee_manager.get_employee(task['employee_id']) if task.get('employee_id') else None
                    text += f"- {employee['name']} ({employee['position']})" if employee else "- Не назначен\n"
        else:
            text += "Задач в проекте нет"

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

        # Создаем сетевую модель и рассчитываем критический путь
        result = network_model.calculate(project, tasks)

        # Обновляем даты начала и окончания задач
        task_manager.update_task_dates(result['task_dates'])

        # Автоматическое распределение задач по сотрудникам
        assignments = {}  # Словарь для хранения назначений (task_id -> employee_id)

        # Получаем список сотрудников
        all_employees = employee_manager.get_all_employees()
        employee_by_position = {}

        # Группируем сотрудников по должностям
        for employee in all_employees:
            position = employee['position']
            if position not in employee_by_position:
                employee_by_position[position] = []
            employee_by_position[position].append(employee)

        # Автоматическое распределение для каждой задачи
        for task in tasks:
            # Пропускаем групповые задачи и подзадачи
            if task['is_group'] or task.get('parent_id'):
                continue

            # Получаем даты задачи
            if task['id'] in result['task_dates']:
                task_dates = result['task_dates'][task['id']]
                start_date = task_dates['start']
                end_date = task_dates['end']
            else:
                continue  # Пропускаем задачи без дат

            # Определяем нужную должность
            position = task.get('position')
            if not position:
                continue  # Пропускаем задачи без указания должности

            # Находим подходящих сотрудников
            suitable_employees = employee_by_position.get(position, [])
            if not suitable_employees:
                continue

            # Находим наименее загруженного сотрудника на нужные даты
            best_employee = None
            min_load = float('inf')

            for employee in suitable_employees:
                # Проверяем доступность сотрудника на эти даты
                available = True

                # Проверяем выходные дни
                from datetime import datetime, timedelta
                start = datetime.strptime(start_date, '%Y-%m-%d')
                end = datetime.strptime(end_date, '%Y-%m-%d')
                current = start

                while current <= end:
                    date_str = current.strftime('%Y-%m-%d')
                    if not employee_manager.is_available(employee['id'], date_str):
                        available = False
                        break
                    current += timedelta(days=1)

                if not available:
                    continue

                # Подсчитываем текущую загрузку сотрудника
                employee_tasks = employee_manager.check_employee_workload(employee['id'], start_date, end_date)
                load = sum(t['duration'] for t in employee_tasks)

                # Если это лучший вариант, запоминаем
                if load < min_load:
                    min_load = load
                    best_employee = employee

            # Если нашли подходящего сотрудника, назначаем его
            if best_employee:
                task_manager.assign_employee(task['id'], best_employee['id'])
                assignments[task['id']] = best_employee['id']

        # Генерируем отчет
        text = f"Календарный план для проекта '{project['name']}'\n\n"

        # Вычисляем фактическую длительность проекта
        if result['task_dates']:
            start_dates = [datetime.datetime.strptime(dates['start'], '%Y-%m-%d') for dates in
                           result['task_dates'].values()]
            end_dates = [datetime.datetime.strptime(dates['end'], '%Y-%m-%d') for dates in
                         result['task_dates'].values()]

            if start_dates and end_dates:
                project_start = min(start_dates)
                project_end = max(end_dates)
                project_duration = (project_end - project_start).days
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
                text += f"• {task['name']} → {employee['name']}\n"
        else:
            text += "\nНе удалось автоматически распределить задачи. Используйте команду /employee_workload для ручного назначения."

        # Генерируем диаграмму Ганта
        gantt_image = gantt_chart.generate(project, tasks, result['task_dates'], result['critical_path'])

        # Отправляем текстовый отчет
        await callback.message.edit_text(text)

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

    except Exception as e:
        await callback.message.edit_text(f"Ошибка при расчете календарного плана: {str(e)}")


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

        # Создаем экспорт в Jira
        export_file = jira_exporter.export(project, tasks)

        # Отправляем файл пользователю
        file = FSInputFile(export_file)
        await bot.send_document(callback.from_user.id, file)

        buttons = [
            [InlineKeyboardButton(text="Назад к проекту", callback_data=f"view_project_{project_id}")]
        ]

        markup = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.edit_text("Экспорт в Jira успешно выполнен", reply_markup=markup)

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
async def assign_employee_to_task(callback: CallbackQuery, state: FSMContext):
    """Показывает список сотрудников для назначения на задачу"""
    try:
        # Безопасно получаем task_id
        parts = callback.data.split("_")
        if len(parts) < 3:
            await callback.message.edit_text("Ошибка: некорректный формат данных.")
            return

        task_id = int(parts[2])

        # Получаем информацию о задаче
        task = task_manager.get_task(task_id)

        # Получаем список подходящих сотрудников
        position = task.get('position')
        employees = []

        if position:
            employees = employee_manager.get_employees_by_position(position)
        else:
            employees = employee_manager.get_all_employees()

        if not employees:
            await callback.message.edit_text(
                f"Нет доступных сотрудников для назначения на задачу '{task['name']}'.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="Назад к списку задач",
                        callback_data=f"assign_to_project_{task['project_id']}"
                    )]
                ])
            )
            return

        # Создаем кнопки для сотрудников
        buttons = []
        for employee in employees:
            buttons.append([InlineKeyboardButton(
                text=f"{employee['name']} ({employee['position']})",
                callback_data=f"set_employee_{task_id}_{employee['id']}"
            )])

        # Добавляем кнопку для снятия назначения
        if task.get('employee_id'):
            buttons.append([InlineKeyboardButton(
                text="Снять назначение",
                callback_data=f"unassign_employee_{task_id}"
            )])

        # Добавляем кнопку возврата
        buttons.append([InlineKeyboardButton(
            text="Назад к списку задач",
            callback_data=f"assign_to_project_{task['project_id']}"
        )])

        markup = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.edit_text(
            f"Выберите сотрудника для задачи '{task['name']}':",
            reply_markup=markup
        )

    except Exception as e:
        await callback.message.edit_text(f"Ошибка при загрузке сотрудников: {str(e)}")


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