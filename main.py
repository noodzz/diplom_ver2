import logging
import asyncio
import json
import os
import tempfile
import datetime

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
from utils.helpers import parse_csv, format_date, is_authorized, is_admin
from utils.scheduler import schedule_project, update_database_assignments, balance_employee_workload

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=os.getenv("BOT_TOKEN"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

test_mode_active = False
original_employees_data = None

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

class AdminState(StatesGroup):
    waiting_for_user_id = State()

# -----------------------------------------------------------------------------
# Обработчики команд
# -----------------------------------------------------------------------------

@router.message(CommandStart())
async def cmd_start(message: Message):
    if not is_authorized(message.from_user.id, db_manager):
        user_id = message.from_user.id
        await message.answer(
            f"Извините, у вас нет доступа к этому боту.\n"
            f"Ваш ID: {user_id}\n"
            f"Обратитесь к администратору для получения доступа."
        )
        return

    welcome_text = (
        "👋 Добро пожаловать в бот для управления проектами!\n\n"
        "С моей помощью вы можете:\n"
        "• Создавать проекты\n"
        "• Рассчитывать оптимальный календарный план\n"
        "• Распределять задачи по сотрудникам\n"
        "• Экспортировать проекты в Jira\n\n"
        "Используйте /create_project, чтобы создать новый проект\n"
        "Или /list_projects, чтобы увидеть список существующих проектов.\n\n"
        "Дополнительная помощь: /help"
    )
    await message.answer(welcome_text)


@router.message(Command("help"))
async def cmd_help(message: Message):
    help_text = (
        "Доступные команды:\n"
        "/create_project - Создать новый проект из шаблона или CSV\n"
        "/list_projects - Список всех проектов\n"
        "/help - Показать эту справку\n\n"
        "/cancel - Отменить текущую операцию\n\n"
        "Рабочий процесс:\n"
        "1. Создайте проект с помощью шаблона или CSV-файла\n"
        "2. Рассчитайте календарный план\n"
        "3. Просмотрите распределение задач по сотрудникам\n" 
        "4. При необходимости экспортируйте проект в Jira"
    )
    await message.answer(help_text)


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    """Отменяет текущую операцию и очищает состояние"""
    # Проверяем, есть ли активное состояние
    current_state = await state.get_state()

    if current_state is None:
        # Если нет активного состояния
        await message.answer("Нет активной операции для отмены.")
        return

    # Если есть состояние, очищаем его и сообщаем пользователю
    await state.clear()

    # Выводим разное сообщение в зависимости от того, какое состояние было активно
    if current_state.startswith('ProjectState:'):
        await message.answer("✅ Создание проекта отменено. Что бы вы хотели сделать дальше?")
    elif current_state.startswith('TaskState:'):
        await message.answer("✅ Добавление задачи отменено.")
    elif current_state.startswith('AdminState:'):
        await message.answer("✅ Административная операция отменена.")
    else:
        await message.answer("✅ Операция отменена.")

    # Предлагаем основные команды
    help_text = (
        "Доступные команды:\n"
        "/create_project - Создать новый проект\n"
        "/list_projects - Список всех проектов\n"
        "/help - Показать справку"
    )
    await message.answer(help_text)

# -----------------------------------------------------------------------------
# Административные функции
# -----------------------------------------------------------------------------

@router.message(Command("admin"))
async def cmd_admin(message: Message):
    """Показывает административное меню"""
    if not is_admin(message.from_user.id, db_manager):
        await message.answer("У вас нет прав администратора.")
        return

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Управление пользователями", callback_data="admin_users")],
        [InlineKeyboardButton(text="Статистика бота", callback_data="admin_stats")]
    ])

    await message.answer("Административное меню:", reply_markup=markup)


@router.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    """Показывает список пользователей"""
    if not is_admin(callback.from_user.id, db_manager):
        await callback.answer("У вас нет прав администратора.")
        return

    users = db_manager.get_all_users()

    text = "Список пользователей:\n\n"

    # Формируем кнопки для каждого пользователя
    buttons = []
    for user in users:
        status = "✅ Активен" if user['is_active'] else "❌ Заблокирован"
        role = "🔑 Администратор" if user['is_admin'] else "👤 Пользователь"
        text += f"ID: {user['id']} - {status}, {role}\n"

        # Добавляем кнопки управления для каждого пользователя (кроме текущего админа)
        if user['id'] != callback.from_user.id:
            action = "block" if user['is_active'] else "unblock"
            label = "🔒 Заблокировать" if user['is_active'] else "🔓 Разблокировать"
            buttons.append([InlineKeyboardButton(
                text=f"{label} пользователя {user['id']}",
                callback_data=f"user_{action}_{user['id']}"
            )])

    # Добавляем кнопки добавления и возврата
    buttons.append([InlineKeyboardButton(text="➕ Добавить пользователя", callback_data="add_user")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text(text, reply_markup=markup)


# Обработчик для блокировки/разблокировки пользователя
@router.callback_query(lambda c: c.data.startswith("user_block_") or c.data.startswith("user_unblock_"))
async def toggle_user_status(callback: CallbackQuery):
    if not is_admin(callback.from_user.id, db_manager):
        await callback.answer("У вас нет прав администратора.")
        return

    parts = callback.data.split("_")
    action = parts[1]  # "block" или "unblock"
    user_id = int(parts[2])

    # Меняем статус пользователя
    is_active = action == "unblock"  # True если разблокировка, False если блокировка
    db_manager.update_user(user_id, is_active=is_active)

    action_text = "разблокирован" if is_active else "заблокирован"
    await callback.answer(f"Пользователь {user_id} {action_text}!")

    # Обновляем список пользователей
    await admin_users(callback)


@router.callback_query(F.data == "add_user")
async def add_user_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id, db_manager):
        await callback.answer("У вас нет прав администратора.")
        return

    await callback.message.edit_text(
        "Введите Telegram ID пользователя, которого хотите добавить:"
    )
    await state.set_state(AdminState.waiting_for_user_id)


@router.message(AdminState.waiting_for_user_id)
async def process_new_user_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id, db_manager):
        await message.answer("У вас нет прав администратора.")
        await state.clear()
        return

    try:
        user_id = int(message.text.strip())

        # Проверяем, существует ли уже такой пользователь
        existing_user = db_manager.get_user(user_id)

        if existing_user:
            await message.answer(
                f"Пользователь с ID {user_id} уже существует.\n\n"
                f"Статус: {'Активен' if existing_user['is_active'] else 'Заблокирован'}\n"
                f"Роль: {'Администратор' if existing_user['is_admin'] else 'Пользователь'}"
            )
        else:
            # Добавляем нового пользователя
            db_manager.add_user(user_id, name=f"User_{user_id}", is_admin=0)
            await message.answer(f"Пользователь с ID {user_id} успешно добавлен!")

        # Возвращаемся к списку пользователей
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Вернуться к списку пользователей", callback_data="admin_users")]
        ])
        await message.answer("Что дальше?", reply_markup=markup)

    except ValueError:
        await message.answer("Ошибка: ID пользователя должен быть числом. Попробуйте еще раз:")

    await state.clear()


@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    """Показывает статистику использования бота"""
    if not is_admin(callback.from_user.id, db_manager):
        await callback.answer("У вас нет прав администратора.")
        return

    try:
        # Получаем статистику по проектам
        total_projects = db_manager.execute("SELECT COUNT(*) FROM projects")[0][0]
        active_projects = db_manager.execute("SELECT COUNT(*) FROM projects WHERE status = 'active'")[0][0]

        # Статистика по задачам
        total_tasks = db_manager.execute("SELECT COUNT(*) FROM tasks")[0][0]
        group_tasks = db_manager.execute("SELECT COUNT(*) FROM tasks WHERE is_group = 1")[0][0]
        subtasks = db_manager.execute("SELECT COUNT(*) FROM tasks WHERE parent_id IS NOT NULL")[0][0]

        # Статистика по пользователям
        total_users = db_manager.execute("SELECT COUNT(*) FROM users")[0][0]
        active_users = db_manager.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")[0][0]
        admin_users = db_manager.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")[0][0]

        # Распределение проектов по пользователям
        projects_by_user = db_manager.execute("""
            SELECT u.id, u.name, COUNT(p.id) as project_count 
            FROM users u 
            LEFT JOIN projects p ON u.id = p.user_id 
            GROUP BY u.id 
            ORDER BY project_count DESC
        """)

        # Последняя активность
        last_project = db_manager.execute(
            "SELECT name, created_at FROM projects ORDER BY created_at DESC LIMIT 1"
        )

        # Формируем отчёт
        stats_text = "📊 **СТАТИСТИКА БОТА**\n\n"

        stats_text += "**Проекты:**\n"
        stats_text += f"• Всего проектов: {total_projects}\n"
        stats_text += f"• Активных проектов: {active_projects}\n"

        stats_text += "\n**Задачи:**\n"
        stats_text += f"• Всего задач: {total_tasks}\n"
        stats_text += f"• Групповых задач: {group_tasks}\n"
        stats_text += f"• Подзадач: {subtasks}\n"

        stats_text += "\n**Пользователи:**\n"
        stats_text += f"• Всего пользователей: {total_users}\n"
        stats_text += f"• Активных пользователей: {active_users}\n"
        stats_text += f"• Администраторов: {admin_users}\n"

        stats_text += "\n**Распределение проектов по пользователям:**\n"
        for user_data in projects_by_user:
            user_id, user_name, count = user_data
            stats_text += f"• {user_name or f'User_{user_id}'}: {count} проект(ов)\n"

        if last_project:
            project_name, created_at = last_project[0]
            stats_text += f"\n**Последний созданный проект:**\n• {project_name} ({created_at})\n"

        # Добавляем техническую информацию
        import platform
        import psutil

        stats_text += "\n**Системная информация:**\n"
        stats_text += f"• ОС: {platform.system()} {platform.release()}\n"
        stats_text += f"• Python: {platform.python_version()}\n"

        try:
            process = psutil.Process(os.getpid())
            memory_usage = process.memory_info().rss / 1024 / 1024  # в МБ
            stats_text += f"• Использование памяти: {memory_usage:.2f} МБ\n"
            stats_text += f"• Время работы бота: {(datetime.datetime.now() - datetime.datetime.fromtimestamp(process.create_time())).total_seconds() / 3600:.2f} ч\n"
        except:
            stats_text += "• Данные о системных ресурсах недоступны\n"

        # Кнопка для возврата
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")]
        ])

        await callback.message.edit_text(stats_text, reply_markup=markup)

    except Exception as e:
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")]
        ])
        await callback.message.edit_text(
            f"Ошибка при получении статистики: {str(e)}",
            reply_markup=markup
        )


@router.callback_query(F.data == "admin")
async def back_to_admin(callback: CallbackQuery):
    """Возвращает к административному меню"""
    if not is_admin(callback.from_user.id, db_manager):
        await callback.answer("У вас нет прав администратора.")
        return

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Управление пользователями", callback_data="admin_users")],
        [InlineKeyboardButton(text="📊 Статистика бота", callback_data="admin_stats")]
    ])

    await callback.message.edit_text("Административное меню:", reply_markup=markup)


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
    start_date = message.text.strip()

    # Проверяем корректность формата даты
    try:
        # Пытаемся распарсить дату
        datetime.datetime.strptime(start_date, '%Y-%m-%d')

        # Если дата корректна, сохраняем её и предлагаем выбор типа проекта
        await state.update_data(start_date=start_date)

        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Использовать шаблон", callback_data="use_template")],
            [InlineKeyboardButton(text="Загрузить из CSV", callback_data="upload_csv")],
        ])

        await message.answer("Как вы хотите создать проект?", reply_markup=markup)
        await state.set_state(ProjectState.waiting_for_choice)
    except ValueError:
        # Если дата некорректна, сообщаем об ошибке и просим ввести дату снова
        await message.answer(
            "❌ Некорректный формат даты. Пожалуйста, введите дату в формате YYYY-MM-DD (например, 2025-05-14)."
        )
        # Остаемся в том же состоянии для повторного ввода


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
        user_id = callback.from_user.id
        print(f"Создание проекта из шаблона. Пользователь ID: {user_id}")
        project_id = project_manager.create_from_template(
            user_data['project_name'],
            user_data['start_date'],
            template_id,
            user_id=user_id
        )

        # Создаем кнопку для перехода к проекту
        buttons = [
            [InlineKeyboardButton(text="📂 Открыть проект", callback_data=f"view_project_{project_id}")],
            [InlineKeyboardButton(text="📋 Список всех проектов", callback_data="back_to_projects")]
        ]
        markup = InlineKeyboardMarkup(inline_keyboard=buttons)

        await callback.message.edit_text(
            f"✅ Проект '{user_data['project_name']}' успешно создан из шаблона!\n\n"
            f"ID проекта: {project_id}\n\n"
            f"Все задачи из шаблона добавлены в проект. Теперь вы можете просмотреть и отредактировать задачи, "
            f"или рассчитать календарный план.",
            reply_markup=markup
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
        "- Параллельная - Для подзадач указывается, могут ли они выполняться параллельно (да/нет)\n"
        "\n"
        "Шаблон для задачи можете найти по ссылке: https://docs.google.com/spreadsheets/d/1n-He466tyHoeZVLSUfI8A4YuXfCdf9W7yLyrT8v2ZI8/edit?gid=0#gid=0"
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

        # Исправляем эту строку: используем message вместо callback
        user_id = message.from_user.id
        print(f"Создание проекта из CSV. Пользователь ID: {user_id}")

        project_id = project_manager.create_from_csv(
            user_data['project_name'],
            user_data['start_date'],
            project_data,
            user_id=user_id
        )

        # Создаем кнопку для перехода к проекту
        buttons = [
            [InlineKeyboardButton(text="📂 Открыть проект", callback_data=f"view_project_{project_id}")],
            [InlineKeyboardButton(text="📋 Список всех проектов", callback_data="back_to_projects")]
        ]
        markup = InlineKeyboardMarkup(inline_keyboard=buttons)

        await message.answer(
            f"✅ Проект '{user_data['project_name']}' успешно создан из CSV!\n\n"
            f"ID проекта: {project_id}\n\n"
            f"Загружено {len(project_data)} задач. Теперь вы можете просмотреть проект и рассчитать календарный план.",
            reply_markup=markup
        )
    except Exception as e:
        await message.answer(f"Ошибка при обработке CSV: {str(e)}")

    await state.clear()

@router.message(Command("list_projects"))
async def cmd_list_projects(message: Message):
    if not is_authorized(message.from_user.id):
        return

    projects = project_manager.get_all_projects(user_id=message.from_user.id)

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
                [InlineKeyboardButton(text="📊 Рассчитать календарный план", callback_data=f"calculate_{project_id}")],
                [InlineKeyboardButton(text="👥 Распределение по сотрудникам", callback_data=f"workload_{project_id}")],
                [InlineKeyboardButton(text="🔄 Экспорт в Jira", callback_data=f"export_jira_{project_id}")],
                [InlineKeyboardButton(text="⬅️ Назад к списку проектов", callback_data="back_to_projects")]
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
                [InlineKeyboardButton(text="📊 Рассчитать календарный план", callback_data=f"calculate_{project_id}")],
                [InlineKeyboardButton(text="👥 Распределение по сотрудникам", callback_data=f"workload_{project_id}")],
                [InlineKeyboardButton(text="🔄 Экспорт в Jira", callback_data=f"export_jira_{project_id}")],
                [InlineKeyboardButton(text="⬅️ Назад к списку проектов", callback_data="back_to_projects")]
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
    """
    Обработчик для расчета календарного плана проекта.
    Выполняет расчет дат задач, критического пути и распределения сотрудников.
    Генерирует отчет, диаграмму Ганта и предоставляет результаты пользователю.
    """
    project_id = int(callback.data.split("_")[1])

    await callback.message.edit_text("Выполняется расчет календарного плана и распределение задач...")

    try:
        # Получаем данные о проекте и задачах
        project = project_manager.get_project_details(project_id)
        tasks = task_manager.get_tasks_by_project(project_id)

        # Дополнительно получаем все задачи проекта, включая подзадачи
        all_tasks = task_manager.get_all_tasks_by_project(project_id)
        print(f"Получено {len(tasks)} основных задач и {len(all_tasks)} задач всего (включая подзадачи)")

        # Выполняем расчет календарного плана с учетом выходных дней
        result = schedule_project(project, tasks, task_manager, employee_manager)

        # Создаем словарь задач для передачи в функцию балансировки
        task_map = {}
        for task in all_tasks:  # Используем все задачи, включая подзадачи
            task_id = task['id']
            task_map[task_id] = task
            # Также добавляем строковое представление ID
            task_map[str(task_id)] = task

        print(f"Создан словарь task_map с {len(task_map)} задачами")

        # Балансируем нагрузку между сотрудниками
        balanced_task_dates = balance_employee_workload(result['task_dates'], task_map, employee_manager)

        # Обновляем результаты с учетом балансировки
        result['task_dates'] = balanced_task_dates

        # Обновляем назначения и даты в базе данных с улучшенной функцией
        update_result = update_database_assignments(result['task_dates'], task_manager, employee_manager)
        print(f"Обновлено {update_result} записей в базе данных")

        # Формируем результаты для отображения
        task_dates = result['task_dates']
        critical_path = result['critical_path']
        duration = result['duration']

        # Отладочная информация
        print(f"Критический путь: {critical_path}")
        print(f"Длительность проекта: {duration} дней")
        print(f"Рассчитаны даты для {len(task_dates)} задач")

        # Генерируем отчет
        print("Формирование отчета...")
        text = generate_planning_report(project, tasks, result, task_manager, employee_manager)

        # Безопасное имя файла для отчета
        safe_project_name = "".join(c if c.isalnum() or c in [' ', '.', '_', '-'] else '_' for c in project['name'])
        temp_dir = tempfile.mkdtemp()
        report_file_path = os.path.join(temp_dir, f"{safe_project_name}_report.txt")

        # Записываем отчет в файл
        with open(report_file_path, 'w', encoding='utf-8') as file:
            file.write(text)

        # Генерируем диаграмму Ганта
        try:
            gantt_image = gantt_chart.generate(project, tasks, result['task_dates'], result['critical_path'])
            has_gantt = True
        except Exception as e:
            print(f"Ошибка при создании диаграммы Ганта: {str(e)}")
            import traceback
            print(traceback.format_exc())
            gantt_image = None
            has_gantt = False

        # Отправляем краткое сообщение
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

        # Отправляем диаграмму Ганта, если она была создана
        if has_gantt and gantt_image:
            gantt_file = FSInputFile(gantt_image)
            await bot.send_photo(
                callback.from_user.id,
                gantt_file,
                caption=f"Диаграмма Ганта для проекта '{project['name']}'",
            )

        # Отправляем кнопки для дальнейших действий
        buttons = [
            [InlineKeyboardButton(text="Просмотреть распределение", callback_data=f"workload_{project_id}")],
            [InlineKeyboardButton(text="Назад к проекту", callback_data=f"view_project_{project_id}")]
        ]

        markup = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.reply("Расчет календарного плана и распределение задач завершены", reply_markup=markup)

        # Очистка временных файлов
        try:
            if os.path.exists(report_file_path):
                os.remove(report_file_path)
            if gantt_image and os.path.exists(gantt_image):
                os.remove(gantt_image)
        except Exception as e:
            print(f"Ошибка при очистке временных файлов: {str(e)}")
    except Exception as e:
        import traceback
        error_msg = f"Ошибка при расчете календарного плана: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        await callback.message.edit_text(f"Ошибка при расчете календарного плана: {str(e)}")
        return

def generate_planning_report(project, tasks, result, task_manager, employee_manager):
    """
    Генерирует отчет о результатах планирования

    Args:
        project (dict): Информация о проекте
        tasks (list): Список задач
        result (dict): Результаты планирования
        task_manager: Менеджер задач
        employee_manager: Менеджер сотрудников

    Returns:
        str: Текст отчета
    """
    import datetime

    task_dates = result['task_dates']
    critical_path = result['critical_path']
    duration = result['duration']

    # Заголовок отчета
    text = f"📊 ОТЧЕТ ПО КАЛЕНДАРНОМУ ПЛАНУ\n"
    text += f"=============================================\n\n"
    text += f"📋 ОБЩАЯ ИНФОРМАЦИЯ О ПРОЕКТЕ\n"
    text += f"Название проекта: '{project['name']}'\n"

    # Вывод диагностической информации
    print(f"Генерация отчета для проекта {project['name']}")
    print(f"Данные задач: {len(tasks)} задач")
    print(f"Данные дат: {len(task_dates)} записей")

    # Вычисляем фактическую длительность проекта
    if task_dates:
        # Находим самую раннюю дату начала и самую позднюю дату окончания
        start_dates = []
        end_dates = []

        for dates in task_dates.values():
            if 'start' in dates:
                start_dates.append(datetime.datetime.strptime(dates['start'], '%Y-%m-%d'))
            if 'end' in dates:
                end_dates.append(datetime.datetime.strptime(dates['end'], '%Y-%m-%d'))

        if start_dates and end_dates:
            project_start = min(start_dates)
            project_end = max(end_dates)
            project_duration = (project_end - project_start).days + 1

            text += f"Длительность проекта: {project_duration} дней\n"
            text += f"Дата начала: {project_start.strftime('%d.%m.%Y')}\n"
            text += f"Дата завершения: {project_end.strftime('%d.%m.%Y')}\n\n"
        else:
            text += f"Длительность проекта: {duration} дней\n\n"
    else:
        text += f"Длительность проекта: {duration} дней\n\n"

    text += f"Общее количество задач: {len(tasks)}\n\n"

    # Критический путь
    text += f"🚩 КРИТИЧЕСКИЙ ПУТЬ\n"
    text += f"Критический путь — последовательность задач, определяющая длительность проекта.\n"
    text += f"Задержка любой из этих задач приведет к задержке всего проекта.\n\n"

    text += f"Примечание: Все даты указаны включительно. Например, задача с датами '19.05.2025 - 21.05.2025' "
    text += f"выполняется с начала 19.05 до конца 21.05.\n\n"

    if critical_path:
        critical_tasks = []
        total_critical_days = 0

        for task_id in critical_path:
            try:
                # Пробуем и с числовым, и со строковым ID
                task = None
                if isinstance(task_id, str) and task_id.isdigit():
                    task = task_manager.get_task(int(task_id))
                else:
                    task = task_manager.get_task(task_id)

                if task:
                    critical_tasks.append(task)
                    total_critical_days += task.get('duration', 0)

                    # Форматируем даты для отображения
                    start_date = "?"
                    end_date = "?"

                    # Пробуем разные варианты ключей для task_dates
                    if task_id in task_dates:
                        if 'start' in task_dates[task_id]:
                            start_date = format_date(task_dates[task_id]['start'])
                        if 'end' in task_dates[task_id]:
                            end_date = format_date(task_dates[task_id]['end'])
                    elif str(task_id) in task_dates:
                        if 'start' in task_dates[str(task_id)]:
                            start_date = format_date(task_dates[str(task_id)]['start'])
                        if 'end' in task_dates[str(task_id)]:
                            end_date = format_date(task_dates[str(task_id)]['end'])
                    elif task.get('start_date') and task.get('end_date'):
                        # Используем даты из задачи, если они есть
                        start_date = format_date(task['start_date'])
                        end_date = format_date(task['end_date'])

                    # Добавляем информацию о задаче
                    text += f"• {task['name']} ({task.get('duration', 0)} дн.)\n"
                    text += f"  Даты: {start_date} - {end_date}\n"

                    # Добавляем информацию о сотруднике, если назначен
                    employee_id = task.get('employee_id')
                    if not employee_id and task_id in task_dates:
                        employee_id = task_dates[task_id].get('employee_id')
                    elif not employee_id and str(task_id) in task_dates:
                        employee_id = task_dates[str(task_id)].get('employee_id')

                    if employee_id:
                        try:
                            employee = employee_manager.get_employee(employee_id)
                            text += f"  Исполнитель: {employee['name']} ({employee['position']})\n"
                        except Exception as e:
                            print(f"Ошибка при получении данных сотрудника {employee_id}: {str(e)}")
                    text += "\n"
            except Exception as e:
                print(f"Ошибка при обработке задачи {task_id} критического пути: {str(e)}")

        text += f"Суммарная длительность критического пути: {total_critical_days} дней\n\n"
    else:
        text += "Критический путь не определен. Возможные причины:\n"
        text += "• Недостаточно связей между задачами\n"
        text += "• Все задачи могут выполняться независимо\n"
        text += "• Задачи с наибольшей длительностью: "

        # Находим самые длинные задачи
        sorted_tasks = sorted(tasks, key=lambda t: t.get('duration', 0), reverse=True)
        long_tasks = [t['name'] for t in sorted_tasks[:3] if t.get('duration', 0) > 0]

        if long_tasks:
            text += ", ".join(long_tasks) + "\n\n"
        else:
            text += "не найдены\n\n"

    # Распределение задач по сотрудникам
    text += f"👥 РАСПРЕДЕЛЕНИЕ ЗАДАЧ\n"

    # Группируем задачи по сотрудникам
    employees_tasks = {}
    for task_id, dates in task_dates.items():
        # Определяем ID сотрудника
        employee_id = None
        if 'employee_id' in dates:
            employee_id = dates['employee_id']

        # Если ID сотрудника не в dates, пытаемся получить его из task
        if not employee_id:
            # Конвертируем task_id в числовой формат, если необходимо
            try:
                numeric_task_id = int(task_id) if isinstance(task_id, str) else task_id
                task = task_manager.get_task(numeric_task_id)
                if task and 'employee_id' in task:
                    employee_id = task['employee_id']
            except Exception as e:
                print(f"Ошибка при получении задачи {task_id}: {str(e)}")

        if employee_id:
            if employee_id not in employees_tasks:
                employees_tasks[employee_id] = []

            try:
                # Получаем задачу по ID
                numeric_task_id = int(task_id) if isinstance(task_id, str) else task_id
                task = task_manager.get_task(numeric_task_id)
                if task:
                    employees_tasks[employee_id].append(task)
            except Exception as e:
                print(f"Ошибка при получении задачи {task_id}: {str(e)}")

    if employees_tasks:
        # Для каждого сотрудника выводим его задачи
        for employee_id, emp_tasks in employees_tasks.items():
            try:
                employee = employee_manager.get_employee(employee_id)
                text += f"{employee['name']} ({employee['position']}):\n"

                # Сортируем задачи по датам
                sorted_tasks = sorted(
                    emp_tasks,
                    key=lambda t: task_dates.get(str(t['id']), {}).get('start', '9999-12-31') if str(
                        t['id']) in task_dates else
                    task_dates.get(t['id'], {}).get('start', '9999-12-31') if t['id'] in task_dates else
                    t.get('start_date', '9999-12-31')
                )

                total_load = 0
                for task in sorted_tasks:
                    # Получаем даты задачи
                    start_date = "?"
                    end_date = "?"

                    # Ищем даты в разных источниках
                    if task['id'] in task_dates:
                        if 'start' in task_dates[task['id']]:
                            start_date = format_date(task_dates[task['id']]['start'])
                        if 'end' in task_dates[task['id']]:
                            end_date = format_date(task_dates[task['id']]['end'])
                    elif str(task['id']) in task_dates:
                        if 'start' in task_dates[str(task['id'])]:
                            start_date = format_date(task_dates[str(task['id'])]['start'])
                        if 'end' in task_dates[str(task['id'])]:
                            end_date = format_date(task_dates[str(task['id'])]['end'])
                    elif task.get('start_date') and task.get('end_date'):
                        # Используем даты из задачи
                        start_date = format_date(task['start_date'])
                        end_date = format_date(task['end_date'])

                    # Выводим информацию о задаче
                    task_duration = task.get('duration', 0)
                    total_load += task_duration

                    if task.get('parent_id'):
                        # Для подзадач показываем родительскую задачу
                        try:
                            parent_task = task_manager.get_task(task['parent_id'])
                            parent_name = parent_task['name'] if parent_task else "Неизвестная задача"
                            text += f"  • {parent_name} → {task['name']} ({task_duration} дн.)\n"
                        except Exception as e:
                            print(f"Ошибка при получении родительской задачи {task['parent_id']}: {str(e)}")
                            text += f"  • {task['name']} ({task_duration} дн.)\n"
                    else:
                        text += f"  • {task['name']} ({task_duration} дн.)\n"

                    text += f"    Даты: {start_date} - {end_date}\n"

                # Суммарная нагрузка сотрудника
                text += f"  Общая нагрузка: {total_load} дней\n\n"

            except Exception as e:
                print(f"Ошибка при обработке сотрудника {employee_id}: {str(e)}")
    else:
        text += "\nНе удалось автоматически распределить задачи.\n"
        text += "Возможные причины:\n"
        text += "• Не указаны должности для задач\n"
        text += "• Нет доступных сотрудников с требуемыми должностями\n"
        text += "• Слишком много выходных дней у сотрудников\n\n"

    # Рекомендации
    text += f"📝 РЕКОМЕНДАЦИИ\n"
    text += f"1. Обратите особое внимание на задачи критического пути\n"
    text += f"2. При необходимости перераспределите нагрузку между сотрудниками\n"
    text += f"3. Для сокращения сроков выполнения проекта оптимизируйте критические задачи\n\n"

    # Подпись
    text += f"=============================================\n"
    text += f"Отчет сгенерирован {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
    text += f"Система автоматизированного календарного планирования"

    return text


# Вспомогательная функция для форматирования даты
def format_date(date_str):
    """
    Форматирует дату для отображения

    Args:
        date_str (str): Дата в формате YYYY-MM-DD

    Returns:
        str: Отформатированная дата (DD.MM.YYYY)
    """
    if not date_str:
        return "Не указана"

    try:
        import datetime
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        return date.strftime('%d.%m.%Y')
    except ValueError:
        return date_str

def assign_task_with_days_off(task, project_start_date, employee_manager, suitable_employees, employee_workload):
    """
    Назначает сотрудника на задачу с учетом выходных дней

    Args:
        task (dict): Задача для назначения
        project_start_date (str): Дата начала проекта в формате '%Y-%m-%d'
        employee_manager: Менеджер сотрудников для проверки доступности
        suitable_employees (list): Список подходящих сотрудников
        employee_workload (dict): Словарь загрузки сотрудников

    Returns:
        tuple: (employee_id, start_date, end_date, calendar_duration) - ID назначенного сотрудника,
              новая дата начала, новая дата окончания и фактическая длительность в календарных днях
    """
    import datetime

    print(f"Назначение сотрудника на задачу '{task['name']}' с учетом выходных дней")

    # Преобразуем дату начала проекта в объект datetime
    try:
        start_date = datetime.datetime.strptime(project_start_date, '%Y-%m-%d')
    except (ValueError, TypeError):
        # Если дата некорректна, используем текущую
        start_date = datetime.datetime.now()

    duration = task.get('duration', 1)  # Длительность задачи в рабочих днях

    # Находим наиболее подходящего сотрудника с учетом выходных дней
    best_employee = None
    best_start_date = start_date
    best_end_date = None
    best_calendar_duration = float('inf')

    for employee in suitable_employees:
        # Проверяем доступность сотрудника на каждый день с учетом выходных
        employee_id = employee['id']
        current_date = start_date
        working_days = 0
        calendar_days = 0

        # Максимальное количество дней для поиска (защита от бесконечного цикла)
        max_days = duration * 3  # Берем с запасом

        while working_days < duration and calendar_days < max_days:
            date_str = current_date.strftime('%Y-%m-%d')

            # Проверяем, является ли этот день рабочим для сотрудника
            is_available = employee_manager.is_available(employee_id, date_str)

            if is_available:
                working_days += 1

            # Увеличиваем счетчик календарных дней и переходим к следующему дню
            calendar_days += 1
            current_date = current_date + datetime.timedelta(days=1)

        # Если удалось набрать нужное количество рабочих дней
        if working_days == duration:
            # Вычисляем дату окончания (последний рабочий день)
            end_date = current_date - datetime.timedelta(days=1)

            # Учитываем текущую загрузку сотрудника
            current_load = employee_workload.get(employee_id, 0)

            # Предпочитаем сотрудника с меньшей загрузкой
            # и с меньшей календарной длительностью для задачи
            if (best_employee is None or
                    current_load < employee_workload.get(best_employee['id'], 0) or
                    (current_load == employee_workload.get(best_employee['id'], 0) and
                     calendar_days < best_calendar_duration)):
                best_employee = employee
                best_calendar_duration = calendar_days
                best_end_date = end_date
        else:
            print(f"Сотрудник {employee['name']} не может выполнить задачу из-за выходных дней")

    if best_employee:
        # Обновляем загрузку выбранного сотрудника
        employee_workload[best_employee['id']] = employee_workload.get(best_employee['id'], 0) + duration

        print(f"Задача '{task['name']}' назначена сотруднику {best_employee['name']}")
        print(f"  Начало: {best_start_date.strftime('%Y-%m-%d')}")
        print(f"  Окончание: {best_end_date.strftime('%Y-%m-%d')}")
        print(f"  Рабочих дней: {duration}")
        print(f"  Календарных дней: {best_calendar_duration}")

        return (best_employee['id'],
                best_start_date.strftime('%Y-%m-%d'),
                best_end_date.strftime('%Y-%m-%d'),
                best_calendar_duration)
    else:
        print(f"Не удалось назначить сотрудника на задачу '{task['name']}' с учетом выходных дней")
        return None, None, None, None


def calculate_project_duration(start_date_str, task_dates):
    """Рассчитывает общую длительность проекта в днях"""
    import datetime

    # Если нет задач с датами, возвращаем 0
    if not task_dates:
        return 0

    # Преобразуем строковую дату в объект datetime
    try:
        project_start = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
    except (ValueError, TypeError):
        return 0

    # Находим самую позднюю дату окончания
    latest_end_date = None

    for task_id, dates in task_dates.items():
        if 'end' in dates:
            try:
                end_date = datetime.datetime.strptime(dates['end'], '%Y-%m-%d')
                if latest_end_date is None or end_date > latest_end_date:
                    latest_end_date = end_date
            except (ValueError, TypeError):
                continue

    # Если не нашли дату окончания, возвращаем 0
    if latest_end_date is None:
        return 0

    # Рассчитываем длительность в днях
    duration = (latest_end_date - project_start).days + 1  # +1 так как включаем день окончания
    return duration


def calculate_critical_path(task_dates, tasks, task_manager):
    """
    Вычисляет критический путь проекта

    Args:
        task_dates (dict): Словарь с датами начала и окончания задач
        tasks (list): Список задач проекта
        task_manager: Менеджер задач

    Returns:
        list: Список ID задач, образующих критический путь
    """
    import datetime

    # Если нет данных о датах, возвращаем пустой список
    if not task_dates:
        return []

    # Ищем самую позднюю дату окончания проекта
    latest_end_date = None
    latest_task_id = None

    for task_id, dates in task_dates.items():
        if 'end' in dates:
            try:
                end_date = datetime.datetime.strptime(dates['end'], '%Y-%m-%d')
                if latest_end_date is None or end_date > latest_end_date:
                    latest_end_date = end_date
                    latest_task_id = task_id
            except (ValueError, TypeError):
                continue

    # Если не нашли последнюю задачу, возвращаем пустой список
    if latest_task_id is None:
        return []

    # Находим путь от последней задачи к начальным задачам
    critical_path = []
    current_task_id = latest_task_id

    while current_task_id is not None:
        critical_path.append(current_task_id)

        # Находим предшественников текущей задачи
        dependencies = task_manager.get_task_dependencies(current_task_id)

        if not dependencies:
            # Это начальная задача, путь построен
            break

        # Ищем предшественника с самой поздней датой окончания
        latest_predecessor_id = None
        latest_predecessor_end = None

        for dep in dependencies:
            predecessor_id = dep['predecessor_id']
            if predecessor_id in task_dates and 'end' in task_dates[predecessor_id]:
                try:
                    end_date = datetime.datetime.strptime(task_dates[predecessor_id]['end'], '%Y-%m-%d')
                    if latest_predecessor_end is None or end_date > latest_predecessor_end:
                        latest_predecessor_end = end_date
                        latest_predecessor_id = predecessor_id
                except (ValueError, TypeError):
                    continue

        # Переходим к предшественнику или завершаем, если предшественников нет
        current_task_id = latest_predecessor_id

    # Возвращаем критический путь в обратном порядке (от начала к концу)
    return list(reversed(critical_path))

# -----------------------------------------------------------------------------
# Экспорт в Jira
# -----------------------------------------------------------------------------

@router.callback_query(lambda c: c.data.startswith("export_jira_"))
async def export_to_jira(callback: CallbackQuery):
    project_id = int(callback.data.split("_")[2])

    await callback.message.edit_text("Выполняется экспорт в Jira...")

    try:
        project = project_manager.get_project_details(project_id)
        tasks = task_manager.get_all_tasks_by_project(project_id)

        # Пробуем прямую интеграцию с Jira API
        result = jira_exporter.import_to_jira(project, tasks, employee_manager)

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

    # Получаем данные о проекте
    project = project_manager.get_project_details(project_id)

    await show_workload_report(callback, project_id, employee_manager, project, task_manager)


async def show_workload_report(callback, project_id, employee_manager, project, task_manager):
    """
    Отображает отчет о распределении задач с учетом ограничений Telegram

    Args:
        callback: Callback от Telegram
        project_id: ID проекта
        employee_manager: Менеджер сотрудников
        project: Информация о проекте
        task_manager: Менеджер задач
    """
    try:
        # Выводим диагностическую информацию о задачах в проекте
        print(f"Показ отчета о распределении для проекта {project_id}: {project['name']}")

        # Получаем все задачи проекта из базы данных
        all_tasks = task_manager.get_all_tasks_by_project(project_id)
        print(f"Всего задач в проекте: {len(all_tasks)}")

        # Для диагностики выводим информацию о датах задач
        for task in all_tasks:
            print(
                f"Задача {task['id']}: {task['name']} - даты из БД: {task.get('start_date', 'Н/Д')} - {task.get('end_date', 'Н/Д')}")

        # Генерируем отчет о распределении задач
        report = employee_manager.generate_workload_report(project_id)

        # Проверяем длину отчета
        if len(report) <= 4000:  # Оставляем запас до лимита в 4096 символов
            # Если отчет короткий, отправляем его напрямую
            markup = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад к проекту", callback_data=f"view_project_{project_id}")]
            ])
            await callback.message.edit_text(report, reply_markup=markup)
        else:
            # Если отчет слишком длинный, сохраняем его в файл и отправляем как документ
            temp_dir = tempfile.mkdtemp()

            # Создаем безопасное имя файла
            safe_project_name = "".join(
                c if c.isalnum() or c in [' ', '.', '_', '-'] else '_' for c in project['name']
            )
            file_path = os.path.join(temp_dir, f"{safe_project_name}_workload_report.txt")

            # Записываем отчет в файл
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(report)

            # Отправляем краткое сообщение
            await callback.message.edit_text(
                f"Отчет о распределении задач для проекта '{project['name']}' прикреплен ниже. "
                f"В проекте задействовано {len(employee_manager.get_employee_workload(project_id))} сотрудников."
            )

            # Отправляем файл с отчетом
            file = FSInputFile(file_path)
            await callback.message.answer_document(
                file,
                caption=f"Отчет о распределении задач для проекта '{project['name']}'"
            )

            # Отправляем кнопки для дальнейших действий
            markup = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад к проекту", callback_data=f"view_project_{project_id}")]
            ])
            await callback.message.answer("Выберите дальнейшее действие:", reply_markup=markup)

            # Очистка временных файлов
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                os.rmdir(temp_dir)
            except Exception as e:
                print(f"Ошибка при очистке временных файлов: {str(e)}")

        # Генерируем и отправляем диаграмму загрузки сотрудников
        workload_data = employee_manager.get_employee_workload(project_id)

        print(f"Создание диаграммы загрузки сотрудников для {len(workload_data)} сотрудников")

        # Выводим детализацию для отладки
        for emp_id, data in workload_data.items():
            print(f"Сотрудник {emp_id}: {data.get('name', 'Без имени')} - {len(data.get('tasks', []))} задач")
            for task in data.get('tasks', []):
                print(
                    f"  Задача: {task.get('id', 'ID?')}: {task.get('name', 'Без имени')} - {task.get('start_date', 'Н/Д')} - {task.get('end_date', 'Н/Д')}")

        if workload_chart and workload_data:
            try:
                workload_image = workload_chart.generate(project, workload_data)
                if os.path.exists(workload_image):
                    workload_file = FSInputFile(workload_image)
                    await callback.message.answer_photo(
                        workload_file,
                        caption=f"Диаграмма загрузки сотрудников для проекта '{project['name']}'"
                    )
                    print(f"Диаграмма загрузки успешно создана и отправлена")
                else:
                    print(f"Ошибка: файл диаграммы загрузки не найден по пути: {workload_image}")
            except Exception as e:
                print(f"Ошибка при создании диаграммы загрузки: {str(e)}")
                import traceback
                print(traceback.format_exc())

    except Exception as e:
        import traceback
        error_msg = f"Ошибка при получении распределения задач: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)

        # Отправляем укороченное сообщение об ошибке
        short_error = f"Ошибка при получении распределения задач: {str(e)}"
        await callback.message.edit_text(
            short_error,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад к проекту", callback_data=f"view_project_{project_id}")]
            ])
        )

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

@router.message(Command("test_mode"))
async def cmd_test_mode(message: Message):
    """
    Включает тестовый режим без учета выходных дней для проверки календарного плана.

    Это временное отключение выходных дней у всех сотрудников для сравнения
    расчетов бота с ручными расчетами без учета выходных.
    """
    # Проверяем права администратора
    if not is_admin(message.from_user.id, db_manager):
        await message.answer("У вас нет прав администратора для включения тестового режима.")
        return

    from utils.test_helpers import disable_days_off_for_testing, update_employees_in_db

    try:
        # Сохраняем оригинальные данные в памяти для возможности восстановления
        original_employees = disable_days_off_for_testing()

        # Сохраняем информацию о тестовом режиме в глобальном контексте
        global test_mode_active, original_employees_data
        test_mode_active = True
        original_employees_data = original_employees

        # Обновляем базу данных
        update_employees_in_db(db_manager)

        # Отправляем информацию пользователю
        await message.answer(
            "✅ Тестовый режим активирован! Выходные дни сотрудников временно отключены.\n\n"
            "В этом режиме календарный план будет рассчитан без учета выходных. "
            "Это позволит сравнить результаты работы бота с ручными расчетами.\n\n"
            "Для возврата в обычный режим используйте команду /normal_mode."
        )

        # Предлагаем пользователю создать новый проект или пересчитать существующий
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Создать новый проект", callback_data="create_test_project")],
            [InlineKeyboardButton(text="Список проектов", callback_data="back_to_projects")]
        ])

        await message.answer(
            "Что вы хотите сделать в тестовом режиме?",
            reply_markup=markup
        )

    except Exception as e:
        await message.answer(f"Ошибка при включении тестового режима: {str(e)}")


@router.message(Command("normal_mode"))
async def cmd_normal_mode(message: Message):
    """
    Возвращает нормальный режим работы бота с учетом выходных дней.
    """
    # Проверяем права администратора
    if not is_admin(message.from_user.id, db_manager):
        await message.answer("У вас нет прав администратора для изменения режима работы.")
        return

    # Проверяем, активен ли тестовый режим
    global test_mode_active, original_employees_data
    if not test_mode_active:
        await message.answer("Бот уже работает в обычном режиме с учетом выходных дней.")
        return

    from utils.test_helpers import restore_days_off, update_employees_in_db

    try:
        # Восстанавливаем оригинальные данные о выходных днях
        restore_days_off(original_employees_data)

        # Сбрасываем флаг тестового режима
        test_mode_active = False
        original_employees_data = None

        # Обновляем базу данных
        update_employees_in_db(db_manager)

        # Отправляем информацию пользователю
        await message.answer(
            "✅ Нормальный режим восстановлен!\n\n"
            "Теперь календарный план будет рассчитываться с учетом выходных дней сотрудников."
        )

    except Exception as e:
        await message.answer(f"Ошибка при восстановлении обычного режима: {str(e)}")


@router.callback_query(F.data == "create_test_project")
async def create_test_project(callback: CallbackQuery, state: FSMContext):
    """
    Начинает создание тестового проекта в тестовом режиме.
    """
    await callback.message.edit_text("Введите название тестового проекта:")
    await state.set_state(ProjectState.waiting_for_name)


async def main():
    # Убедимся, что таблицы созданы
    db_manager.init_db()

    # Запускаем бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())