import os
import logging
import asyncio
import json
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
        # Рассчитываем и сохраняем даты подзадач
        subtask_dates = calculate_subtask_dates(task_manager, result['task_dates'])
        if subtask_dates:
            task_manager.update_task_dates(subtask_dates)
            print(f"Обновлены даты для {len(subtask_dates)} подзадач")
        # ШАГ 5: Генерируем отчет о результатах
        print("Генерируем отчет...")
        text = f"📊 ОТЧЕТ ПО КАЛЕНДАРНОМУ ПЛАНУ\n"
        text += f"=============================================\n\n"
        text += f"📋 ОБЩАЯ ИНФОРМАЦИЯ О ПРОЕКТЕ\n"
        text += f"Название проекта: '{project['name']}'\n"

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
                text += f"Общее количество задач: {len(tasks)}\n\n"
            else:
                text += f"Длительность проекта: {result['duration']} дней\n\n"
        else:
            text += f"Длительность проекта: {result['duration']} дней\n\n"

        # Критический путь
        text += f"🚩 КРИТИЧЕСКИЙ ПУТЬ\n"
        text += f"Критический путь — последовательность задач, определяющая длительность проекта.\n"
        text += f"Задержка любой из этих задач приведет к задержке всего проекта.\n\n"
        if result['critical_path']:
            critical_tasks = []
            total_critical_days = 0

            for task_id in result['critical_path']:
                task = task_manager.get_task(task_id)
                critical_tasks.append(task)
                total_critical_days += task['duration'] - 1

                # Форматируем даты для отображения
                start_date = "?"
                end_date = "?"
                if task_id in result['task_dates']:
                    start_date = format_date(result['task_dates'][task_id]['start'])
                    end_date = format_date(result['task_dates'][task_id]['end'])

                # Добавляем информацию о задаче
                text += f"• {task['name']} ({task['duration']} дн.)\n"
                text += f"  Даты: {start_date} - {end_date}\n"

                # Добавляем информацию о сотруднике, если назначен
                if task.get('employee_id'):
                    try:
                        employee = employee_manager.get_employee(task['employee_id'])
                        text += f"  Исполнитель: {employee['name']} ({employee['position']})\n"
                    except:
                        pass
                text += "\n"
            text += f"Длина критического пути: {total_critical_days} дней\n\n"
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

        # Добавляем информацию о распределении задач по сотрудникам
        text += f"👥 РАСПРЕДЕЛЕНИЕ ЗАДАЧ\n"

        if assignments:
            # Группируем по сотрудникам
            employees_tasks = {}
            for task_id, employee_id in assignments.items():
                if employee_id not in employees_tasks:
                    employees_tasks[employee_id] = []

                task = task_manager.get_task(task_id)
                if task:
                    employees_tasks[employee_id].append(task)
            # Выводим задачи каждого сотрудника
            for employee_id, tasks_list in employees_tasks.items():
                try:
                    employee = employee_manager.get_employee(employee_id)
                    text += f"{employee['name']} ({employee['position']}):\n"

                    # Сортируем задачи по датам
                    sorted_tasks = sorted(tasks_list,
                                          key=lambda t: result['task_dates'].get(t['id'], {}).get('start',
                                                                                                  '9999-12-31')
                                          if t['id'] in result['task_dates'] else '9999-12-31')

                    for task in sorted_tasks:
                        # Определяем даты
                        start_date = "?"
                        end_date = "?"
                        # Проверяем сначала в result['task_dates']
                        if task['id'] in result['task_dates']:
                            start_date = format_date(result['task_dates'][task['id']]['start'])
                            end_date = format_date(result['task_dates'][task['id']]['end'])
                        else:
                            # Если нет в результате, пробуем получить из базы данных
                            task_data = task_manager.get_task(task['id'])
                            if task_data and task_data.get('start_date') and task_data.get('end_date'):
                                start_date = format_date(task_data['start_date'])
                                end_date = format_date(task_data['end_date'])

                        # Выводим информацию о задаче
                        if task.get('parent_id'):
                            # Для подзадач показываем родительскую задачу
                            parent_task = task_manager.get_task(task['parent_id'])
                            parent_name = parent_task['name'] if parent_task else "Неизвестная задача"
                            text += f"  • {parent_name} → {task['name']} ({task['duration']} дн.)\n"
                            text += f"    Даты: {start_date} - {end_date}\n"
                        else:
                            text += f"  • {task['name']} ({task['duration']} дн.)\n"
                            text += f"    Даты: {start_date} - {end_date}\n"

                    # Суммарная нагрузка сотрудника
                    total_load = sum(task['duration'] for task in tasks_list)
                    text += f"  Общая нагрузка: {total_load} дней\n\n"

                except Exception as e:
                    # Если не удалось получить информацию о сотруднике, пропускаем
                    continue
        else:
            text += "\nНе удалось автоматически распределить задачи."

        # Добавляем рекомендации или замечания
        text += f"📝 РЕКОМЕНДАЦИИ\n"
        text += f"1. Обратите особое внимание на задачи критического пути\n"
        text += f"2. При необходимости перераспределите нагрузку между сотрудниками\n"
        text += f"3. Для сокращения сроков выполнения проекта оптимизируйте критические задачи\n\n"

        # Добавляем подпись
        text += f"=============================================\n"
        text += f"Отчет сгенерирован {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        text += f"Система автоматизированного календарного планирования"

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
            if os.path.exists(gantt_image):
                os.remove(gantt_image)
        except Exception as e:
            print(f"Ошибка при очистке временных файлов: {str(e)}")

    except Exception as e:
        await callback.message.edit_text(f"Ошибка при расчете календарного плана: {str(e)}")


def calculate_subtask_dates(task_manager, task_dates):
    """Рассчитывает даты для подзадач на основе дат родительских задач"""
    # Получаем все задачи с датами
    tasks_with_dates = []
    for task_id, dates in task_dates.items():
        task = task_manager.get_task(task_id)
        if task and 'start' in dates and 'end' in dates:
            task['start_date'] = dates['start']
            task['end_date'] = dates['end']
            tasks_with_dates.append(task)

    # Рассчитываем даты для подзадач
    subtask_dates = {}
    for task in tasks_with_dates:
        if task.get('is_group'):
            subtasks = task_manager.get_subtasks(task['id'])

            if not subtasks:
                continue

            # Если у родительской задачи есть даты
            parent_start = datetime.datetime.strptime(task['start_date'], '%Y-%m-%d')
            parent_end = datetime.datetime.strptime(task['end_date'], '%Y-%m-%d')

            # Группируем подзадачи
            parallel_subtasks = [st for st in subtasks if st.get('parallel')]
            sequential_subtasks = [st for st in subtasks if not st.get('parallel')]

            # Обрабатываем параллельные подзадачи
            for subtask in parallel_subtasks:
                # Параллельные подзадачи начинаются с родительской
                subtask_start = parent_start
                subtask_end = subtask_start + datetime.timedelta(days=subtask['duration'] - 1)

                # Проверяем, не выходит ли за пределы родительской
                if subtask_end > parent_end:
                    subtask_end = parent_end

                subtask_dates[subtask['id']] = {
                    'start': subtask_start.strftime('%Y-%m-%d'),
                    'end': subtask_end.strftime('%Y-%m-%d')
                }

            # Обрабатываем последовательные подзадачи
            current_date = parent_start
            for subtask in sequential_subtasks:
                subtask_start = current_date
                subtask_end = subtask_start + datetime.timedelta(days=subtask['duration'] - 1)

                # Проверяем, не выходит ли за пределы родительской
                if subtask_end > parent_end:
                    subtask_end = parent_end

                subtask_dates[subtask['id']] = {
                    'start': subtask_start.strftime('%Y-%m-%d'),
                    'end': subtask_end.strftime('%Y-%m-%d')
                }

                # Следующая подзадача начинается после текущей
                current_date = subtask_end + datetime.timedelta(days=1)

    return subtask_dates

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
                f"Задача '{task['name']}' (ID: {task['id']}): календарная длительность скорректирована с {original_duration} до {calendar_days} дней")
            task['adjusted_duration'] = calendar_days
            # Сохраняем оригинальную рабочую длительность
            working_duration = original_duration
            # Обновляем в базе данных
            task_manager.db.execute(
                "UPDATE tasks SET duration = ?, working_duration = ? WHERE id = ?",
                (calendar_days, working_duration, task['id'])
            )
            if task.get('parent_id'):
                # Получаем все подзадачи родительской задачи
                parent_subtasks = task_manager.get_subtasks(task['parent_id'])
                parent_task = task_manager.get_task(task['parent_id'])

                # Группируем по признаку параллельности
                parallel_subtasks = [t for t in parent_subtasks if t.get('parallel')]
                sequential_subtasks = [t for t in parent_subtasks if not t.get('parallel')]

                # Находим новую длительность родительской задачи
                parallel_duration = max([t['duration'] for t in parallel_subtasks]) if parallel_subtasks else 0
                sequential_duration = sum(t['duration'] for t in sequential_subtasks)

                new_parent_duration = max(parallel_duration, sequential_duration)
                if sequential_subtasks and parallel_subtasks:
                    # Если есть и параллельные и последовательные задачи, берем максимум
                    new_parent_duration = max(parallel_duration, sequential_duration)
                elif sequential_subtasks:
                    # Если только последовательные, суммируем
                    new_parent_duration = sequential_duration
                elif parallel_subtasks:
                    # Если только параллельные, берем максимум
                    new_parent_duration = parallel_duration

                # Обновляем длительность родительской задачи если нужно
                if new_parent_duration > parent_task['duration']:
                    print(f"Родительская задача '{parent_task['name']}' (ID: {parent_task['id']}): "
                          f"длительность скорректирована с {parent_task['duration']} до {new_parent_duration} дней")

                    task_manager.db.execute(
                        "UPDATE tasks SET duration = ? WHERE id = ?",
                        (new_parent_duration, task['parent_id'])
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

    try:
        # Получаем данные о проекте
        project = project_manager.get_project_details(project_id)

        # Получаем распределение задач по сотрудникам
        workload_data = employee_manager.get_employee_workload(project_id)

        # Генерируем отчет о распределении задач
        report = employee_manager.generate_workload_report(project_id)

        # Создаем кнопку для возврата к просмотру проекта
        buttons = [
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