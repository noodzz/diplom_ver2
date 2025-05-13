import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import numpy as np
import os
import tempfile


class GanttChart:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()

    def generate(self, project, tasks, task_dates, critical_path=None):
        """
        Генерирует диаграмму Ганта для проекта

        Args:
            project (dict): Информация о проекте
            tasks (list): Список задач проекта
            task_dates (dict): Словарь с датами начала и окончания задач
            critical_path (list, optional): Список ID задач, входящих в критический путь

        Returns:
            str: Путь к созданному файлу диаграммы
        """
        # Фильтруем только основные задачи (без подзадач)
        main_tasks = [task for task in tasks if not task.get('parent_id')]

        # Сортируем задачи по дате начала
        task_list = []
        for task in main_tasks:
            if task['id'] in task_dates:
                start_date = datetime.strptime(task_dates[task['id']]['start'], '%Y-%m-%d')
                task_list.append((task, start_date))
            else:
                # Если нет дат для задачи, используем дату проекта
                start_date = datetime.strptime(project['start_date'], '%Y-%m-%d')
                task_list.append((task, start_date))

        # Сортировка по дате начала
        task_list.sort(key=lambda x: x[1])

        # Преобразуем даты в объекты datetime
        start_dates = []
        end_dates = []
        sorted_tasks = []

        for task, start_date in task_list:
            sorted_tasks.append(task)

            if task['id'] in task_dates:
                start_dates.append(datetime.strptime(task_dates[task['id']]['start'], '%Y-%m-%d'))
                end_dates.append(datetime.strptime(task_dates[task['id']]['end'], '%Y-%m-%d'))
            else:
                # Если нет дат для задачи, используем даты проекта
                start_date = datetime.strptime(project['start_date'], '%Y-%m-%d')
                start_dates.append(start_date)
                end_dates.append(start_date + timedelta(days=task['duration']))

        # Определяем общие даты проекта
        project_start = min(start_dates) if start_dates else datetime.strptime(project['start_date'], '%Y-%m-%d')
        project_end = max(end_dates) if end_dates else project_start + timedelta(days=30)

        # Создаем фигуру с нужными размерами
        fig_height = max(8, len(sorted_tasks) * 0.4 + 2)  # Динамическая высота в зависимости от числа задач
        fig, ax = plt.subplots(figsize=(12, fig_height))

        # Названия задач
        labels = [f"{task['name']} ({task['duration']} дн.)" for task in sorted_tasks]

        # Позиции заданий на оси y
        y_positions = np.arange(len(labels))

        # Продолжительность выполнения задач
        durations = [(end - start).days for start, end in zip(start_dates, end_dates)]

        # Цвета для критического пути и обычных задач
        colors = []
        if critical_path:
            for task in sorted_tasks:
                if task['id'] in critical_path:
                    colors.append('r')  # Красный для задач критического пути
                else:
                    colors.append('b')  # Синий для обычных задач
        else:
            colors = ['b'] * len(sorted_tasks)  # Все задачи синие, если нет критического пути

        # Рисуем горизонтальные полосы для задач
        for i, (start, duration, color) in enumerate(zip(start_dates, durations, colors)):
            # Вычисляем ширину полосы (минимум 1 день)
            width = max(1, duration)
            ax.barh(y_positions[i], width, left=start, height=0.5, align='center',
                    color=color, alpha=0.8, edgecolor='black')

        # Настраиваем оси
        ax.set_yticks(y_positions)
        ax.set_yticklabels(labels)
        ax.set_xlabel('Дата')
        ax.set_ylabel('Задача')

        # Устанавливаем диапазон дат с небольшим запасом
        date_padding = timedelta(days=max(3, int((project_end - project_start).days * 0.05)))
        ax.set_xlim(project_start - date_padding, project_end + date_padding)

        # Форматируем заголовок с добавлением длительности проекта
        project_duration = (project_end - project_start).days
        ax.set_title(f'Диаграмма Ганта для проекта "{project["name"]}"\nДлительность: {project_duration} дней')

        # Добавляем сетку для лучшей читаемости
        ax.grid(True, axis='x', linestyle='--', alpha=0.7)

        # Форматируем даты на оси x
        date_format = mdates.DateFormatter('%d.%m.%Y')
        ax.xaxis.set_major_formatter(date_format)

        # Устанавливаем интервал для делений оси X в зависимости от длительности проекта
        if project_duration <= 14:
            # Для коротких проектов - ежедневные метки
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        elif project_duration <= 60:
            # Для средних проектов - еженедельные метки
            ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))  # Понедельники
        else:
            # Для длинных проектов - ежемесячные метки
            ax.xaxis.set_major_locator(mdates.MonthLocator())

        # Поворачиваем метки для лучшей читаемости
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

        # Добавляем даты начала и окончания проекта
        ax.axvline(x=project_start, color='g', linestyle='--', alpha=0.7)
        ax.axvline(x=project_end, color='g', linestyle='--', alpha=0.7)

        # Подписываем даты начала и окончания проекта
        ax.text(project_start, -1, f"Начало: {project_start.strftime('%d.%m.%Y')}",
                ha='center', va='top', color='g', fontweight='bold')
        ax.text(project_end, -1, f"Окончание: {project_end.strftime('%d.%m.%Y')}",
                ha='center', va='top', color='g', fontweight='bold')

        # Добавляем информацию о датах для каждой задачи
        for i, (start, end, task) in enumerate(zip(start_dates, end_dates, sorted_tasks)):
            # Начальная дата
            ax.text(start - timedelta(days=0.2), y_positions[i],
                    start.strftime('%d.%m'),
                    va='center', ha='right', fontsize=8)

            # Конечная дата
            ax.text(end + timedelta(days=0.2), y_positions[i],
                    end.strftime('%d.%m'),
                    va='center', ha='left', fontsize=8)

            # Добавляем зависимости
            if hasattr(task, 'predecessors') and task['predecessors']:
                for pred_id in task['predecessors']:
                    # Находим индекс предшествующей задачи
                    for j, t in enumerate(sorted_tasks):
                        if t['id'] == pred_id:
                            # Рисуем стрелку от конца предшествующей задачи к началу текущей
                            arrow = ax.annotate('',
                                                xytext=(end_dates[j], y_positions[j]),
                                                xy=(start, y_positions[i]),
                                                arrowprops=dict(arrowstyle='->',
                                                                color='gray',
                                                                alpha=0.5,
                                                                connectionstyle='arc3,rad=0.2'))
                            break

        # Добавляем легенду
        if critical_path:
            from matplotlib.patches import Patch
            legend_elements = [
                Patch(facecolor='r', edgecolor='black', alpha=0.8, label='Критический путь'),
                Patch(facecolor='b', edgecolor='black', alpha=0.8, label='Обычные задачи')
            ]
            ax.legend(handles=legend_elements, loc='upper right')

        # Плотная компоновка
        fig.tight_layout()

        # Создаем безопасное имя файла, удаляя недопустимые символы
        safe_project_name = self._create_safe_filename(project['name'])

        # Сохраняем диаграмму с высоким разрешением
        chart_file = os.path.join(self.temp_dir, f"{safe_project_name}_gantt.png")
        plt.savefig(chart_file, dpi=200, bbox_inches='tight')
        plt.close(fig)

        return chart_file

    def _create_safe_filename(self, filename):
        """
        Создает безопасное имя файла, удаляя или заменяя недопустимые символы

        Args:
            filename (str): Исходное имя файла

        Returns:
            str: Безопасное имя файла
        """
        # Список недопустимых символов в Windows
        invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']

        # Заменяем недопустимые символы на безопасные
        safe_name = filename
        for char in invalid_chars:
            safe_name = safe_name.replace(char, '_')

        # Ограничиваем длину имени файла
        if len(safe_name) > 100:
            safe_name = safe_name[:100]

        return safe_name