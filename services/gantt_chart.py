import datetime

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
import tempfile


class GanttChart:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()

    def generate(self, project, tasks, task_dates, critical_path=None):
        """
        Создает диаграмму Ганта с четким дневным позиционированием.

        Args:
            project (dict): Информация о проекте
            tasks (list): Список задач проекта
            task_dates (dict): Словарь с датами начала и окончания задач
            critical_path (list, optional): Список ID задач, входящих в критический путь

        Returns:
            str: Путь к созданному файлу диаграммы
        """
        import os
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from datetime import datetime, timedelta, date
        import numpy as np
        import pandas as pd

        print(f"Построение диаграммы Ганта с новым алгоритмом для проекта '{project['name']}'")

        # Преобразуем задачи в формат pandas DataFrame для более точного управления датами
        data = []

        # Сначала собираем все основные задачи без подзадач
        main_tasks = [task for task in tasks if not task.get('parent_id')]

        # Проходим по задачам и извлекаем даты из task_dates
        for task in main_tasks:
            task_id = task['id']
            task_id_str = str(task_id)
            task_name = task.get('name', f"Задача {task_id}")
            duration = task.get('duration', 1)

            # Проверяем, является ли задача частью критического пути
            is_critical = False
            if critical_path:
                is_critical = task_id in critical_path or task_id_str in critical_path

            # Получаем даты из разных источников
            start_date = None
            end_date = None

            # Ищем даты в task_dates
            if task_id in task_dates and 'start' in task_dates[task_id] and 'end' in task_dates[task_id]:
                start_date = task_dates[task_id]['start']
                end_date = task_dates[task_id]['end']
            elif task_id_str in task_dates and 'start' in task_dates[task_id_str] and 'end' in task_dates[task_id_str]:
                start_date = task_dates[task_id_str]['start']
                end_date = task_dates[task_id_str]['end']
            # Ищем даты в самой задаче
            elif task.get('start_date') and task.get('end_date'):
                start_date = task['start_date']
                end_date = task['end_date']
            # Если нет дат, используем даты проекта
            else:
                start_date = project['start_date']
                end_date = (datetime.strptime(start_date, '%Y-%m-%d') +
                            timedelta(days=duration - 1)).strftime('%Y-%m-%d')

            # Преобразуем строковые даты в объекты datetime
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')

            # Вычисляем фактическую длительность в днях
            actual_duration = (end_dt - start_dt).days + 1

            # Создаем запись для DataFrame
            data.append({
                'Task': f"{task_name} ({duration} дн.)",
                'Start': start_dt,
                'End': end_dt,
                'Duration': actual_duration,
                'Critical': is_critical,
                'ID': task_id
            })

            # Выводим отладочную информацию
            print(f"Задача: {task_name}")
            print(f"  ID: {task_id}")
            print(f"  Даты: {start_date} - {end_date}")
            print(f"  Длительность: {duration} дней, Фактическая: {actual_duration} дней")
            print(f"  Критический путь: {'Да' if is_critical else 'Нет'}")

        # Создаем DataFrame
        if not data:
            print("Нет данных для построения диаграммы")
            # Создаем пустую диаграмму с сообщением
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.text(0.5, 0.5, "Нет данных для построения диаграммы",
                    ha='center', va='center', fontsize=14)
            chart_file = os.path.join(self.temp_dir, "empty_gantt.png")
            plt.savefig(chart_file, dpi=150)
            plt.close(fig)
            return chart_file

        df = pd.DataFrame(data)

        # Сортируем задачи по дате начала
        df = df.sort_values('Start')

        # Определяем максимальную длину имени задачи для расчета размера фигуры
        max_task_name_length = max(len(task_name) for task_name in df['Task'])

        # Создаем фигуру с учетом количества задач и длины их названий
        fig_height = max(6, len(df) * 0.4 + 1)
        # Увеличиваем ширину фигуры для размещения всех дат
        fig_width = 14 + (max_task_name_length / 20)  # Динамически регулируем ширину
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

        # Определяем общие даты проекта
        project_start = df['Start'].min()
        project_end = df['End'].max()

        # Вычисляем длительность проекта в днях
        project_duration = (project_end - project_start).days + 1

        # Добавляем буфер с обеих сторон для лучшего отображения
        buffer_days = max(2, int(project_duration * 0.05))  # 5% от длительности проекта, но не менее 2 дней
        extended_start = project_start - timedelta(days=buffer_days)
        extended_end = project_end + timedelta(days=buffer_days)

        # Создаем расширенный список дат от начала до конца проекта для точного позиционирования
        extended_date_range = [extended_start + timedelta(days=x) for x in
                               range((extended_end - extended_start).days + 1)]

        # Создаем основной список дат для отображения задач
        date_range = [project_start + timedelta(days=x) for x in range((project_end - project_start).days + 1)]

        # Создаем Y-координаты для задач
        y_positions = np.arange(len(df))

        # Рисуем задачи на диаграмме - с корректировкой для визуального разделения
        for i, (_, row) in enumerate(df.iterrows()):
            # Получаем индекс начала и конца в date_range
            start_idx = (row['Start'] - project_start).days
            end_idx = (row['End'] - project_start).days

            # Вычисляем длительность в днях
            duration_days = end_idx - start_idx + 1

            # Для визуального разделения, сделаем блоки чуть короче их полной длительности
            visual_adjustment = 0.05  # 5% корректировка для визуального разделения

            # Задаем цвет в зависимости от того, входит ли задача в критический путь
            color = 'r' if row['Critical'] else 'b'

            # Рисуем прямоугольник с небольшими визуальными отступами для устранения пересечений
            ax.barh(y_positions[i], duration_days - visual_adjustment,
                    left=start_idx, height=0.7,
                    align='center', color=color, alpha=0.9, edgecolor='black')

        # Настраиваем оси
        ax.set_yticks(y_positions)
        ax.set_yticklabels(df['Task'].tolist())

        # Создаем более компактные подписи дат
        date_labels = [d.strftime('%d.%m') for d in date_range]

        # Устанавливаем метки для каждого дня, но отображаем только определенные интервалы
        ax.set_xticks(range(len(date_range)))

        # Определяем интервал для меток даты на основе количества дней
        if project_duration > 60:
            date_interval = 5  # Показываем каждый 5-й день для длинных проектов
        elif project_duration > 30:
            date_interval = 3  # Показываем каждый 3-й день для средних проектов
        else:
            date_interval = 1  # Показываем каждый день для коротких проектов

        # Устанавливаем метки с правильным интервалом
        visible_ticks = range(0, len(date_range), date_interval)
        visible_labels = [date_labels[i] for i in visible_ticks]

        ax.set_xticks(visible_ticks)
        ax.set_xticklabels(visible_labels, rotation=45, fontsize=8, ha='right')

        # Добавляем основную сетку
        ax.grid(True, axis='x', which='major', linestyle='-', alpha=0.5)

        # Настраиваем заголовок
        ax.set_title(f'Диаграмма Ганта для проекта "{project["name"]}"\nДлительность: {project_duration} дней')

        # Настраиваем оси
        ax.set_xlabel('Дата')
        ax.set_ylabel('Задача')

        # Устанавливаем диапазон по X с дополнительным пространством справа для последних дат
        ax.set_xlim(-1, len(date_range) + 1)  # Добавляем запас с обеих сторон

        # Добавляем примечание о формате дат
        fig.text(0.5, 0.01,
                 "Примечание: Конечные даты указаны включительно. Например, задача '19.05 - 21.05' выполняется с начала 19.05 до конца 21.05.",
                 ha='center', fontsize=9)

        # Плотная компоновка c уменьшенными отступами справа
        plt.tight_layout(rect=[0, 0.03, 0.98, 0.97], pad=2.0)

        # Увеличиваем отступ справа для помещения всех дат
        plt.subplots_adjust(right=0.95)

        # Создаем безопасное имя файла
        safe_project_name = self._create_safe_filename(project['name'])

        # Сохраняем диаграмму
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