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

        # Создаем фигуру
        fig_height = max(6, len(df) * 0.4 + 1)
        fig, ax = plt.subplots(figsize=(12, fig_height))

        # Определяем общие даты проекта
        project_start = df['Start'].min()
        project_end = df['End'].max()

        # Вычисляем длительность проекта в днях
        project_duration = (project_end - project_start).days + 1

        # Создаем список дат от начала до конца проекта для точного позиционирования
        date_range = [project_start + timedelta(days=x) for x in range((project_end - project_start).days + 1)]

        # Создаем Y-координаты для задач
        y_positions = np.arange(len(df))

        # Рисуем задачи на диаграмме
        for i, (_, row) in enumerate(df.iterrows()):
            # Получаем индекс начала и конца в date_range
            start_idx = (row['Start'] - project_start).days
            end_idx = (row['End'] - project_start).days

            # Вычисляем длительность в днях
            duration_days = end_idx - start_idx + 1

            # Задаем цвет в зависимости от того, входит ли задача в критический путь
            color = 'r' if row['Critical'] else 'b'

            # Рисуем прямоугольник
            ax.barh(y_positions[i], duration_days, left=start_idx, height=0.7,
                    align='center', color=color, alpha=0.9, edgecolor='black')

            # Выводим отладочную информацию
            print(f"Прямоугольник для задачи '{row['Task']}':")
            print(f"  Позиция Y: {y_positions[i]}")
            print(f"  Начало: индекс {start_idx} ({row['Start'].strftime('%Y-%m-%d')})")
            print(f"  Конец: индекс {end_idx} ({row['End'].strftime('%Y-%m-%d')})")
            print(f"  Длительность: {duration_days} дней")

        # Настраиваем оси
        ax.set_yticks(y_positions)
        ax.set_yticklabels(df['Task'].tolist())

        # Создаем сетку с четкими делениями для каждого дня
        ax.set_xticks(range(len(date_range)))
        ax.set_xticklabels([d.strftime('%d.%m') for d in date_range], rotation=45, fontsize=8)

        # Добавляем основную сетку
        ax.grid(True, axis='x', which='major', linestyle='-', alpha=0.5)

        # Настраиваем заголовок
        ax.set_title(f'Диаграмма Ганта для проекта "{project["name"]}"\nДлительность: {project_duration} дней')

        # Настраиваем оси
        ax.set_xlabel('Дата')
        ax.set_ylabel('Задача')

        # Устанавливаем диапазон по X
        ax.set_xlim(-0.5, len(date_range) - 0.5)

        # Добавляем примечание о формате дат
        fig.text(0.5, 0.01,
                 "Примечание: Конечные даты указаны включительно. Например, задача '19.05 - 21.05' выполняется с начала 19.05 до конца 21.05.",
                 ha='center', fontsize=9)

        # Плотная компоновка
        fig.tight_layout(rect=[0, 0.03, 1, 0.97])

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