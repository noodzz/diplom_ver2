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
        print(f"Создание диаграммы Ганта для проекта '{project['name']}'")

        # Фильтруем только основные задачи (без подзадач)
        main_tasks = [task for task in tasks if not task.get('parent_id')]

        # Для отладки выводим данные из базы
        print(f"Всего задач: {len(tasks)}, основных задач: {len(main_tasks)}")
        print(f"Данные дат из task_dates: {len(task_dates)} записей")

        # Проверка соответствия ID задач и их дат
        for task in main_tasks:
            task_id = task['id']
            task_id_str = str(task_id)

            if task_id in task_dates:
                print(f"Задача {task_id}: {task['name']} - даты из task_dates: {task_dates[task_id]}")
            elif task_id_str in task_dates:
                print(
                    f"Задача {task_id}: {task['name']} - даты из task_dates по строковому ключу: {task_dates[task_id_str]}")
            else:
                # Проверяем, есть ли даты в самой задаче
                if 'start_date' in task and 'end_date' in task:
                    print(
                        f"Задача {task_id}: {task['name']} - даты из задачи: {task['start_date']} - {task['end_date']}")
                else:
                    print(f"ПРЕДУПРЕЖДЕНИЕ: Даты для задачи {task_id}: {task['name']} не найдены!")

        # Преобразуем даты в объекты datetime и создаем список задач
        task_list = []
        for task in main_tasks:
            task_id = task['id']
            task_id_str = str(task_id)

            # Ищем даты в разных источниках (приоритет: task_dates -> задача -> проект)
            if task_id in task_dates and 'start' in task_dates[task_id] and 'end' in task_dates[task_id]:
                # Даты из словаря дат по числовому ID
                start_date = datetime.strptime(task_dates[task_id]['start'], '%Y-%m-%d')
                end_date = datetime.strptime(task_dates[task_id]['end'], '%Y-%m-%d')
            elif task_id_str in task_dates and 'start' in task_dates[task_id_str] and 'end' in task_dates[task_id_str]:
                # Даты из словаря дат по строковому ID
                start_date = datetime.strptime(task_dates[task_id_str]['start'], '%Y-%m-%d')
                end_date = datetime.strptime(task_dates[task_id_str]['end'], '%Y-%m-%d')
            elif 'start_date' in task and 'end_date' in task and task['start_date'] and task['end_date']:
                # Даты из самой задачи
                start_date = datetime.strptime(task['start_date'], '%Y-%m-%d')
                end_date = datetime.strptime(task['end_date'], '%Y-%m-%d')
            else:
                # Если нет дат, используем даты проекта
                print(f"Не найдены даты для задачи {task_id}: {task['name']}, используем дату проекта")
                start_date = datetime.strptime(project['start_date'], '%Y-%m-%d')
                end_date = start_date + timedelta(days=task.get('duration', 1) - 1)  # -1 т.к. включительно

            # Рассчитываем фактическую длительность задачи с учетом выходных
            actual_duration = (end_date - start_date).days + 1  # +1 т.к. включительно

            # Создаем копию задачи с добавлением фактической длительности
            task_copy = task.copy()
            task_copy['actual_duration'] = actual_duration

            task_list.append((task_copy, start_date, end_date))
            print(
                f"Добавлена задача {task_id}: {task['name']} с датами {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}, длительность: {actual_duration} дней")

        # Сортировка по дате начала
        task_list.sort(key=lambda x: x[1])

        # Создаем списки для построения диаграммы
        sorted_tasks = []
        start_dates = []
        end_dates = []

        for task, start, end in task_list:
            sorted_tasks.append(task)
            start_dates.append(start)
            # КРИТИЧЕСКИ ВАЖНО: для правильного отображения прямоугольников
            # конечная дата должна быть на следующий день после фактического окончания
            end_dates.append(end + timedelta(days=1))  # Добавляем день для невключительной даты

        # Определяем общие даты проекта
        if not start_dates:
            # Если нет задач с датами, используем даты проекта
            project_start = datetime.strptime(project['start_date'], '%Y-%m-%d')
            project_end = project_start + timedelta(days=7)  # Предполагаем по умолчанию 7 дней
        else:
            # Используем фактические даты задач
            project_start = min(start_dates)
            project_end = max([end - timedelta(days=1) for end in end_dates])  # Убираем добавленный день

        # Конец проекта для отображения (на день больше)
        project_end_display = project_end + timedelta(days=1)

        print(f"Период проекта: {project_start.strftime('%Y-%m-%d')} - {project_end.strftime('%Y-%m-%d')}")

        # Создаем фигуру с нужными размерами
        fig_height = max(8, len(sorted_tasks) * 0.4 + 2)
        fig, ax = plt.subplots(figsize=(12, fig_height))

        # Названия задач с фактической длительностью
        labels = [f"{task['name']} ({task.get('actual_duration', task.get('duration', 0))} дн.)" for task in
                  sorted_tasks]
        y_positions = np.arange(len(labels))

        # Цвета для критического пути и обычных задач
        colors = []
        if critical_path:
            for task in sorted_tasks:
                if task['id'] in critical_path or str(task['id']) in critical_path:
                    colors.append('r')  # Красный для задач критического пути
                else:
                    colors.append('b')  # Синий для обычных задач
        else:
            colors = ['b'] * len(sorted_tasks)

        # Рисуем горизонтальные полосы для задач
        for i, (start, end, task, color) in enumerate(zip(start_dates, end_dates, sorted_tasks, colors)):
            # Вычисляем ширину полосы в днях
            # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Используем разницу между датами для вычисления ширины
            width_days = (end - start).days

            # Рисуем прямоугольник
            ax.barh(y_positions[i], width_days, left=start, height=0.5, align='center',
                    color=color, alpha=0.8, edgecolor='black')

            # Добавляем даты по бокам прямоугольника
            # Начальная дата
            ax.text(start - timedelta(days=0.2), y_positions[i],
                    start.strftime('%d.%m'),
                    va='center', ha='right', fontsize=8)

            # Конечная дата (невключительная)
            ax.text(end + timedelta(days=0.2), y_positions[i],
                    (end - timedelta(days=1)).strftime('%d.%m'),  # Вычитаем день для правильного отображения
                    va='center', ha='left', fontsize=8)

        # Настраиваем оси
        ax.set_yticks(y_positions)
        ax.set_yticklabels(labels)
        ax.set_xlabel('Дата')
        ax.set_ylabel('Задача')

        # Устанавливаем диапазон дат с небольшим запасом
        date_padding = timedelta(days=max(3, int((project_end_display - project_start).days * 0.05)))
        ax.set_xlim(project_start - date_padding, project_end_display + date_padding)

        # Форматируем заголовок с добавлением длительности проекта
        project_duration = (project_end - project_start).days + 1  # +1 т.к. включительно
        ax.set_title(f'Диаграмма Ганта для проекта "{project["name"]}"\nДлительность: {project_duration} дней')

        # Добавляем сетку
        ax.grid(True, axis='x', linestyle='--', alpha=0.7)

        # Форматируем даты на оси x
        date_format = mdates.DateFormatter('%d.%m.%Y')
        ax.xaxis.set_major_formatter(date_format)

        # Устанавливаем интервал для делений оси X
        if project_duration <= 14:
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        elif project_duration <= 60:
            ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))
        else:
            ax.xaxis.set_major_locator(mdates.MonthLocator())

        # Поворачиваем метки
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

        # Добавляем даты начала и окончания проекта
        ax.axvline(x=project_start, color='g', linestyle='--', alpha=0.7)
        ax.axvline(x=project_end_display, color='g', linestyle='--', alpha=0.7)

        # Подписываем даты начала и окончания проекта
        ax.text(project_start, -1, f"Начало: {project_start.strftime('%d.%m.%Y')}",
                ha='center', va='top', color='g', fontweight='bold')
        ax.text(project_end_display, -1, f"Окончание: {project_end.strftime('%d.%m.%Y')}",
                ha='center', va='top', color='g', fontweight='bold')

        # Добавляем примечание о формате дат
        fig.text(0.5, 0.01,
                 "Примечание: Конечные даты указаны невключительно. Например, задача '19.05 - 21.05' продолжается до конца дня 20.05.",
                 ha='center', fontsize=9)

        # Плотная компоновка
        fig.tight_layout(rect=[0, 0.03, 1, 0.97])  # Оставляем место для примечания

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