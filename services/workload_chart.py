import os
import tempfile

import numpy as np
from matplotlib import pyplot as plt


class WorkloadChart:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()

    def generate(self, project, employee_workload):
        """
        Генерирует диаграмму загрузки сотрудников

        Args:
            project (dict): Информация о проекте
            employee_workload (dict): Распределение задач по сотрудникам

        Returns:
            str: Путь к созданному файлу диаграммы
        """
        if not employee_workload:
            # Нет данных для отображения
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.text(0.5, 0.5, "Нет данных о распределении задач",
                    ha='center', va='center', fontsize=14)

            plt.tight_layout()
            chart_file = os.path.join(self.temp_dir, f"{self._create_safe_filename(project['name'])}_workload.png")
            plt.savefig(chart_file, dpi=150)
            plt.close()
            return chart_file

        # Группируем сотрудников по должностям
        positions = {}
        for employee_id, data in employee_workload.items():
            position = data['position']
            if position not in positions:
                positions[position] = []
            positions[position].append(employee_id)

        # Печатаем отладочную информацию
        print(f"Создание диаграммы загрузки для {len(employee_workload)} сотрудников")

        # Создаем фигуру
        fig_height = max(8, len(employee_workload) * 0.7)
        fig, ax = plt.subplots(figsize=(12, fig_height))

        # Подготавливаем данные для графика
        employee_names = []
        employee_durations = []
        colors = []

        # Разные цвета для разных должностей
        position_colors = {
            "Проектный менеджер": "tab:blue",
            "Технический специалист": "tab:orange",
            "Старший технический специалист": "tab:green",
            "Руководитель настройки": "tab:red",
            "Младший специалист": "tab:purple",
            "Старший специалист": "tab:brown",
            "Руководитель контента": "tab:pink"
        }

        # Заполняем данные для графика
        for position, employee_ids in positions.items():
            # Сортируем сотрудников по нагрузке (от большей к меньшей)
            sorted_employees = sorted(
                employee_ids,
                key=lambda emp_id: sum(task.get('duration', 0) for task in employee_workload[emp_id].get('tasks', [])),
                reverse=True
            )

            for employee_id in sorted_employees:
                data = employee_workload[employee_id]

                # Рассчитываем общую продолжительность задач
                total_duration = sum(task.get('duration', 0) for task in data.get('tasks', []))

                employee_names.append(f"{data['name']} ({position})")
                employee_durations.append(total_duration)
                colors.append(position_colors.get(position, "tab:gray"))

        # Создаем горизонтальную столбчатую диаграмму
        y_pos = np.arange(len(employee_names))
        bars = ax.barh(y_pos, employee_durations, align='center', color=colors, alpha=0.8)

        # Настраиваем оси
        ax.set_yticks(y_pos)
        ax.set_yticklabels(employee_names)
        ax.invert_yaxis()  # Инвертируем ось Y, чтобы сотрудники шли сверху вниз

        # Добавляем значения на диаграмму
        for i, duration in enumerate(employee_durations):
            ax.text(duration + 0.5, i, f"{duration} дней", va='center')

        # Добавляем заголовок и подписи осей
        ax.set_title(f"Распределение загрузки сотрудников в проекте '{project['name']}'")
        ax.set_xlabel('Продолжительность (дней)')

        # Добавляем среднюю нагрузку
        if employee_durations:
            avg_duration = sum(employee_durations) / len(employee_durations)
            # Округляем среднюю нагрузку до 1 десятичного знака
            avg_duration_text = f"{avg_duration:.1f}"

            # Чертим линию средней нагрузки
            avg_line = ax.axvline(x=avg_duration, color='r', linestyle='--', alpha=0.7)

            # Добавляем подпись для средней нагрузки
            ax.text(
                avg_duration + 0.5,
                len(employee_names) - 0.5,
                f"Средняя нагрузка: {avg_duration_text} дней",
                va='top',
                ha='left',
                color='r'
            )

        plt.tight_layout()

        # Сохраняем диаграмму
        chart_file = os.path.join(self.temp_dir, f"{self._create_safe_filename(project['name'])}_workload.png")
        plt.savefig(chart_file, dpi=150)
        plt.close()

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