import datetime
from collections import defaultdict, deque
import json


class NetworkModel:
    def __init__(self):
        self.graph = None
        self.tasks = None
        self.task_dict = None

    def calculate(self, project, tasks):
        """
        Рассчитывает календарный план проекта, используя метод критического пути (CPM)
        ИСКЛЮЧАЕТ подзадачи из анализа

        Args:
            project (dict): Информация о проекте
            tasks (list): Список задач проекта

        Returns:
            dict: Результаты расчета
        """
        if not tasks:
            return {
                'duration': 0,
                'critical_path': [],
                'task_dates': {},
                'early_times': {},
                'late_times': {},
                'reserves': {}
            }

        try:
            # ИСПРАВЛЕНИЕ: Фильтруем подзадачи перед анализом
            main_tasks = []
            for task in tasks:
                # Исключаем подзадачи (у них есть parent_id)
                if not task.get('parent_id'):
                    main_tasks.append(task)

            print(f"[CPM Debug] Всего задач: {len(tasks)}, основных задач для CPM: {len(main_tasks)}")

            if not main_tasks:
                return self._fallback_calculation(project, tasks)

            # Используем только основные задачи для анализа
            self.tasks = list(main_tasks)
            self.task_dict = {task['id']: task for task in main_tasks if 'id' in task}

            # Строим граф зависимостей только для основных задач
            self._build_dependency_graph()

            # Проверяем на циклы
            if self._has_cycles():
                print("⚠️ Обнаружены циклические зависимости в проекте")
                return self._fallback_calculation(project, main_tasks)

            # Выполняем расчет методом критического пути
            early_start, early_finish = self._forward_pass()
            late_start, late_finish = self._backward_pass(early_finish)
            reserves = self._calculate_reserves(early_start, late_start)

            # Диагностика критических задач (только основные)
            critical_tasks_debug = [tid for tid, reserve in reserves.items() if abs(reserve) < 0.001]
            print(f"[CPM Debug] === ДИАГНОСТИКА КРИТИЧЕСКОГО ПУТИ (ТОЛЬКО ОСНОВНЫЕ ЗАДАЧИ) ===")
            print(f"[CPM Debug] Найдено {len(critical_tasks_debug)} критических основных задач:")

            critical_with_times = []
            for tid in critical_tasks_debug:
                task = self.task_dict.get(tid, {})
                start_time = early_start.get(tid, 0)
                finish_time = early_finish.get(tid, 0)
                duration = task.get('duration', 1)
                task_name = task.get('name', 'Неизвестно')

                critical_with_times.append((tid, start_time, finish_time, duration, task_name))

            # Сортируем по времени начала
            critical_with_times.sort(key=lambda x: x[1])

            total_critical_duration = 0
            for i, (tid, start_time, finish_time, duration, task_name) in enumerate(critical_with_times):
                print(f"[CPM Debug]   {i + 1}. '{task_name}' (ID: {tid})")
                print(f"[CPM Debug]      День {start_time} - {finish_time} ({duration} дн.)")

                if i == 0:
                    total_critical_duration = finish_time
                else:
                    prev_finish = critical_with_times[i - 1][2]
                    if abs(start_time - prev_finish) < 0.001:
                        total_critical_duration = finish_time
                    else:
                        total_critical_duration = max(total_critical_duration, finish_time)

            print(f"[CPM Debug] Расчетная длительность по критическому пути: {total_critical_duration} дней")

            critical_path = self._find_critical_path(reserves)

            print(f"[CPM Debug] Найден критический путь из {len(critical_path)} основных задач:")
            path_duration = 0
            for i, tid in enumerate(critical_path):
                task = self.task_dict.get(tid, {})
                duration = task.get('duration', 1)
                task_name = task.get('name', 'Неизвестно')
                start_time = early_start.get(tid, 0)

                print(f"[CPM Debug]   {i + 1}. '{task_name}' (ID: {tid}, {duration} дн.)")

                if i == 0:
                    path_duration = start_time + duration
                else:
                    path_duration = max(path_duration, start_time + duration)

            print(f"[CPM Debug] Длительность по найденному пути: {path_duration} дней")
            print(f"[CPM Debug] === КОНЕЦ ДИАГНОСТИКИ КРИТИЧЕСКОГО ПУТИ ===")

            # ИСПРАВЛЕНИЕ: Убираем некорректное добавление +1
            raw_duration = max(early_finish.values()) if early_finish else 0
            project_duration = int(raw_duration) if raw_duration > 0 else 0

            print(f"[CPM Debug] === ИСПРАВЛЕННЫЙ РАСЧЕТ ДЛИТЕЛЬНОСТИ ===")
            print(f"[CPM Debug] Время окончания проекта: {raw_duration}")
            print(f"[CPM Debug] Длительность проекта (CPM): {project_duration} дней")
            print(f"[CPM Debug] === КОНЕЦ ИСПРАВЛЕНИЯ ===")

            # Генерируем даты задач только для основных задач
            task_dates = self._calculate_task_dates(project['start_date'], early_start, early_finish)

            return {
                'duration': int(project_duration),
                'critical_path': critical_path,
                'task_dates': task_dates,
                'early_times': early_start,
                'late_times': late_start,
                'reserves': reserves
            }

        except Exception as e:
            print(f"Ошибка в сетевом анализе: {str(e)}")
            return self._fallback_calculation(project, main_tasks if 'main_tasks' in locals() else tasks)

    def _build_dependency_graph(self):
        """Строит граф зависимостей между основными задачами"""
        from collections import defaultdict

        self.predecessors = defaultdict(list)  # task_id -> [predecessor_ids]
        self.successors = defaultdict(list)  # task_id -> [successor_ids]

        for task in self.tasks:
            task_id = task['id']
            deps = self._get_task_dependencies(task_id)

            for pred_id in deps:
                # Проверяем, что предшественник тоже является основной задачей
                if pred_id in self.task_dict:
                    self.predecessors[task_id].append(pred_id)
                    self.successors[pred_id].append(task_id)

    def _get_task_dependencies(self, task_id):
        """Получает список предшественников задачи, фильтруя подзадачи"""
        task = self.task_dict.get(task_id)
        if not task:
            return []

        dependencies = []
        predecessors = task.get('predecessors', [])

        if isinstance(predecessors, list):
            dependencies = [pred for pred in predecessors if isinstance(pred, (int, str))]
        elif isinstance(predecessors, str) and predecessors.strip():
            try:
                import json
                dependencies = json.loads(predecessors)
            except:
                dependencies = [
                    int(pred.strip()) for pred in predecessors.split(',')
                    if pred.strip().isdigit()
                ]

        # Фильтруем зависимости, оставляя только основные задачи
        filtered_dependencies = []
        for dep_id in dependencies:
            if dep_id in self.task_dict:  # Проверяем, что это основная задача
                filtered_dependencies.append(dep_id)

        return filtered_dependencies

    def _has_cycles(self):
        """Проверяет граф на наличие циклов"""
        WHITE, GRAY, BLACK = 0, 1, 2
        colors = {task_id: WHITE for task_id in self.task_dict}

        def dfs(task_id):
            if colors[task_id] == GRAY:
                return True  # Цикл найден
            if colors[task_id] == BLACK:
                return False

            colors[task_id] = GRAY
            for succ_id in self.successors.get(task_id, []):
                if dfs(succ_id):
                    return True
            colors[task_id] = BLACK
            return False

        for task_id in self.task_dict:
            if colors[task_id] == WHITE:
                if dfs(task_id):
                    return True
        return False

    def _forward_pass(self):
        """Прямой проход - вычисление ранних времен"""
        early_start = {}
        early_finish = {}

        # Топологическая сортировка для правильного порядка обработки
        sorted_tasks = self._topological_sort()

        for task_id in sorted_tasks:
            task = self.task_dict[task_id]
            duration = max(1, task.get('duration', 1))

            # Раннее время начала = максимальное раннее время окончания предшественников
            max_pred_finish = 0
            for pred_id in self.predecessors.get(task_id, []):
                if pred_id in early_finish:
                    max_pred_finish = max(max_pred_finish, early_finish[pred_id])

            early_start[task_id] = max_pred_finish
            early_finish[task_id] = early_start[task_id] + duration

        # Сохраняем для использования в поиске критического пути
        self._early_start_cache = early_start

        return early_start, early_finish

    def _backward_pass(self, early_finish):
        """Обратный проход - вычисление поздних времен"""
        late_start = {}
        late_finish = {}

        # Определяем общую длительность проекта
        project_duration = max(early_finish.values()) if early_finish else 0

        # Получаем задачи в обратном топологическом порядке
        sorted_tasks = list(reversed(self._topological_sort()))

        for task_id in sorted_tasks:
            task = self.task_dict[task_id]
            duration = max(1, task.get('duration', 1))

            # Для конечных задач позднее время окончания = раннему времени окончания
            if not self.successors.get(task_id):
                late_finish[task_id] = early_finish[task_id]
            else:
                # Позднее время окончания = минимальное позднее время начала последователей
                min_succ_start = float('inf')
                for succ_id in self.successors.get(task_id, []):
                    if succ_id in late_start:
                        min_succ_start = min(min_succ_start, late_start[succ_id])

                late_finish[task_id] = min_succ_start if min_succ_start != float('inf') else project_duration

            late_start[task_id] = late_finish[task_id] - duration

        return late_start, late_finish

    def _topological_sort(self):
        """Топологическая сортировка задач"""
        in_degree = {task_id: len(self.predecessors.get(task_id, [])) for task_id in self.task_dict}
        queue = deque([task_id for task_id, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            task_id = queue.popleft()
            result.append(task_id)

            for succ_id in self.successors.get(task_id, []):
                in_degree[succ_id] -= 1
                if in_degree[succ_id] == 0:
                    queue.append(succ_id)

        return result

    def _calculate_reserves(self, early_start, late_start):
        """Вычисляет резервы времени"""
        reserves = {}
        for task_id in early_start:
            if task_id in late_start:
                reserves[task_id] = late_start[task_id] - early_start[task_id]
            else:
                reserves[task_id] = 0
        return reserves

    def _find_critical_path(self, reserves):
        """Находит критический путь"""
        # Критические задачи имеют нулевой резерв
        critical_tasks = [task_id for task_id, reserve in reserves.items() if abs(reserve) < 0.001]

        if not critical_tasks:
            return []

        # Строим полный критический путь
        path = []

        # Находим начальную критическую задачу (без критических предшественников)
        start_tasks = []
        for task_id in critical_tasks:
            critical_preds = [pred for pred in self.predecessors.get(task_id, []) if pred in critical_tasks]
            if not critical_preds:
                start_tasks.append(task_id)

        if not start_tasks:
            # Если не нашли стартовые задачи, возвращаем все критические в порядке времени начала
            early_times = getattr(self, '_early_start_cache', {})
            return sorted(critical_tasks, key=lambda tid: early_times.get(tid, 0))

        # Строим путь от начальной задачи, проходя через все критические задачи
        def build_complete_path(current, visited_path):
            if current in visited_path:
                return visited_path  # Избегаем циклов

            new_path = visited_path + [current]

            # Ищем всех критических преемников
            critical_successors = []
            for succ_id in self.successors.get(current, []):
                if succ_id in critical_tasks and succ_id not in visited_path:
                    critical_successors.append(succ_id)

            if not critical_successors:
                return new_path  # Конец пути

            # Если есть несколько преемников, выбираем тот, который ведет к самому длинному пути
            best_path = new_path
            for succ_id in critical_successors:
                candidate_path = build_complete_path(succ_id, new_path)
                if len(candidate_path) > len(best_path):
                    best_path = candidate_path

            return best_path

        # Строим путь от каждой начальной задачи и выбираем самый длинный
        best_path = []
        for start_task in start_tasks:
            candidate_path = build_complete_path(start_task, [])
            if len(candidate_path) > len(best_path):
                best_path = candidate_path

        # Если путь все еще неполный, добавляем оставшиеся критические задачи
        remaining_critical = [tid for tid in critical_tasks if tid not in best_path]
        if remaining_critical:
            # Сортируем по времени начала и добавляем
            early_times = getattr(self, '_early_start_cache', {})
            remaining_sorted = sorted(remaining_critical, key=lambda tid: early_times.get(tid, 0))

            # Проверяем, можно ли их логически включить в путь
            for task_id in remaining_sorted:
                # Проверяем, есть ли связь с уже включенными задачами
                can_include = False
                for path_task in best_path:
                    if (task_id in self.successors.get(path_task, []) or
                            path_task in self.successors.get(task_id, [])):
                        can_include = True
                        break

                if can_include or not best_path:  # Включаем если есть связь или путь пуст
                    best_path.append(task_id)

        return best_path

    def _calculate_task_dates(self, project_start_date, early_start, early_finish):
        """Вычисляет календарные даты задач"""
        try:
            start_date = datetime.datetime.strptime(project_start_date, '%Y-%m-%d')
        except:
            start_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        task_dates = {}

        for task_id in early_start:
            try:
                task_start = start_date + datetime.timedelta(days=early_start[task_id])
                task_end = start_date + datetime.timedelta(days=early_finish[task_id] - 1)

                task_dates[task_id] = {
                    'start': task_start.strftime('%Y-%m-%d'),
                    'end': task_end.strftime('%Y-%m-%d')
                }
            except:
                # В случае ошибки используем даты по умолчанию
                duration = self.task_dict[task_id].get('duration', 1)
                task_dates[task_id] = {
                    'start': start_date.strftime('%Y-%m-%d'),
                    'end': (start_date + datetime.timedelta(days=duration - 1)).strftime('%Y-%m-%d')
                }

        return task_dates

    def _fallback_calculation(self, project, tasks):
        """Упрощенный расчет в случае ошибок"""
        try:
            start_date = datetime.datetime.strptime(project['start_date'], '%Y-%m-%d')
        except:
            start_date = datetime.datetime.now()

        task_dates = {}
        current_date = start_date

        for task in tasks:
            task_id = task['id']
            duration = max(1, task.get('duration', 1))

            task_dates[task_id] = {
                'start': current_date.strftime('%Y-%m-%d'),
                'end': (current_date + datetime.timedelta(days=duration - 1)).strftime('%Y-%m-%d')
            }

            current_date += datetime.timedelta(days=duration)

        total_duration = sum(task.get('duration', 1) for task in tasks)

        return {
            'duration': total_duration,
            'critical_path': [task['id'] for task in tasks],
            'task_dates': task_dates,
            'early_times': {},
            'late_times': {},
            'reserves': {}
        }