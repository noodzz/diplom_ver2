import datetime
from collections import defaultdict, deque
import json


class NetworkModel:
    def __init__(self):
        self.graph = None
        self.tasks = None
        self.task_mapping = None
        self.reverse_mapping = None

    def calculate(self, project, tasks):
        """
        Рассчитывает календарный план проекта, используя алгоритм Форда

        Args:
            project (dict): Информация о проекте
            tasks (list): Список задач проекта

        Returns:
            dict: Результаты расчета (длительность проекта, критический путь, даты задач)
        """
        # Проверяем, что список задач не пуст
        if not tasks:
            return {
                'duration': 0,
                'critical_path': [],
                'task_dates': {},
                'early_times': [],
                'late_times': [],
                'reserves': []
            }

        # Инициализируем переменные
        self.tasks = list(tasks)  # Создаем копию списка задач
        self.graph = self._build_graph(self.tasks)

        # Проверяем, что граф не пуст
        if not self.graph:
            return {
                'duration': 0,
                'critical_path': [],
                'task_dates': {},
                'early_times': [],
                'late_times': [],
                'reserves': []
            }

        # Проверяем граф на цикличность
        if self._has_cycle():
            raise ValueError("Обнаружен цикл в графе зависимостей. Невозможно рассчитать календарный план.")

        # Применяем алгоритм Форда для вычисления ранних времен наступления событий
        early_times = self._calculate_early_times()

        # Проверяем, что результат не пуст
        if not early_times:
            return {
                'duration': 0,
                'critical_path': [],
                'task_dates': {},
                'early_times': [],
                'late_times': [],
                'reserves': []
            }

        # Вычисляем поздние времена наступления событий
        late_times = self._calculate_late_times(early_times)

        # Проверяем, что результат не пуст
        if not late_times:
            return {
                'duration': 0,
                'critical_path': [],
                'task_dates': {},
                'early_times': early_times,
                'late_times': [],
                'reserves': []
            }

        # Определяем резервы времени
        reserves = self._calculate_reserves(early_times, late_times)

        # Находим критический путь
        critical_path = self._find_critical_path(reserves)

        # Вычисляем даты начала и окончания задач
        task_dates = self._calculate_task_dates(project['start_date'], early_times)

        # Длительность проекта
        project_duration = 0
        if early_times and len(early_times) > 0:
            project_duration = early_times[-1]  # Используем последний элемент списка

            # Проверка на корректность длительности
            if project_duration == 0 and len(task_dates) > 0:
                # Если длительность все еще 0, но есть даты задач, вычисляем по датам
                end_dates = []
                for task_id, dates in task_dates.items():
                    if 'end' in dates:
                        try:
                            end_date = datetime.datetime.strptime(dates['end'], '%Y-%m-%d')
                            start_date = datetime.datetime.strptime(project['start_date'], '%Y-%m-%d')
                            days_diff = (end_date - start_date).days + 1
                            end_dates.append(days_diff)
                        except (ValueError, TypeError):
                            pass

                if end_dates:
                    project_duration = max(end_dates)

        return {
            'duration': project_duration,
            'critical_path': critical_path,
            'task_dates': task_dates,
            'early_times': early_times,
            'late_times': late_times,
            'reserves': reserves
        }

    def _build_graph(self, tasks):
        """
        Строит сетевую модель на основе задач проекта

        Args:
            tasks (list): Список задач проекта

        Returns:
            dict: Граф зависимостей в виде списка смежности с весами (длительностями)
        """
        # Проверяем, что список задач не пуст
        if not tasks:
            return {}

        # Создаем словарь для сопоставления задач с их данными
        task_dict = {}
        for task in tasks:
            if 'id' in task:
                task_dict[task['id']] = task

        # Создаем граф зависимостей
        # Формат: {task_id: [(successor_id, duration), ...]}
        graph = defaultdict(list)

        # Добавляем все задачи как узлы
        for task in tasks:
            if 'id' in task:
                task_id = task['id']
                if task_id not in graph:
                    graph[task_id] = []

        # Строим связи между задачами на основе предшественников
        for task in tasks:
            if 'id' not in task:
                continue

            task_id = task['id']
            task_duration = task.get('duration', 0)
            predecessors = self._get_task_dependencies(task_id)

            # Для каждого предшественника добавляем связь к текущей задаче
            for pred_id in predecessors:
                if pred_id in task_dict:
                    pred_duration = task_dict[pred_id].get('duration', 0)
                    # Добавляем дугу от предшественника к текущей задаче
                    graph[pred_id].append((task_id, pred_duration))

        return dict(graph)

    def _get_task_dependencies(self, task_id):
        """
        Возвращает список идентификаторов задач, от которых зависит указанная задача

        Args:
            task_id (int): Идентификатор задачи

        Returns:
            list: Список идентификаторов задач-предшественников
        """
        dependencies = []

        # Создаем локальную копию списка задач для безопасной итерации
        tasks_copy = list(self.tasks)

        # Ищем задачу по ID
        task = None
        for t in tasks_copy:
            if 'id' in t and t['id'] == task_id:
                task = t
                break

        if task:
            # Проверяем, есть ли у задачи предшественники
            if 'predecessors' in task and task['predecessors']:
                # Дополнительно проверяем тип данных - должен быть список
                if isinstance(task['predecessors'], list):
                    dependencies = task['predecessors']
                elif isinstance(task['predecessors'], str):
                    # Пытаемся разобрать JSON или разделить строку по запятым
                    try:
                        import json
                        dependencies = json.loads(task['predecessors'])
                    except:
                        # Если не получается разобрать JSON, пробуем разделить по запятым
                        dependencies = [int(id.strip()) for id in task['predecessors'].split(',') if
                                        id.strip().isdigit()]

        return dependencies

    def _has_cycle(self):
        """
        Проверяет граф на наличие циклов используя DFS

        Returns:
            bool: True, если в графе есть цикл, иначе False
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {node: WHITE for node in self.graph}

        def dfs(node):
            if color[node] == GRAY:
                return True  # Найден цикл
            if color[node] == BLACK:
                return False

            color[node] = GRAY
            for successor, _ in self.graph.get(node, []):
                if dfs(successor):
                    return True
            color[node] = BLACK
            return False

        for node in self.graph:
            if color[node] == WHITE:
                if dfs(node):
                    return True
        return False

    def _calculate_early_times(self):
        """
        Вычисляет наиболее ранние времена завершения задач, используя алгоритм Форда

        Returns:
            dict: Словарь {task_id: early_finish_time}
        """
        if not self.graph:
            return {}

        # Инициализируем ранние времена завершения
        early_times = {}
        all_tasks = set(self.graph.keys())

        # Добавляем все задачи, которые являются преемниками
        for task_id in self.graph:
            for successor_id, _ in self.graph[task_id]:
                all_tasks.add(successor_id)

        # Инициализируем все времена нулем
        for task_id in all_tasks:
            early_times[task_id] = 0

        # Получаем длительности задач
        task_durations = {}
        for task in self.tasks:
            if 'id' in task:
                task_durations[task['id']] = task.get('duration', 0)

        # Применяем алгоритм Форда
        changed = True
        iteration = 0
        max_iterations = len(all_tasks) * 2

        while changed and iteration < max_iterations:
            changed = False
            iteration += 1

            for task_id in self.graph:
                task_duration = task_durations.get(task_id, 0)
                current_early_finish = early_times[task_id]

                # Рассчитываем раннее время завершения как максимум из:
                # 1. Текущего времени завершения
                # 2. Длительности самой задачи (если нет предшественников)
                new_early_finish = max(current_early_finish, task_duration)

                # Проверяем всех предшественников
                for pred_task in self.tasks:
                    if 'id' not in pred_task:
                        continue
                    pred_id = pred_task['id']

                    # Если текущая задача зависит от pred_id
                    dependencies = self._get_task_dependencies(task_id)
                    if pred_id in dependencies:
                        pred_early_finish = early_times.get(pred_id, 0)
                        # Добавляем длительность предшественника если она больше 0
                        pred_duration = task_durations.get(pred_id, 0)
                        if pred_duration > 0:
                            pred_finish_time = pred_duration
                        else:
                            pred_finish_time = pred_early_finish

                        # Раннее время начала текущей задачи = max(раннее время завершения предшественников)
                        new_early_finish = max(new_early_finish, pred_finish_time + task_duration)

                if new_early_finish != current_early_finish:
                    early_times[task_id] = new_early_finish
                    changed = True

        return early_times

    def _calculate_late_times(self, early_times):
        """
        Вычисляет наиболее поздние времена начала задач

        Args:
            early_times (dict): Словарь ранних времен завершения

        Returns:
            dict: Словарь поздних времен начала
        """
        if not early_times:
            return {}

        # Находим максимальное раннее время (длительность проекта)
        project_duration = max(early_times.values()) if early_times else 0

        # Инициализируем поздние времена
        late_times = {}
        for task_id in early_times:
            late_times[task_id] = project_duration

        # Получаем длительности задач
        task_durations = {}
        for task in self.tasks:
            if 'id' in task:
                task_durations[task['id']] = task.get('duration', 0)

        # Строим обратный граф зависимостей
        reverse_graph = defaultdict(list)
        for task in self.tasks:
            if 'id' not in task:
                continue
            task_id = task['id']
            dependencies = self._get_task_dependencies(task_id)
            for pred_id in dependencies:
                reverse_graph[pred_id].append(task_id)

        # Находим задачи без последователей (конечные задачи)
        end_tasks = []
        for task_id in early_times:
            if task_id not in reverse_graph or not reverse_graph[task_id]:
                end_tasks.append(task_id)

        # Устанавливаем поздние времена для конечных задач
        for task_id in end_tasks:
            task_duration = task_durations.get(task_id, 0)
            late_times[task_id] = early_times[task_id] - task_duration

        # Применяем алгоритм обратного прохода
        changed = True
        iteration = 0
        max_iterations = len(early_times) * 2

        while changed and iteration < max_iterations:
            changed = False
            iteration += 1

            for task_id in reverse_graph:
                current_late_start = late_times[task_id]
                task_duration = task_durations.get(task_id, 0)

                # Находим минимальное позднее время начала среди последователей
                min_successor_late_start = float('inf')

                for successor_id in reverse_graph[task_id]:
                    successor_late_start = late_times.get(successor_id, project_duration)
                    min_successor_late_start = min(min_successor_late_start, successor_late_start)

                if min_successor_late_start != float('inf'):
                    new_late_start = min_successor_late_start - task_duration

                    if new_late_start < current_late_start:
                        late_times[task_id] = new_late_start
                        changed = True

        return late_times

    def _calculate_reserves(self, early_times, late_times):
        """
        Вычисляет резервы времени для каждой задачи

        Args:
            early_times (dict): Словарь ранних времен завершения
            late_times (dict): Словарь поздних времен начала

        Returns:
            dict: Словарь резервов времени
        """
        reserves = {}

        # Получаем длительности задач
        task_durations = {}
        for task in self.tasks:
            if 'id' in task:
                task_durations[task['id']] = task.get('duration', 0)

        for task_id in early_times:
            if task_id in late_times:
                task_duration = task_durations.get(task_id, 0)
                # Резерв = позднее время начала - (раннее время завершения - длительность)
                early_start = early_times[task_id] - task_duration
                late_start = late_times[task_id]
                reserves[task_id] = late_start - early_start
            else:
                reserves[task_id] = 0

        return reserves

    def _find_critical_path(self, reserves):
        """
        Находит критический путь в графе

        Args:
            reserves (dict): Словарь резервов времени

        Returns:
            list: Список идентификаторов задач, образующих критический путь
        """
        if not reserves:
            return []

        # Находим все критические задачи (с нулевым резервом)
        critical_tasks = [task_id for task_id, reserve in reserves.items() if abs(reserve) < 0.001]

        if not critical_tasks:
            return []

        # Строим критический путь, следуя зависимостям
        critical_path = []

        # Находим начальные критические задачи (без предшественников или с некритическими предшественниками)
        start_tasks = []
        for task_id in critical_tasks:
            dependencies = self._get_task_dependencies(task_id)
            critical_predecessors = [dep for dep in dependencies if dep in critical_tasks]
            if not critical_predecessors:
                start_tasks.append(task_id)

        if not start_tasks:
            # Если не нашли стартовые задачи, берем первую критическую
            start_tasks = [critical_tasks[0]]

        # Строим путь от стартовой задачи
        def build_path(current_task, path, visited):
            if current_task in visited:
                return path

            visited.add(current_task)
            path.append(current_task)

            # Ищем критических последователей
            successors = []
            for task in self.tasks:
                if 'id' not in task:
                    continue
                task_id = task['id']
                dependencies = self._get_task_dependencies(task_id)
                if current_task in dependencies and task_id in critical_tasks:
                    successors.append(task_id)

            # Продолжаем путь с первым найденным критическим последователем
            if successors:
                return build_path(successors[0], path, visited)

            return path

        # Строим путь от каждой стартовой задачи и выбираем самый длинный
        best_path = []
        for start_task in start_tasks:
            path = build_path(start_task, [], set())
            if len(path) > len(best_path):
                best_path = path

        return best_path

    def _calculate_task_dates(self, project_start_date, early_times):
        """
        Вычисляет даты начала и окончания задач

        Args:
            project_start_date (str): Дата начала проекта (YYYY-MM-DD)
            early_times (dict): Словарь ранних времен завершения

        Returns:
            dict: Словарь с датами начала и окончания для каждой задачи
        """
        if not early_times:
            return {}

        start_date = datetime.datetime.strptime(project_start_date, '%Y-%m-%d')
        task_dates = {}

        # Получаем длительности задач
        task_durations = {}
        for task in self.tasks:
            if 'id' in task:
                task_durations[task['id']] = task.get('duration', 0)

        for task_id, early_finish_time in early_times.items():
            task_duration = task_durations.get(task_id, 0)

            # Раннее время начала = раннее время завершения - длительность
            early_start_time = early_finish_time - task_duration

            # Даты начала и окончания
            task_start_date = start_date + datetime.timedelta(days=early_start_time)
            task_end_date = start_date + datetime.timedelta(days=early_finish_time - 1)  # -1 так как включительно

            task_dates[task_id] = {
                'start': task_start_date.strftime('%Y-%m-%d'),
                'end': task_end_date.strftime('%Y-%m-%d')
            }

        return task_dates