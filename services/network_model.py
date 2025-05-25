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
        ИСПРАВЛЕННАЯ версия расчета календарного плана
        """
        if not tasks:
            return {
                'duration': 0,
                'critical_path': [],
                'task_dates': {},
                'early_times': [],
                'late_times': [],
                'reserves': []
            }

        self.tasks = list(tasks)
        self.graph = self._build_graph(self.tasks)

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
            raise ValueError("Обнаружен цикл в графе зависимостей.")

        # Применяем алгоритм Форда
        early_times = self._calculate_early_times()
        if not early_times:
            return {
                'duration': 0,
                'critical_path': [],
                'task_dates': {},
                'early_times': [],
                'late_times': [],
                'reserves': []
            }

        late_times = self._calculate_late_times(early_times)
        reserves = self._calculate_reserves(early_times, late_times)
        critical_path = self._find_critical_path(reserves)
        task_dates = self._calculate_task_dates(project['start_date'], early_times)

        # ОТЛАДКА
        print(f"=== ОТЛАДКА АЛГОРИТМА ФОРДА ===")
        print(f"Ранние времена: {early_times}")
        print(f"Количество узлов: {len(early_times)}")
        if early_times:
            print(f"Время стока (early_times[-1]): {early_times[-1]}")

        # ИСПРАВЛЕНИЕ 1: Альтернативный расчет через задачи
        alternative_duration = 0
        if task_dates:
            start_dates = []
            end_dates = []

            for task_id, dates in task_dates.items():
                if 'start' in dates and 'end' in dates:
                    try:
                        start_date = datetime.datetime.strptime(dates['start'], '%Y-%m-%d')
                        end_date = datetime.datetime.strptime(dates['end'], '%Y-%m-%d')
                        start_dates.append(start_date)
                        end_dates.append(end_date)
                    except ValueError:
                        continue

            if start_dates and end_dates:
                project_start = min(start_dates)
                project_end = max(end_dates)
                alternative_duration = (project_end - project_start).days + 1
                print(
                    f"Альтернативный расчет: {alternative_duration} дней ({project_start.strftime('%Y-%m-%d')} -> {project_end.strftime('%Y-%m-%d')})")

        # ИСПРАВЛЕНИЕ 2: Расчет через максимальное время завершения задач
        max_task_finish_time = 0
        for task in self.tasks:
            task_id = task['id']
            if task_id in self.task_mapping:
                node = self.task_mapping[task_id]
                if node < len(early_times):
                    task_finish_time = early_times[node] + task.get('duration', 0)
                    max_task_finish_time = max(max_task_finish_time, task_finish_time)

        print(f"Максимальное время завершения задач: {max_task_finish_time}")

        # Выбираем наиболее разумную длительность
        ford_duration = early_times[-1] if early_times else 0

        # Если альтернативные расчеты дают разумные результаты, используем их
        candidates = [d for d in [ford_duration, alternative_duration, max_task_finish_time] if d > 0]

        if len(candidates) > 1:
            # Если есть несколько кандидатов, выбираем наименьший разумный
            final_duration = min(candidates)
            print(f"Кандидаты на длительность: {candidates}, выбран: {final_duration}")
        else:
            final_duration = ford_duration
            print(f"Используем стандартный расчет: {final_duration}")

        print(f"=== КОНЕЦ ОТЛАДКИ ===")

        return {
            'duration': final_duration,
            'critical_path': critical_path,
            'task_dates': task_dates,
            'early_times': early_times,
            'late_times': late_times,
            'reserves': reserves
        }

    def _build_graph(self, tasks):
        """
        ИСПРАВЛЕННАЯ версия построения графа с правильным стоком
        """
        if not tasks:
            return {}

        # Создаем маппинг задач -> узлы графа
        self.task_mapping = {}
        self.reverse_mapping = {}

        # Узел 0 - фиктивный источник
        node_id = 1

        # Создаем маппинг для всех задач
        for task in tasks:
            if 'id' in task:
                task_id = task['id']
                self.task_mapping[task_id] = node_id
                self.reverse_mapping[node_id] = task_id
                node_id += 1

        if node_id == 1:  # Нет задач
            return {}

        # Узел node_id - фиктивный сток
        sink_id = node_id
        self.reverse_mapping[sink_id] = 'sink'

        # Инициализируем граф
        graph = defaultdict(list)

        # Определяем задачи, которые имеют последователей
        tasks_with_successors = set()

        for task in tasks:
            task_id = task['id']
            dependencies = self._get_task_dependencies(task_id)

            # Все задачи, от которых зависит текущая, имеют последователей
            for dep_id in dependencies:
                tasks_with_successors.add(dep_id)

        # Строим граф
        for task in tasks:
            task_id = task['id']
            task_node = self.task_mapping[task_id]
            task_duration = task.get('duration', 0)

            dependencies = self._get_task_dependencies(task_id)

            if not dependencies:
                # Задача без предшественников - соединяем с источником
                # ИСПРАВЛЕНИЕ: источник соединяется с задачей дугой длительности 0
                graph[0].append((task_node, 0))
            else:
                # Соединяем с предшественниками
                for dep_id in dependencies:
                    if dep_id in self.task_mapping:
                        dep_node = self.task_mapping[dep_id]
                        # ИСПРАВЛЕНИЕ: дуга имеет длительность ПРЕДШЕСТВУЮЩЕЙ задачи
                        dep_task = next((t for t in tasks if t['id'] == dep_id), None)
                        dep_duration = dep_task.get('duration', 0) if dep_task else 0
                        graph[dep_node].append((task_node, dep_duration))

            # Если задача не имеет последователей - соединяем со стоком
            if task_id not in tasks_with_successors:
                # ИСПРАВЛЕНИЕ: сток соединяется с задачей дугой длительности задачи
                graph[task_node].append((sink_id, task_duration))

        return graph

    def _get_task_dependencies(self, task_id):
        """
        ИСПРАВЛЕННАЯ версия получения зависимостей
        """
        dependencies = []

        task = None
        for t in self.tasks:
            if t.get('id') == task_id:
                task = t
                break

        if not task:
            return dependencies

        predecessors = task.get('predecessors')
        if not predecessors:
            return dependencies

        # Обработка разных форматов предшественников
        if isinstance(predecessors, list):
            dependencies = predecessors
        elif isinstance(predecessors, str):
            if predecessors.strip() in ["NULL", "", "null"]:
                return dependencies

            try:
                # Пытаемся разобрать JSON
                dependencies = json.loads(predecessors)
                if not isinstance(dependencies, list):
                    dependencies = [dependencies]
            except:
                # Пытаемся разделить по запятым
                if ',' in predecessors:
                    dependencies = [int(p.strip()) for p in predecessors.split(',')
                                    if p.strip().isdigit()]
                elif predecessors.strip().isdigit():
                    dependencies = [int(predecessors.strip())]

        # Фильтруем только существующие задачи
        valid_dependencies = []
        for dep_id in dependencies:
            if any(t.get('id') == dep_id for t in self.tasks):
                valid_dependencies.append(dep_id)

        return valid_dependencies

    def _calculate_early_times(self):
        """
        ИСПРАВЛЕННАЯ версия расчета ранних времен
        """
        if not self.graph:
            return []

        try:
            n = max(max(self.graph.keys()), max(neighbor for neighbors in self.graph.values()
                                                for neighbor, _ in neighbors)) + 1
        except ValueError:
            return []

        early_times = [0] * n

        # Алгоритм Беллмана-Форда для поиска максимальных путей
        changed = True
        iterations = 0
        max_iterations = n * 2

        while changed and iterations < max_iterations:
            changed = False
            iterations += 1

            for node in self.graph:
                for neighbor, weight in self.graph[node]:
                    if neighbor < n and early_times[neighbor] < early_times[node] + weight:
                        early_times[neighbor] = early_times[node] + weight
                        changed = True

        return early_times

    def _calculate_late_times(self, early_times):
        """
        ИСПРАВЛЕННАЯ версия расчета поздних времен
        """
        if not early_times:
            return []

        n = len(early_times)
        if n == 0:
            return []

        # Длительность проекта = ранее время последнего события
        project_duration = early_times[-1]

        # Инициализируем поздние времена
        late_times = [project_duration] * n

        # Строим обратный граф
        reverse_graph = defaultdict(list)
        for node in self.graph:
            for neighbor, weight in self.graph[node]:
                if neighbor < n:
                    reverse_graph[neighbor].append((node, weight))

        # Алгоритм обратного прохода
        changed = True
        iterations = 0
        max_iterations = n * 2

        while changed and iterations < max_iterations:
            changed = False
            iterations += 1

            for node in range(n - 1, -1, -1):
                for predecessor, weight in reverse_graph.get(node, []):
                    if predecessor < n:
                        new_late_time = late_times[node] - weight
                        if late_times[predecessor] > new_late_time:
                            late_times[predecessor] = new_late_time
                            changed = True

        return late_times

    def _calculate_reserves(self, early_times, late_times):
        """
        Вычисляет резервы времени
        """
        if len(early_times) != len(late_times):
            return []

        reserves = []
        for i in range(len(early_times)):
            reserve = late_times[i] - early_times[i]
            reserves.append(max(0, reserve))  # Резерв не может быть отрицательным

        return reserves

    def _find_critical_path(self, reserves):
        """
        Находит критический путь
        """
        critical_nodes = []

        if not reserves or len(reserves) <= 2:
            return critical_nodes

        for node, reserve in enumerate(reserves):
            # Критические узлы имеют нулевой резерв (исключаем источник и сток)
            if reserve == 0 and node > 0 and node < len(reserves) - 1:
                critical_nodes.append(node)

        # Преобразуем узлы в ID задач
        critical_path = []
        for node in critical_nodes:
            if node in self.reverse_mapping and self.reverse_mapping[node] != 'sink':
                critical_path.append(self.reverse_mapping[node])

        return critical_path

    def _has_cycle(self):
        """
        Проверяет граф на наличие циклов
        """
        visited = set()
        rec_stack = set()

        def dfs(node):
            visited.add(node)
            rec_stack.add(node)

            for neighbor, _ in self.graph.get(node, []):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        for node in list(self.graph.keys()):
            if node not in visited:
                if dfs(node):
                    return True

        return False

    def _calculate_task_dates(self, project_start_date, early_times):
        """
        Вычисляет даты начала и окончания задач
        """
        start_date = datetime.datetime.strptime(project_start_date, '%Y-%m-%d')
        task_dates = {}

        for task in self.tasks:
            task_id = task['id']
            if task_id in self.task_mapping:
                node = self.task_mapping[task_id]
                if node < len(early_times):
                    task_start = start_date + datetime.timedelta(days=early_times[node])
                    task_duration = task.get('duration', 1)
                    task_end = task_start + datetime.timedelta(days=task_duration - 1)

                    task_dates[task_id] = {
                        'start': task_start.strftime('%Y-%m-%d'),
                        'end': task_end.strftime('%Y-%m-%d')
                    }

        return task_dates