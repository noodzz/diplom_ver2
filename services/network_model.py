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
            dict: Граф зависимостей
        """
        # Проверяем, что список задач не пуст
        if not tasks:
            return {}

        # Создаем словарь для сопоставления идентификаторов задач с вершинами графа
        self.task_mapping = {}
        self.reverse_mapping = {}

        # Инициализируем граф
        graph = defaultdict(list)

        # Добавляем фиктивный источник (вершина 0)
        graph[0] = []

        # Добавляем задачи в граф
        node_id = 1
        for task in tasks:
            if 'id' in task:  # Проверяем, что задача имеет id
                self.task_mapping[task['id']] = node_id
                self.reverse_mapping[node_id] = task['id']
                node_id += 1

        # Проверяем, что у нас есть хотя бы одна задача
        if node_id == 1:
            return {}

        # Добавляем фиктивный сток (вершина node_id)
        self.reverse_mapping[node_id] = 'sink'
        sink_id = node_id

        # Сначала получаем все зависимости и зависимые задачи
        task_dependencies = {}
        task_has_dependents = set()

        for task in tasks:
            if 'id' not in task:  # Пропускаем задачи без id
                continue

            task_id = task['id']
            dependencies = self._get_task_dependencies(task_id)
            task_dependencies[task_id] = dependencies

            # Отмечаем все задачи, от которых зависит текущая
            for dep_id in dependencies:
                task_has_dependents.add(dep_id)

        # Добавляем дуги в граф
        for task in tasks:
            if 'id' not in task:  # Пропускаем задачи без id
                continue

            task_id = task['id']

            # Проверяем, есть ли у задачи поле duration
            if 'duration' not in task:
                continue

            dependencies = task_dependencies.get(task_id, [])

            if dependencies:
                # Если есть зависимости, добавляем дуги от них к текущей задаче
                for dep_id in dependencies:
                    if dep_id in self.task_mapping:
                        graph[self.task_mapping[dep_id]].append((self.task_mapping[task_id], task['duration']))
            else:
                # Если нет зависимостей, добавляем дугу от источника
                graph[0].append((self.task_mapping[task_id], 0))

            # Если нет зависящих задач, добавляем дугу к стоку
            if task_id not in task_has_dependents:
                graph[self.task_mapping[task_id]].append((sink_id, 0))

        return graph

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
                        dependencies = [id.strip() for id in task['predecessors'].split(',')]

        print(f"Зависимости для задачи {task_id}: {dependencies}")
        return dependencies

    def _has_cycle(self):
        """
        Проверяет граф на наличие циклов

        Returns:
            bool: True, если в графе есть цикл, иначе False
        """
        visited = set()
        rec_stack = set()

        # Создаем копию графа для безопасной итерации
        graph_copy = {k: list(v) for k, v in self.graph.items()}

        def dfs(node):
            visited.add(node)
            rec_stack.add(node)

            for neighbor, _ in graph_copy.get(node, []):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        # Создаем копию ключей для безопасной итерации
        graph_nodes = list(graph_copy.keys())

        for node in graph_nodes:
            if node not in visited:
                if dfs(node):
                    return True

        return False

    def _calculate_early_times(self):
        """
        Вычисляет наиболее ранние времена наступления событий, используя алгоритм Форда

        Returns:
            list: Список ранних времен наступления для каждой вершины
        """
        # Проверяем, что граф не пуст
        if not self.graph:
            return []

        # Определяем количество вершин в графе
        try:
            n = max(self.graph.keys()) + 1
        except ValueError:  # Если граф пуст
            return []

        # Инициализируем массив ранних времен
        early_times = [0] * n

        # Создаем копию графа для безопасной итерации
        graph_copy = {k: list(v) for k, v in self.graph.items()}

        # Флаг для отслеживания изменений
        changed = True
        iteration_count = 0
        max_iterations = n * 10  # Ограничение на количество итераций для предотвращения бесконечного цикла

        # Выполняем алгоритм Форда
        while changed and iteration_count < max_iterations:
            changed = False
            iteration_count += 1

            for node in graph_copy:
                for neighbor, weight in graph_copy[node]:
                    # Проверяем, что neighbor не выходит за пределы списка
                    if 0 <= neighbor < n:
                        if early_times[neighbor] < early_times[node] + weight:
                            early_times[neighbor] = early_times[node] + weight
                            changed = True

        return early_times

    def _calculate_late_times(self, early_times):
        """
        Вычисляет наиболее поздние времена наступления событий

        Args:
            early_times (list): Список ранних времен наступления

        Returns:
            list: Список поздних времен наступления для каждой вершины
        """
        # Проверяем, что список ранних времен не пуст
        if not early_times:
            return []

        # Определяем количество вершин в графе
        n = len(early_times)

        # Проверяем, что у нас есть хотя бы один узел (источник)
        if n == 0:
            return []

        # Общая длительность проекта - это раннее время наступления последнего события
        project_duration = early_times[n - 1]

        # Инициализируем массив поздних времен
        late_times = [project_duration] * n

        # Строим обратный граф
        reverse_graph = defaultdict(list)

        # Создаем копию графа для безопасной итерации
        graph_copy = {k: list(v) for k, v in self.graph.items()}

        for node in graph_copy:
            for neighbor, weight in graph_copy[node]:
                # Проверяем, что neighbor не выходит за пределы списка
                if 0 <= neighbor < n:
                    reverse_graph[neighbor].append((node, weight))

        # Выполняем алгоритм Форда для обратного графа
        changed = True
        iteration_count = 0
        max_iterations = n * 10  # Ограничение на количество итераций для предотвращения бесконечного цикла

        while changed and iteration_count < max_iterations:
            changed = False
            iteration_count += 1

            for node in range(n - 1, -1, -1):
                for neighbor, weight in reverse_graph.get(node, []):
                    # Проверяем, что neighbor не выходит за пределы списка
                    if 0 <= neighbor < n:
                        if late_times[neighbor] > late_times[node] - weight:
                            late_times[neighbor] = late_times[node] - weight
                            changed = True

        return late_times

    def _calculate_reserves(self, early_times, late_times):
        """
        Вычисляет резервы времени для каждой вершины

        Args:
            early_times (list): Список ранних времен наступления
            late_times (list): Список поздних времен наступления

        Returns:
            list: Список резервов времени для каждой вершины
        """
        reserves = []
        for i in range(len(early_times)):
            reserves.append(late_times[i] - early_times[i])

        return reserves

    def _find_critical_path(self, reserves):
        """
        Находит критический путь в графе

        Args:
            reserves (list): Список резервов времени

        Returns:
            list: Список идентификаторов задач, образующих критический путь
        """
        critical_nodes = []

        if not reserves or len(reserves) <= 2:  # Проверяем, что у нас есть хотя бы один узел (кроме источника и стока)
            return critical_nodes

        for node, reserve in enumerate(reserves):
            if reserve == 0 and node > 0 and node < len(reserves) - 1:  # Исключаем источник и сток
                critical_nodes.append(node)

        # Преобразуем идентификаторы вершин в идентификаторы задач
        critical_path = []
        for node in critical_nodes:
            if node in self.reverse_mapping and self.reverse_mapping[node] != 'sink':
                critical_path.append(self.reverse_mapping[node])

        return critical_path

    def _calculate_task_dates(self, project_start_date, early_times):
        """
        Вычисляет даты начала и окончания задач с учетом зависимостей

        Args:
            project_start_date (str): Дата начала проекта (YYYY-MM-DD)
            early_times (list): Список ранних времен наступления

        Returns:
            dict: Словарь с датами начала и окончания для каждой задачи
        """
        start_date = datetime.datetime.strptime(project_start_date, '%Y-%m-%d')

        # Словарь с датами для задач
        task_dates = {}

        # Проверяем, что у нас есть хотя бы один узел (кроме источника и стока)
        if len(early_times) <= 2:
            return task_dates

        # Получаем список всех задач с их именами и ID
        task_list = []
        for task in self.tasks:
            if 'id' in task:
                task_list.append(task)

        # Строим словарь зависимостей
        task_dependencies = {}
        for task in task_list:
            task_id = task['id']
            predecessors = []

            # Проверяем, есть ли у задачи предшественники
            if 'predecessors' in task and task['predecessors']:
                predecessors = task['predecessors']

            task_dependencies[task_id] = predecessors

        # Вспомогательная функция для проверки, все ли предшественники задачи имеют даты
        def are_all_predecessors_scheduled(task_id):
            predecessors = task_dependencies.get(task_id, [])
            return all(pred_id in task_dates for pred_id in predecessors)

        # Вспомогательная функция для получения самой поздней даты окончания предшественников
        def get_latest_predecessor_end_date(task_id):
            predecessors = task_dependencies.get(task_id, [])
            if not predecessors:
                return start_date

            end_dates = []
            for pred_id in predecessors:
                if pred_id in task_dates:
                    end_date = datetime.datetime.strptime(task_dates[pred_id]['end'], '%Y-%m-%d')
                    end_dates.append(end_date)

            if end_dates:
                return max(end_dates) + datetime.timedelta(days=1)  # Добавляем 1 день для перехода
            else:
                return start_date

        # Определяем задачи без предшественников
        tasks_without_predecessors = [task['id'] for task in task_list
                                      if not task_dependencies.get(task['id'], [])]

        # Сначала планируем задачи без предшественников, начиная с даты начала проекта
        for task_id in tasks_without_predecessors:
            # Находим задачу
            task = None
            for t in task_list:
                if t['id'] == task_id:
                    task = t
                    break

            if task:
                # Дата начала - дата начала проекта
                task_start = start_date

                # Дата окончания = дата начала + длительность - 1
                task_end = task_start + datetime.timedelta(days=task['duration'] - 1)

                # Добавляем даты в словарь
                task_dates[task_id] = {
                    'start': task_start.strftime('%Y-%m-%d'),
                    'end': task_end.strftime('%Y-%m-%d')
                }

        # Затем планируем остальные задачи в порядке зависимостей
        while len(task_dates) < len(task_list):
            # Флаг, показывающий, планировали ли мы какую-то задачу на этой итерации
            scheduled_task = False

            for task in task_list:
                task_id = task['id']

                # Пропускаем уже запланированные задачи
                if task_id in task_dates:
                    continue

                # Проверяем, все ли предшественники задачи уже запланированы
                if are_all_predecessors_scheduled(task_id):
                    # Определяем дату начала задачи как самую позднюю дату окончания среди предшественников
                    task_start = get_latest_predecessor_end_date(task_id)

                    # Дата окончания = дата начала + длительность - 1
                    task_end = task_start + datetime.timedelta(days=task['duration'] - 1)

                    # Добавляем даты в словарь
                    task_dates[task_id] = {
                        'start': task_start.strftime('%Y-%m-%d'),
                        'end': task_end.strftime('%Y-%m-%d')
                    }

                    scheduled_task = True

            # Если ни одна задача не была запланирована на этой итерации,
            # и мы ещё не запланировали все задачи, значит, есть циклическая зависимость
            if not scheduled_task and len(task_dates) < len(task_list):
                print("Внимание: обнаружена циклическая зависимость или некоторые задачи не могут быть запланированы.")

                # Планируем оставшиеся задачи на основе ранних сроков
                for task in task_list:
                    task_id = task['id']

                    if task_id not in task_dates:
                        # Находим узел в графе
                        node = None
                        for n, tid in self.reverse_mapping.items():
                            if tid == task_id:
                                node = n
                                break

                        if node is not None and node < len(early_times):
                            # Определяем дату начала на основе ранних сроков
                            task_start = start_date + datetime.timedelta(days=early_times[node])

                            # Дата окончания = дата начала + длительность - 1
                            task_end = task_start + datetime.timedelta(days=task['duration'] - 1)

                            # Добавляем даты в словарь
                            task_dates[task_id] = {
                                'start': task_start.strftime('%Y-%m-%d'),
                                'end': task_end.strftime('%Y-%m-%d')
                            }

                # Выходим из цикла, так как мы обработали все оставшиеся задачи
                break

        # Для задач, которые всё ещё не запланированы (например, из-за ошибок в данных),
        # планируем их на дату начала проекта
        for task in task_list:
            task_id = task['id']
            if task_id not in task_dates:
                task_dates[task_id] = {
                    'start': start_date.strftime('%Y-%m-%d'),
                    'end': (start_date + datetime.timedelta(days=task['duration'] - 1)).strftime('%Y-%m-%d')
                }

        return task_dates