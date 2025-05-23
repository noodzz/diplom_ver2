# Файл конфигурации с настройками бота
from typing import List


class Config:
    # Список разрешенных идентификаторов пользователей
    ALLOWED_USER_IDS: List[int] = [
        # здесь нужно указать список разрешенных ID пользователей Telegram
        6633100206,  # Пример: ваш Telegram ID
    ]

    # Настройки базы данных
    DB_NAME: str = "project_bot.db"

    # Настройки Jira (при необходимости)
    JIRA_URL: str = ""
    JIRA_USERNAME: str = ""
    JIRA_API_TOKEN: str = ""

    # Шаблоны проектов
    PROJECT_TEMPLATES = {
        1: {
            "name": "1 поток",
            "tasks": [
                {
                    "name": "Расчёт стоимостей",
                    "duration": 3,
                    "predecessors": [],
                    "position": "Проектный менеджер",
                    "is_group": False
                },
                {
                    "name": "Создание тарифов обучения",
                    "duration": 1,
                    "predecessors": ["Расчёт стоимостей"],
                    "position": "Технический специалист",
                    "is_group": False
                },
                {
                    "name": "Создание продуктовых типов и продуктов",
                    "duration": 2,
                    "predecessors": ["Создание тарифов обучения"],
                    "position": "Настройка",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Собрать таблицу для скрипта",
                            "duration": 1,
                            "position": "Технический специалист",
                            "parallel": False
                        },
                        {
                            "name": "Создать продукты и ПТ",
                            "duration": 1,
                            "position": "Старший технический специалист",
                            "parallel": False
                        }
                    ]
                },
                {
                    "name": "Создание потоков обучения",
                    "duration": 2,
                    "predecessors": ["Создание продуктовых типов и продуктов"],
                    "position": "Настройка",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Собрать таблицу для скрипта",
                            "duration": 1,
                            "position": "Технический специалист",
                            "parallel": False
                        },
                        {
                            "name": "Создать потоки",
                            "duration": 1,
                            "position": "Старший технический специалист",
                            "parallel": False
                        }
                    ]
                },
                {
                    "name": "Создание тарифов для внешнего сайта",
                    "duration": 1,
                    "predecessors": ["Создание тарифов обучения"],
                    "position": "Старший технический специалист",
                    "is_group": False
                },
                {
                    "name": "Создание продуктовых страниц для внешнего сайта",
                    "duration": 1,
                    "predecessors": ["Создание продуктовых типов и продуктов"],
                    "position": "Старший технический специалист",
                    "is_group": False
                },
                {
                    "name": "Создание продуктовых типов для внешнего сайта",
                    "duration": 1,
                    "predecessors": ["Создание продуктовых типов и продуктов"],
                    "position": "Старший технический специалист",
                    "is_group": False
                },
                {
                    "name": "Сборка и загрузка образовательных программ",
                    "duration": 2,
                    "predecessors": ["Создание продуктовых типов для внешнего сайта"],
                    "position": "Настройка",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Собрать таблицу для скрипта",
                            "duration": 1,
                            "position": "Технический специалист",
                            "parallel": False
                        },
                        {
                            "name": "Запустить скрипт",
                            "duration": 1,
                            "position": "Старший технический специалист",
                            "parallel": False
                        }
                    ]
                },
                {
                    "name": "Создание настроек групп учеников",
                    "duration": 1,
                    "predecessors": ["Создание продуктовых типов и продуктов"],
                    "position": "Руководитель настройки",
                    "is_group": False
                },
                {
                    "name": "Создание пакетных предложений",
                    "duration": 1,
                    "predecessors": ["Создание потоков обучения"],
                    "position": "Старший технический специалист",
                    "is_group": False
                },
                {
                    "name": "Создание комплектов курсов",
                    "duration": 1,
                    "predecessors": ["Создание пакетных предложений"],
                    "position": "Старший технический специалист",
                    "is_group": False
                },
                {
                    "name": "Создание связей с подарочными курсами",
                    "duration": 1,
                    "predecessors": ["Создание пакетных предложений"],
                    "position": "Технический специалист",
                    "is_group": False
                },
                {
                    "name": "Настройка порядка карточек в каталоге",
                    "duration": 1,
                    "predecessors": ["Создание продуктовых типов и продуктов"],
                    "position": "Технический специалист",
                    "is_group": False
                },
                {
                    "name": "Настройка актуального месяца покупки",
                    "duration": 1,
                    "predecessors": ["Создание потоков обучения"],
                    "position": "Старший технический специалист",
                    "is_group": False
                },
                {
                    "name": "Импорт академических часов",
                    "duration": 2,
                    "predecessors": ["Создание потоков обучения"],
                    "position": "Технический специалист",
                    "is_group": False
                },
                {
                    "name": "Постконтроль созданных объектов",
                    "duration": 2,
                    "predecessors": ["Создание тарифов для внешнего сайта", "Создание продуктовых страниц для внешнего сайта",
                                     "Сборка и загрузка образовательных программ на внешний сайт", "Создание настроек групп учеников",
                                     "Создание комплектов курсов", "Создание связей с подарочными курсами",
                                     "Настройка порядка карточек в каталоге", "Настройка актуального месяца покупки",
                                     "Импорт академических часов"],
                    "position": "Настройка",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Постконтроль объектов",
                            "duration": 2,
                            "position": "Старший технический специалист",
                            "parallel": True
                        },
                        {
                            "name": "Постконтроль объектов",
                            "duration": 2,
                            "position": "Старший технический специалист",
                            "parallel": True
                        },
                        {
                            "name": "Постконтроль объектов",
                            "duration": 2,
                            "position": "Руководитель настройки",
                            "parallel": True
                        }
                    ]
                },
                {
                    "name": "Создание модулей обучения",
                    "duration": 2,
                    "predecessors": [],
                    "position": "Контент",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Подготовить таблицу для модулей",
                            "duration": 1,
                            "position": "Младший специалист",
                            "parallel": False
                        },
                        {
                            "name": "Создать модули",
                            "duration": 1,
                            "position": "Руководитель контента",
                            "parallel": False
                        }
                    ]
                },
                {
                    "name": "Создание уровней обучения",
                    "duration": 2,
                    "predecessors": [],
                    "position": "Контент",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Подготовить таблицу для уровней",
                            "duration": 1,
                            "position": "Младший специалист",
                            "parallel": False
                        },
                        {
                            "name": "Создать уровни",
                            "duration": 1,
                            "position": "Руководитель контента",
                            "parallel": False
                        }
                    ]
                },
                {
                    "name": "Настройка связей между потоками и модулями",
                    "duration": 1,
                    "predecessors": ["Создание потоков обучения", "Создание модулей обучения"],
                    "position": "Старший специалист",
                    "is_group": False
                },
                {
                    "name": "Настройка связей между потоками и уровнями",
                    "duration": 1,
                    "predecessors": ["Создание потоков обучения", "Создание уровней обучения"],
                    "position": "Старший специалист",
                    "is_group": False
                },
                {
                    "name": "Сборка сводной таблицы для создания занятий",
                    "duration": 7,
                    "predecessors": ["Настройка связей между потоками и модулями","Настройка связей между потоками и уровнями"],
                    "position": "Контент",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Сборка сводной",
                            "duration": 7,
                            "position": "Старший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Сборка сводной",
                            "duration": 7,
                            "position": "Старший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Сборка сводной",
                            "duration": 7,
                            "position": "Старший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Сборка сводной",
                            "duration": 7,
                            "position": "Руководитель контента",
                            "parallel": True
                        }
                    ]
                },
                {
                    "name": "Создание занятий и домашних заданий",
                    "duration": 3,
                    "predecessors": ["Сборка сводной таблицы для создания занятий"],
                    "position": "Руководитель контента",
                    "is_group": False
                },
                {
                    "name": "Создание связей между уроками и модулями",
                    "duration": 1,
                    "predecessors": ["Создание занятий и домашних заданий"],
                    "position": "Контент",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Создание урок-модуль",
                            "duration": 1,
                            "position": "Старший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Создание урок-модуль",
                            "duration": 1,
                            "position": "Старший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Создание урок-модуль",
                            "duration": 1,
                            "position": "Старший специалист",
                            "parallel": True
                        }
                    ]
                },
                {
                    "name": "Создание связей продукт-урок-уровень",
                    "duration": 2,
                    "predecessors": ["Создание занятий и домашних заданий"],
                    "position": "Контент",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Создание связей продукт-урок-уровень",
                            "duration": 2,
                            "position": "Старший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Создание связей продукт-урок-уровень",
                            "duration": 2,
                            "position": "Старший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Создание связей продукт-урок-уровень",
                            "duration": 2,
                            "position": "Старший специалист",
                            "parallel": True
                        }
                    ]
                },
                {
                    "name": "Наполнение контентом занятий и домашних заданий",
                    "duration": 3,
                    "predecessors": ["Создание связей продукт-урок-уровень"],
                    "position": "Контент",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Перенос наполнения",
                            "duration": 3,
                            "position": "Младший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Перенос наполнения",
                            "duration": 3,
                            "position": "Младший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Перенос наполнения",
                            "duration": 3,
                            "position": "Младший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Перенос наполнения",
                            "duration": 3,
                            "position": "Младший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Перенос наполнения",
                            "duration": 3,
                            "position": "Младший специалист",
                            "parallel": True
                        }
                    ]
                },
                {
                    "name": "Постконтроль созданных уроков",
                    "duration": 7,
                    "predecessors": ["Создание связей продукт-урок-уровень"],
                    "position": "Контент",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Постконтроль",
                            "duration": 7,
                            "position": "Младший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Постконтроль",
                            "duration": 7,
                            "position": "Младший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Постконтроль",
                            "duration": 7,
                            "position": "Младший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Постконтроль",
                            "duration": 7,
                            "position": "Младший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Постконтроль",
                            "duration": 7,
                            "position": "Младший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Постконтроль",
                            "duration": 7,
                            "position": "Младший специалист",
                            "parallel": True
                        }
                    ]
                }
            ]
        },
        2: {
            "name": "Бесплатный курс с уровнями",
            "tasks": [
                {
                    "name": "Создание продуктовых типов и продуктов",
                    "duration": 2,
                    "predecessors": [],
                    "position": "Настройка",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Таблица для скрипта",
                            "duration": 1,
                            "position": "Технический специалист",
                            "parallel": False
                        },
                        {
                            "name": "Создание продуктов и ПТ",
                            "duration": 1,
                            "position": "Старший технический специалист",
                            "parallel": False
                        }
                    ]
                },
                {
                    "name": "Создание потоков обучения",
                    "duration": 2,
                    "predecessors": ["Создание продуктовых типов и продуктов"],
                    "position": "Настройка",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Таблица для скрипта",
                            "duration": 1,
                            "position": "Технический специалист",
                            "parallel": False
                        },
                        {
                            "name": "Создание потоков",
                            "duration": 1,
                            "position": "Старший технический специалист",
                            "parallel": False
                        }
                    ]
                },
                {
                    "name": "Постконтроль созданных объектов",
                    "duration": 1,
                    "predecessors": ["Создание потоков обучения"],
                    "position": "Руководитель настройки",
                    "is_group": False
                },
                {
                    "name": "Создание уровней",
                    "duration": 2,
                    "predecessors": [],
                    "position": "Контент",
                    "is_group": True,
                    "subtasks": [
                        {
                            "name": "Создание уровней",
                            "duration": 2,
                            "position": "Старший специалист",
                            "parallel": True
                        },
                        {
                            "name": "Создание уровней",
                            "duration": 2,
                            "position": "Старший специалист",
                            "parallel": True
                        }
                    ]
                },
            ]
        }
    }

    # Должности сотрудников
    POSITIONS = [
        "Проектный менеджер",
        "Технический специалист",
        "Старший технический специалист",
        "Руководитель настройки",
        "Младший специалист",
        "Старший специалист",
        "Руководитель контента",
    ]

    # Список сотрудников с их должностями и выходными днями
    EMPLOYEES = [
        {"id": 1, "name": "Иванов И.И.", "position": "Проектный менеджер", "days_off": [2, 6]},
        {"id": 2, "name": "Петров П.П.", "position": "Технический специалист", "days_off": [3, 5]},
        {"id": 3, "name": "Сидоров С.С.", "position": "Технический специалист", "days_off": [1, 7]},
        {"id": 4, "name": "Козлов К.К.", "position": "Старший технический специалист", "days_off": [6, 7]},
        {"id": 5, "name": "Смирнов С.С.", "position": "Старший технический специалист", "days_off": [6, 7]},
        {"id": 6, "name": "Попов П.П.", "position": "Руководитель настройки", "days_off": [6, 7]},
        {"id": 7, "name": "Младший0 В.В.", "position": "Младший специалист", "days_off": [6]},
        {"id": 8, "name": "Младший1 М.М.", "position": "Младший специалист", "days_off": [6]},
        {"id": 9, "name": "Младший2 М.М.", "position": "Младший специалист", "days_off": [4]},
        {"id": 10, "name": "Младший3 М.М.", "position": "Младший специалист", "days_off": [3]},
        {"id": 11, "name": "Младший4 М.М.", "position": "Младший специалист", "days_off": [7]},
        {"id": 12, "name": "Младший5 М.М.", "position": "Младший специалист", "days_off": [1]},
        {"id": 13, "name": "Старший0 В.В.", "position": "Старший специалист", "days_off": [5]},
        {"id": 14, "name": "Старший1 В.В.", "position": "Старший специалист", "days_off": [2]},
        {"id": 15, "name": "Старший2 В.В.", "position": "Старший специалист", "days_off": [6]},
        {"id": 16, "name": "Зайцев З.З.", "position": "Руководитель контента", "days_off": [6, 7]},
    ]