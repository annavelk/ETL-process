#!/usr/bin/python3
import psycopg2
import pandas as pd
import os
import glob
import logging
import subprocess
import pathlib
from pathlib import Path
from py_scripts.connect import conn_src
from py_scripts.connect import conn_dwh
from py_scripts.connect import cursor_src
from py_scripts.connect import cursor_dwh
import smtplib

#Получаем строку, содержащую путь к рабочей директории:
dir_path = pathlib.Path.cwd()

print('''
0. СОЗДАНИЕ ПОДКЛЮЧЕНИЙ''')
# Создание подключения к PostgreSQL. 
# Данные импортируются из отдельного файла, к которому может быть ограничен доступ в целях безопасности
path_conn = Path(dir_path, 'py_scripts', 'connect.py')
####################################################################################
# Объединяем полученную строку с недостающими частями пути для загрузки файлов
# Транзакции
path_trans = Path(dir_path, 'py_scripts', 'transaction.py')

# Черный список паспорт
path_bl = Path(dir_path, 'py_scripts', 'black_list.py')

# Терминалы
path_term = Path(dir_path, 'py_scripts', 'terminals.py')

# Карты
path_cards = Path(dir_path, 'py_scripts', 'cards.py')

# Клиенты
path_cl = Path(dir_path, 'py_scripts', 'clients.py')

# Аккаунты
path_acc = Path(dir_path, 'py_scripts', 'accounts.py')

# Отчёты
path_rep = Path(dir_path, 'py_scripts', 'report.py')

program_list = [path_conn, path_trans, path_bl, path_term, path_cards, path_cl, path_acc, path_rep]

for program in program_list:
    subprocess.run(program, capture_output=False)

####################################################################################

# Закрываем соединение
print('8. Закрытие подключений')
cursor_src.close()
cursor_dwh.close()
conn_src.close()
conn_dwh.close()
                           

                          

