#!/usr/bin/python3
import psycopg2


conn_src = psycopg2.connect(database = "bank",
                            host =     "de-edu-db.chronosavant.ru",
                            user =     "bank_etl",
                            password = "bank_etl_password",
                            port =     "5432")
                            
conn_dwh = psycopg2.connect(database = "edu",
                            host =     "de-edu-db.chronosavant.ru",
                            user =     "deaise",
                            password = "meriadocbrandybuck",
                            port =     "5432")

# Отключение автокоммита
conn_src.autocommit = False
conn_dwh.autocommit = False

# Создание курсора
cursor_src = conn_src.cursor()
cursor_dwh = conn_dwh.cursor()
