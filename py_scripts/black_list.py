#!/usr/bin/python3
import psycopg2
import pandas as pd
import os
import glob
from connect import cursor_src
from connect import cursor_dwh
from connect import conn_dwh
####################################################################################

# Определяю переменные для таблиц
meta_table = 'vean_meta_date'
schema_name = 'deaise'
dwh_table_name = 'vean_dwh_fact_passport_blacklist'
stg_table_name = 'vean_stg_blacklist'
stg_column = ['date', 'passport']
tgt_column = ['passport_num', 'entry_dt']
stg_key = 'passport'
tgt_key = 'passport_num'
update_dt_column = "to_date(date, 'YYYY-MM-DD')"

print('''
2. ОБНОВЛЕНИЕ ТАБЛИЦ: ''', stg_table_name, ', ', dwh_table_name)


# Забираю файлы по маске названия
files_name = glob.glob('passport_blacklist_*.xlsx')
# Сортирую файлы, определяю количество файлов в каталоге
files_sort = sorted(files_name, key=lambda a: a[23:27] + a[21:23] + a[19:21])
print('Текущий каталог содержит файлов - ', len(files_name), ': ', files_sort)
# Определяю дату последнего изменения и преобразую в строку.
cursor_dwh.execute(f"""SELECT 
                        coalesce( 
                            (select max_update_dt 
                            from {schema_name}.{meta_table} 
                            where schema_name = '{schema_name}' 
                            and table_name = '{dwh_table_name}'),
	                        to_timestamp('1900-01-01','YYYY-MM-DD') ) 
                        from {schema_name}.{meta_table} 
                        where schema_name = '{schema_name}' 
                        and table_name = '{dwh_table_name}' """)

meta_data = str(cursor_dwh.fetchone()[0]).split(' ')[0]
print('Дата последнего обновления: ', meta_data)
# Временная переменная для определения большей даты при загрузке из нескольких файлов
next_date = meta_data
# Циклом прохожусь по файлам, записывая новые данные в stage, определяю наибольшую дату
for i in files_sort:    
    file_date = (i.split('_')[2]).split('.')[0]
    file_date = file_date[4:8:1] + '-' + file_date[2:4:1] + '-' + file_date[0:2:1]
    if file_date > meta_data:
        print("""
        2.1 Очистка """, stg_table_name)
        cursor_dwh.execute( f"DELETE FROM {schema_name}.{stg_table_name} " )
        ## deaise.vean_stg_transactions
        print('2.2 Загрузка данных из файлов в ', stg_table_name)
        df = pd.read_excel( i, sheet_name='blacklist', header=0, index_col=None )
        cursor_dwh.executemany( f"""INSERT INTO {schema_name}.{stg_table_name}(
                                {','.join(stg_column)}) 
                            VALUES( %s, %s)""", df.values.tolist() )
        row = len(df.index)
        print(i,': Поступило из файла ', row, ' строк')
        if file_date > next_date:
            next_date = file_date
        ## deaise.vean_dwh_fact_transactions
        print('2.3. Загрузка данных в ', dwh_table_name)
        ## Загрузка новых данных из stage

        cursor_dwh.execute( f""" insert into {schema_name}.{dwh_table_name} ( 
                                    {','.join(tgt_column)}
                                )
                                select 
                                    passport,
                                    cast(date as date)	
                                from {schema_name}.{stg_table_name} stg
                                left join {schema_name}.{dwh_table_name} tgt
                                    on 1=1
                                    and stg.{stg_key} = tgt.{tgt_key}
                                where tgt.{tgt_key} is null""")
    else:
        print(i, ': Обновлений не обнаружено')
    # Переименовывание файла и перенос его в архив
    os.rename(i, f'archive/{i}.backup')
meta_data = next_date

# Обновление данных о дате последней загрузки 
print("""
2.4. Обновление таблицы """, meta_table)

cursor_dwh.execute(  f"""insert into {schema_name}.{meta_table}( 
                                schema_name, 
                                table_name, 
                                max_update_dt )
                            select 
	                            '{schema_name}',
	                            '{dwh_table_name}', 
	                            coalesce((select max(to_date('{meta_data}','YYYY-MM-DD')) from {schema_name}.{stg_table_name}), to_date('1900-01-01','YYYY-MM-DD'))
                            where not exists (select 1 from {schema_name}.{meta_table} where schema_name = '{schema_name}' and table_name = '{dwh_table_name}')""")
 
cursor_dwh.execute( f"""update {schema_name}.{meta_table}
                            set max_update_dt = coalesce((select max(to_date('{meta_data}','YYYY-MM-DD')) from {schema_name}.{stg_table_name}), max_update_dt)
                            where schema_name = '{schema_name}'
                            and table_name = '{dwh_table_name}' """)

print('Дата текущего обновления: ', meta_data)
####################################################################################
conn_dwh.commit()