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
infinity_date = "'9999-12-31','YYYY-MM-DD'"
default_date = "'1900-01-01','YYYY-MM-DD'"

dwh_table_name = 'vean_dwh_dim_terminals_hist'
stg_table_name = 'vean_stg_terminals'
stg_key = 'terminal_id'
tgt_key = 'terminal_id'
update_dt_column = "to_date(update_dt, 'YYYY-MM-DD')"
stg_del = 'vean_stg_terminals_del'
stg_del_id = 'terminal_id'

print('''
3. ОБНОВЛЕНИЕ ТАБЛИЦ: ''', stg_table_name, ', ', dwh_table_name)
# Забираю файлы по маске названия
files_name = glob.glob('terminals_*.xlsx')
# Сортирую файлы, определяю количество файлов в каталоге
files_sort = sorted(files_name, key=lambda a: a[14:18] + a[12:14] + a[10:12])
print('Текущий каталог содержит файлов - ', len(files_name), ': ', files_sort)
# Определяю дату последнего изменения и преобразую в строку.
cursor_dwh.execute(f"""SELECT 
                        coalesce( 
                            (select max_update_dt 
                            from {schema_name}.{meta_table} 
                            where schema_name = '{schema_name}' 
                            and table_name = '{dwh_table_name}'),
	                        to_timestamp({default_date}) ) 
                        from {schema_name}.{meta_table} 
                        where schema_name = '{schema_name}' 
                        and table_name = '{dwh_table_name}' """)

meta_data = str(cursor_dwh.fetchone()[0]).split(' ')[0]
print('Дата последнего обновления: ', meta_data)
# Временная переменная для определения большей даты при загрузке из нескольких файлов
next_date = meta_data
# Циклом прохожусь по файлам, записывая новые данные в stage, определяю наибольшую дату
for i in files_sort:  
    file_date = (i.split('_')[1]).split('.')[0]
    file_date = file_date[4:8:1] + '-' + file_date[2:4:1] + '-' + file_date[0:2:1]
    if file_date > meta_data:
        df = pd.read_excel( i, sheet_name='terminals', header=0, index_col=None )
        df['update_dt'] = file_date
        ## deaise.vean_stg_transactions
        print("""
        3.1. Очистка """, stg_table_name, stg_del)
        cursor_dwh.execute( f"DELETE FROM {schema_name}.{stg_table_name} " )
        cursor_dwh.execute( f"DELETE FROM {schema_name}.{stg_del} " )
        print('3.2. Загрузка данных из файлов в ', stg_table_name)
        cursor_dwh.executemany( f"""INSERT INTO {schema_name}.{stg_table_name}(
                                        terminal_id,
                                        terminal_type,
                                        terminal_city,
                                        terminal_address,
                                        update_dt) 
                            VALUES( %s, %s, %s, %s, %s)""", df.values.tolist() )
        row = len(df.index)
        print(i,': Поступило из файла ', row, ' строк')
        if file_date > next_date:
            next_date = file_date
        
        print('3.3. Захват ключей полным срезом для ', stg_del)
        # Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
        cursor_dwh.execute( f"""insert into {schema_name}.{stg_del} ( {stg_del_id} )
                                    select {stg_key} 
                                    from {schema_name}.{stg_table_name} """)
        
        print('3.4. Загрузка данных в ', dwh_table_name)
        ## Загрузка новых данных из stage

        cursor_dwh.execute( f""" insert into {schema_name}.{dwh_table_name} ( 
                                    terminal_id,
                                    terminal_type,
                                    terminal_city,
                                    terminal_address,
                                    effective_from, 
                                    effective_to, 
                                    deleted_flg
                                )
                                select 
                                    stg.terminal_id,
                                    stg.terminal_type,
                                    stg.terminal_city,
                                    stg.terminal_address,
                                    to_date(update_dt, 'YYYY-MM-DD') effective_from,
                                    to_date({infinity_date}) effective_to,
                                    'N' deleted_flg
                                from {schema_name}.{stg_table_name} stg
                                left join {schema_name}.{dwh_table_name} tgt
                                    on 1=1
                                    and stg.{stg_key} = tgt.{tgt_key}
                                where tgt.{tgt_key} is null""")

        print('3.5. Обновление данных в ', dwh_table_name)
        ## Обновление данных
        cursor_dwh.execute( f"""insert into {schema_name}.{dwh_table_name} ( 
                                    terminal_id,
                                    terminal_type,
                                    terminal_city,
                                    terminal_address,
                                    effective_from, 
                                    effective_to, 
                                    deleted_flg )
                                select 
                                    stg.terminal_id,
                                    stg.terminal_type,
                                    stg.terminal_city,
                                    stg.terminal_address,
                                    to_date(update_dt, 'YYYY-MM-DD'),
                                    to_date({infinity_date}) effective_to,
                                    'N' deleted_flg
                                from {schema_name}.{stg_table_name} stg 
                                inner join {schema_name}.{dwh_table_name} tgt
                                on stg.{stg_key} = tgt.{tgt_key}
                                and tgt.effective_to = to_date({infinity_date})
                                where ( stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null) or ( stg.terminal_type is not null and tgt.terminal_type is null))
                                or ( stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null) or ( stg.terminal_city is not null and tgt.terminal_city is null))
                                or ( stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null) or ( stg.terminal_address is not null and tgt.terminal_address is null))
                                or tgt.deleted_flg = 'Y' """)

        cursor_dwh.execute( f"""update {schema_name}.{dwh_table_name} tgt 
                                set effective_to = tmp.update_date - interval '1 second'
                                from (
                                    select 
                                        stg.terminal_id,
                                        stg.terminal_type,
                                        stg.terminal_city,
                                        stg.terminal_address,
                                        to_date(update_dt, 'YYYY-MM-DD') update_date
                                    from {schema_name}.{stg_table_name} stg 
                                    inner join {schema_name}.{dwh_table_name} tgt
                                    on stg.{stg_key} = tgt.{tgt_key}
                                    and tgt.effective_to = to_date({infinity_date})
                                    where ( stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null) or ( stg.terminal_type is not null and tgt.terminal_type is null))
                                    or ( stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null) or ( stg.terminal_city is not null and tgt.terminal_city is null))
                                    or ( stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null) or ( stg.terminal_address is not null and tgt.terminal_address is null))
                                    or tgt.deleted_flg = 'Y') tmp
                                where tgt.{tgt_key} = tmp.{stg_key}
                                and tgt.effective_to = to_date({infinity_date})
                                and ( ( tmp.terminal_type <> tgt.terminal_type or ( tmp.terminal_type is null and tgt.terminal_type is not null) or ( tmp.terminal_type is not null and tgt.terminal_type is null) )
                                or ( tmp.terminal_city <> tgt.terminal_city or ( tmp.terminal_city is null and tgt.terminal_city is not null) or ( tmp.terminal_city is not null and tgt.terminal_city is null) )
                                or ( tmp.terminal_address <> tgt.terminal_address or ( tmp.terminal_address is null and tgt.terminal_address is not null) or ( tmp.terminal_address is not null and tgt.terminal_address is null) ) 
                                or tgt.deleted_flg = 'Y') ; """)

        print('3.6. Обработка удалений данных в ', dwh_table_name)
        # Обработка удалений в приемнике.
        cursor_dwh.execute( f"""insert into {schema_name}.{dwh_table_name} ( 
                                    terminal_id,
                                    terminal_type,
                                    terminal_city,
                                    terminal_address,
                                    effective_from, 
                                    effective_to, 
                                    deleted_flg )
                                select 
                                    tgt.terminal_id,
                                    tgt.terminal_type,
                                    tgt.terminal_city,
                                    tgt.terminal_address,
                                    to_date('{file_date}', 'YYYY-MM-DD') effective_from,
                                    to_date({infinity_date}) effective_to,
                                    'Y' deleted_flg
                                from {schema_name}.{dwh_table_name} tgt 
                                left join {schema_name}.{stg_del} stg
                                on stg.{stg_del_id} = tgt.{tgt_key}
                                where stg.{stg_del_id} is null
                                and tgt.effective_to = to_date({infinity_date})
                                and tgt.deleted_flg = 'N' """)

        cursor_dwh.execute( f"""update {schema_name}.{dwh_table_name} tgt 
                                set effective_to = to_date('{file_date}', 'YYYY-MM-DD') - interval '1 second'
                                where tgt.{tgt_key} in (
                                    select 
                                        tgt.{tgt_key}
                                    from {schema_name}.{dwh_table_name} tgt 
                                    left join {schema_name}.{stg_del} stg
                                    on stg.{stg_del_id} = tgt.{tgt_key}
                                    where stg.{stg_del_id} is null
                                    and tgt.effective_to = to_date({infinity_date})
                                    and deleted_flg = 'N')
                                and tgt.effective_to = to_date({infinity_date})
                                and deleted_flg = 'N' """)
    else:
        print(i, ': Обновлений не обнаружено')
    # Переименовывание файла и перенос его в архив
    os.rename(i, f'archive/{i}.backup')
meta_data = next_date

# Обновление данных о дате последней загрузки 
print("""
3.7. Обновление таблицы """, meta_table)

cursor_dwh.execute(  f"""insert into {schema_name}.{meta_table}( 
                                schema_name, 
                                table_name, 
                                max_update_dt )
                            select 
	                            '{schema_name}',
	                            '{dwh_table_name}', 
	                            coalesce((select max({update_dt_column}) from {schema_name}.{stg_table_name}), to_date({default_date}))
                            where not exists (select 1 from {schema_name}.{meta_table} where schema_name = '{schema_name}' and table_name = '{dwh_table_name}')""")
 
cursor_dwh.execute( f"""update {schema_name}.{meta_table}
                            set max_update_dt = coalesce((select max({update_dt_column}) from {schema_name}.{stg_table_name}), max_update_dt)
                            where schema_name = '{schema_name}'
                            and table_name = '{dwh_table_name}' """)
                            
print('Дата текущего обновления: ', meta_data)

####################################################################################
conn_dwh.commit()