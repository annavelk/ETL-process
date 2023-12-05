#!/usr/bin/python3
import psycopg2
import pandas as pd
from connect import cursor_src
from connect import cursor_dwh
from connect import conn_dwh
####################################################################################

# Определяю переменные для таблиц
meta_table = 'vean_meta_date'
schema_name = 'deaise'
infinity_date = "'9999-12-31','YYYY-MM-DD'"
default_date = "'1899-01-01','YYYY-MM-DD'"
source_schema_name = 'info'
source_table_name = 'accounts'
dwh_table_name = 'vean_dwh_dim_accounts_hist'
stg_table_name = 'vean_stg_accounts'
stg_key = 'account'
tgt_key = 'account_num'
update_dt_column = "update_dt"
stg_del = 'vean_stg_accounts_del'
stg_del_id = 'account'

print('''
6. ОБНОВЛЕНИЕ ТАБЛИЦ: ''', stg_table_name, ', ', dwh_table_name)

## deaise.vean_stg_transactions
print("""
6.1 Подготовка. Очистка """, stg_table_name, stg_del)
cursor_dwh.execute( f"DELETE FROM {schema_name}.{stg_table_name} " )
cursor_dwh.execute( f"DELETE FROM {schema_name}.{stg_del} " )

print('6.2 Загрузка данных из источника в ', stg_table_name)
# Определение времени последней загрузки
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

meta_date = str(cursor_dwh.fetchone()[0]).split(' ')[0]
print('Дата последнего обновления: ', meta_date)

# Выполнение SQL кода в базе данных с возвратом результата
cursor_src.execute( f""" SELECT 
                            account,
                            valid_to,
                            client,
                            create_dt,
                            update_dt  
                        FROM {source_schema_name}.{source_table_name}
                        where cast(coalesce(update_dt, create_dt) as timestamp(0)) > cast('{meta_date}' as timestamp(0))""" )

records = cursor_src.fetchall()
df = pd.DataFrame( records )

cursor_dwh.executemany( f"""INSERT INTO {schema_name}.{stg_table_name}(
                            account,
                            valid_to,
                            client,
                            create_dt,
                            update_dt ) 
                           VALUES( %s, %s, %s, %s, %s)""", df.values.tolist() )

row = len(df.index)
print('-------Извлечено ', row, ' строк')

if row > 0:
    print('6.3. Захват ключей полным срезом для ', stg_del)
    # Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
    cursor_src.execute( f"""select {stg_key} 
                            from {source_schema_name}.{source_table_name} """)

    records = cursor_src.fetchall()
    df = pd.DataFrame( records )

    cursor_dwh.executemany( f"""insert into {schema_name}.{stg_del}
                                ( {stg_del_id} )
                                VALUES( %s)""", df.values.tolist())
            
    print('6.4. Загрузка данных в ', dwh_table_name)
    ## Загрузка новых данных из stage
    cursor_dwh.execute( f""" insert into {schema_name}.{dwh_table_name} ( 
                                account_num,
                                valid_to,
                                client,
                                effective_from, 
                                effective_to, 
                                deleted_flg)
                            select 
                                stg.account,
                                stg.valid_to,
                                stg.client,
                                create_dt effective_from,
                                to_date({infinity_date}) effective_to,
                                'N' deleted_flg
                            from {schema_name}.{stg_table_name} stg
                            left join {schema_name}.{dwh_table_name} tgt
                                on 1=1
                                and stg.{stg_key} = tgt.{tgt_key}
                            where tgt.{tgt_key} is null""")

    print('6.5. Обновление данных в ', dwh_table_name)
    ## Обновление данных
    cursor_dwh.execute( f"""insert into {schema_name}.{dwh_table_name} ( 
                                account_num,
                                valid_to,
                                client,
                                effective_from, 
                                effective_to, 
                                deleted_flg )
                            select 
                                stg.account,
                                stg.valid_to,
                                stg.client,
                                update_dt effective_from,
                                to_date({infinity_date}) effective_to,
                                'N' deleted_flg
                            from {schema_name}.{stg_table_name} stg 
                            inner join {schema_name}.{dwh_table_name} tgt
                            on stg.{stg_key} = tgt.{tgt_key}
                            and tgt.effective_to = to_date({infinity_date})
                            where ( stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null) or ( stg.valid_to is not null and tgt.valid_to is null))
                            or ( stg.client <> tgt.client or ( stg.client is null and tgt.client is not null) or ( stg.client is not null and tgt.client is null))
                            or tgt.deleted_flg = 'Y' """)

    cursor_dwh.execute( f"""update {schema_name}.{dwh_table_name} tgt 
                            set effective_to = tmp.update_dt - interval '1 second'
                            from (
                                select 
                                    stg.account,
                                    stg.valid_to,
                                    stg.client,
                                    stg.update_dt
                                from {schema_name}.{stg_table_name} stg 
                                inner join {schema_name}.{dwh_table_name} tgt
                                on stg.{stg_key} = tgt.{tgt_key}
                                and tgt.effective_to = to_date({infinity_date})
                                where ( stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null) or ( stg.valid_to is not null and tgt.valid_to is null))
                                or ( stg.client <> tgt.client or ( stg.client is null and tgt.client is not null) or ( stg.client is not null and tgt.client is null))
                                or tgt.deleted_flg = 'Y') tmp
                            where tgt.{tgt_key} = tmp.{stg_key}
                            and tgt.effective_to = to_date({infinity_date})
                            and (( tmp.valid_to <> tgt.valid_to or ( tmp.valid_to is null and tgt.valid_to is not null) or ( tmp.valid_to is not null and tgt.valid_to is null))
                                or ( tmp.client <> tgt.client or ( tmp.client is null and tgt.client is not null) or ( tmp.client is not null and tgt.client is null))
                            or tgt.deleted_flg = 'Y') """)

    print('6.6. Обработка удалений данных в ', dwh_table_name)
    # Обработка удалений в приемнике.
    cursor_dwh.execute( f"""insert into {schema_name}.{dwh_table_name} ( 
                                account_num,
                                valid_to,
                                client,
                                effective_from, 
                                effective_to, 
                                deleted_flg )
                            select 
                                tgt.account_num,
                                tgt.valid_to,
                                tgt.client,
                                now() effective_from,
                                to_date({infinity_date}) effective_to,
                                'Y' deleted_flg
                            from {schema_name}.{dwh_table_name} tgt 
                            left join {schema_name}.{stg_del} stg
                            on stg.{stg_del_id} = tgt.{tgt_key}
                            where stg.{stg_del_id} is null
                            and tgt.effective_to = to_date({infinity_date})
                            and tgt.deleted_flg = 'N' """)

    cursor_dwh.execute( f"""update {schema_name}.{dwh_table_name} tgt 
                            set effective_to = now() - interval '1 second'
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

    # Обновление данных о дате последней загрузки 
    print("""
    6.7. Обновление таблицы """, meta_table)

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
                                set max_update_dt = coalesce((select max(coalesce({update_dt_column}, create_dt)) from {schema_name}.{stg_table_name}), max_update_dt)
                                where schema_name = '{schema_name}'
                                and table_name = '{dwh_table_name}' """)
else:
    print(source_schema_name,'.',source_table_name,': обновлений не обнаружено')                            
####################################################################################
conn_dwh.commit()