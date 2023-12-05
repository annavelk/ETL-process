#!/usr/bin/python3
import psycopg2
import pandas as pd
from connect import cursor_src
from connect import cursor_dwh
from connect import conn_dwh
####################################################################################
print ("""
7. Построение витрины отчетности по мошенническим операциям vean_rep_fraud
""")

cursor_dwh.execute( f""" with cl as (select
                                        tr.trans_id,
                                        tr.trans_date,
                                        tr.card_num,
                                        tr.oper_type,
                                        tr.amt,
                                        tr.oper_result,
                                        tr.terminal,
                                        c.account_num,
                                        acc.valid_to,
                                        acc.client,
                                        concat(cli.last_name, ' ', cli.first_name, ' ', cli.patronymic) as fio,
                                        cli.date_of_birth,
                                        cli.passport_num,
                                        cli.passport_valid_to,
                                        cli.phone,
                                        bl.passport_num as pass_bl,
                                        coalesce(bl.entry_dt, to_date('9999-12-31','YYYY-MM-DD')) entry_dt,
                                        ter.terminal_id,
                                        ter.terminal_type,
                                        ter.terminal_city,
                                        ter.terminal_address,
                                        ter.effective_from, 
                                        ter.effective_to, 
                                        ter.deleted_flg
                                        from vean_dwh_fact_transactions tr
                                        left join vean_dwh_dim_terminals_hist ter
                                        on tr.terminal = ter.terminal_id
                                        and tr.trans_date > ter.effective_from 
                                        and tr.trans_date < ter.effective_to and ter.deleted_flg = 'N'
                                        left join vean_dwh_dim_cards_hist c
                                        on trim(tr.card_num) = trim(c.card_num)
                                        left join vean_dwh_dim_accounts_hist acc 
                                        on c.account_num = acc.account_num
                                        left join vean_dwh_dim_clients_hist cli 
                                        on acc.client = cli.client_id
                                        left join vean_dwh_fact_passport_blacklist bl
                                        on trim(cli.passport_num) = trim(bl.passport_num)
                                    ), lg as (select
                                    cl.card_num, cl.trans_date, cl.terminal_city, cl.fio, cl.passport_num, cl. phone, cl.trans_id, cl.oper_type, cl.oper_result, cl.amt,
                                    (lag(cl.terminal_city) over (partition by cl.card_num order by cl.trans_date)) as lag_city,
                                    cl.trans_date - (lag(cl.trans_date) over (partition by cl.card_num order by cl.trans_date)) as lag_pr_date,
                                    lag (cl.oper_result) OVER (PARTITION BY cl.card_num ORDER BY cl.trans_date) res_1, 
                                    lag(cl.oper_result,2) OVER (PARTITION BY cl.card_num ORDER BY cl.trans_date) res_2, 
                                    lag(cl.oper_result,3) OVER (PARTITION BY cl.card_num ORDER BY cl.trans_date) res_3, 
                                    lag (cl.amt) OVER (PARTITION BY cl.card_num ORDER BY cl.trans_date) amt_1, 
                                    lag(cl.amt,2) OVER (PARTITION BY cl.card_num ORDER BY cl.trans_date) amt_2, 
                                    lag(cl.amt,3) OVER (PARTITION BY cl.card_num ORDER BY cl.trans_date) amt_3, 
                                    lag(cl.trans_date,3) OVER (PARTITION BY cl.card_num ORDER BY cl.trans_date) dt 
                                    from cl 
                                    )
                        insert into deaise.vean_rep_fraud ( 
                            event_dt,
                            passport,
                            fio,
                            phone,
                            event_type,
                            report_dt)
                        select 
                            cl1.trans_date as event_dt,
                            cl1.passport_num as passport,
                            cl1.fio,
                            cl1.phone as phone,
                            1 as event_type,
                            now() as report_dt
                            from cl as cl1
                            where (cl1.passport_valid_to < cl1.trans_date) or ((cl1.pass_bl is not null) and (cl1.entry_dt <= cl1.trans_date))
                        union ALL	
                        select 
                            cl.trans_date as event_dt,
                            cl.passport_num as passport,
                            cl.fio,
                            cl.phone as phone,
                            2 as event_type,
                            now() as report_dt
                            from cl
                            where (cl.trans_date >= cl.valid_to)
                        union ALL		
                        select
                            lg.trans_date as event_dt,
                            lg.passport_num as passport,
                            lg.fio,
                            lg.phone as phone,
                            3 as event_type,
                            now() as report_dt
                        from lg
                        where lg.terminal_city <> lg.lag_city
                        and lg.lag_pr_date <= interval '1 hour'
                        union ALL
                        select
                            lg.trans_date as event_dt,
                            lg.passport_num as passport,
                            lg.fio,
                            lg.phone as phone,
                            4 as event_type,
                            now() as report_dt
                        from lg
                        where (lg.oper_result = 'SUCCESS'  
                            and (lg.res_1 = 'REJECT' and lg.res_2 = 'REJECT' and lg.res_3 = 'REJECT')
                            and (lg.amt < lg.amt_1 and lg.amt_1 < lg.amt_2 and lg.amt_2 < lg.amt_3 )
                            and (lg.trans_date - lg.dt) <=interval '20 minute')
                            and (oper_type = 'PAYMENT' or oper_type = 'WITHDRAW')""")
                            
####################################################################################
conn_dwh.commit()