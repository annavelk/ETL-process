create table if not exists deaise.vean_stg_transactions (
	transaction_id varchar(12),
	transaction_date varchar(20),
	amount varchar(17),
	card_num varchar(20),
	oper_type varchar(10),
	oper_result varchar(7),
	terminal varchar(5)
);

create table if not exists deaise.vean_stg_terminals (
	terminal_id varchar(5),
	terminal_type varchar(3),
	terminal_city varchar(50),
	terminal_address varchar(100),
	update_dt varchar(10)
);

create table deaise.vean_stg_terminals_del( 
	terminal_id varchar(5)
);

create table if not exists deaise.vean_stg_blacklist (
	date varchar(20),
	passport varchar(15)
);

create table if not exists deaise.vean_stg_cards (
	card_num varchar(20) NULL,
	account varchar(20) NULL,
	create_dt timestamp(0) NULL,
	update_dt timestamp(0) NULL
);

create table deaise.vean_stg_cards_del( 
	card_num varchar(20)
);

create table if not exists deaise.vean_stg_accounts (
	account varchar(20) NULL,
	valid_to date NULL,
	client varchar(10) NULL,
	create_dt timestamp(0) NULL,
	update_dt timestamp(0) NULL
);

create table deaise.vean_stg_accounts_del( 
	account varchar(20)
);

create table if not exists deaise.vean_stg_clients (
	client_id varchar(10) NULL,
	last_name varchar(20) NULL,
	first_name varchar(20) NULL,
	patronymic varchar(20) NULL,
	date_of_birth date NULL,
	passport_num varchar(15) NULL,
	passport_valid_to date NULL,
	phone varchar(16) NULL,
	create_dt timestamp(0) NULL,
	update_dt timestamp(0) NULL
);

create table deaise.vean_stg_clients_del( 
	client_id varchar(10)
);

create table if not exists deaise.vean_dwh_fact_transactions (
	trans_id varchar(12),
	trans_date timestamp(0),
	card_num varchar(20),
	oper_type varchar(10),
	amt decimal(15,2),
	oper_result varchar(7),
	terminal varchar(5)
);

create table if not exists deaise.vean_dwh_fact_passport_blacklist (
	passport_num varchar(15),
	entry_dt date
);

create table if not exists deaise.vean_dwh_dim_terminals_hist (
	terminal_id varchar(5),
	terminal_type varchar(3),
	terminal_city varchar(50),
	terminal_address varchar(100),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg varchar(1)
);

create table if not exists deaise.vean_dwh_dim_cards_hist (
	card_num varchar(20),
	account_num varchar(20),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg varchar(1)
);

create table if not exists deaise.vean_dwh_dim_accounts_hist (
	account_num varchar(20),
	valid_to date,
	client varchar(65),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg varchar(1)
);

create table if not exists deaise.vean_dwh_dim_clients_hist (
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg varchar(1)
);

create table if not exists deaise.vean_rep_fraud (
	event_dt timestamp(0),
	passport varchar(15),
	fio varchar(65),
	phone varchar(16),
	event_type numeric,
	report_dt timestamp(0)
);

create table deaise.vean_meta_date(
    schema_name varchar(30),
    table_name varchar(50),
    max_update_dt timestamp(0)
);

insert into deaise.vean_meta_date( schema_name, table_name, max_update_dt )
values( 'deaise','vean_dwh_dim_terminals_hist', to_timestamp('1900-01-01','YYYY-MM-DD')),
	( 'deaise','vean_dwh_dim_cards_hist', to_timestamp('1899-01-01','YYYY-MM-DD')),
	( 'deaise','vean_dwh_dim_accounts_hist', to_timestamp('1899-01-01','YYYY-MM-DD')),
	( 'deaise','vean_dwh_fact_transactions', to_timestamp('1900-01-01','YYYY-MM-DD')),
	( 'deaise','vean_dwh_fact_passport_blacklist', to_timestamp('1900-01-01','YYYY-MM-DD')),
	( 'deaise','vean_dwh_dim_clients_hist', to_timestamp('1899-01-01','YYYY-MM-DD'));

