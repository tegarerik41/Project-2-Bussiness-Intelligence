-- membuat schema tabel didalam data warehouse
create table fact_transaction (
	bill_id integer not null,
	user_id integer,
	line_item_amount float,
	bill_discount float,
	transaction_date date,
	description text,
	inventory_category varchar(225),
	color text,
	size varchar(225),
	zone_name varchar(225),
	store_name varchar(225),
	year integer);
	
create table dim_date (
	transaction_date date not null,
	transaction_date_day integer,
	transaction_date_month integer,
	transaction_date_year integer);

create table dim_year (
	transaction_date_year integer not null,
	transaction_date_year_information text);

create table dim_zone (
	zone_name varchar(225) not null,
	zone_information text);