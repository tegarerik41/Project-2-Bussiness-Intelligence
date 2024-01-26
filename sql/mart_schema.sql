-- membuat data mart
create table mart_metrik (
	tahun integer,
	customer integer,
	transaksi integer,
	penjualan float,
	barang float,
	diskon float);

create table mart_zone (
	tahun integer,
	zone_name varchar(255),
	penjualan float);