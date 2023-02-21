set hivevar:version=3.1.0;
create database if not exists table_test;
use table_test;

drop table if exists t1;
create table t1(c1 string) stored as textfile;
load data local inpath '/etc/profile' overwrite into table t1;

desc t1;
desc formatted t1;
desc extended t1;

alter table t1 add columns (c2 string);
alter table t1 CHANGE COLUMN c2 c2 string COMMENT 'c2 comment'  first;
desc t1;
--c2 在第1列的位置上。

CREATE EXTERNAL TABLE IF NOT EXISTS stocks (
c_exchange string,
symbol STRING,
ymd STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
volume INT,
price_adj_close FLOAT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/data/stocks';
drop table stocks;

CREATE temporary TABLE IF NOT EXISTS t_tempory_stocks (
c_exchange string,
symbol STRING,
ymd STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
volume INT,
price_adj_close FLOAT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/data/t_tempory_stocks';

drop table if exists t2;
create table t2 as select count(1)cnt, c1 from t1 group by c1;
insert overwrite table t2 select count(1)cnt, c1 from t1 group by c1;
insert into t2 select count(1)cnt, c1 from t1 group by c1;

drop table t2;

-- partitioned table
create table t3(c1 string) partitioned by (dt string);
insert overwrite table t3 partition(dt=20230101) select distinct(c1) from t1;

-- dynamic partition
insert overwrite table t3  select c1,length(c1) from t1 group by c1;
SHOW PARTITIONS t3;
drop table t3;

create view v_t3 as select c1 from t1 where c1 <>4;
select count(distinct c1) from v_t3;
drop view v_t3;



-- complex table
drop table if exists complex_table;
create table complex_table(
    c_int int,
    c_array array<string>,
    c_map map<string, string>,
    c_struct struct<name:string,age:int>
)
row format delimited
FIELDS TERMINATED BY '\t'
collection items terminated by ','
map keys terminated by ':'
lines terminated by '\n'
stored as textfile;

insert into complex_table values(
  1, 
  array("array_value1","array_value2"), 
  str_to_map("key1:value1,key2:value2"),
  named_struct("name","Alice", "age",18)
);

select c_array[0], c_array[1], c_array[2] from complex_table ;
select c_struct.name, c_struct.age from complex_table;
drop table complex_table;

create table union_testnew(
  foo uniontype<int, double, string, array<string>, map<string, string>>
)
row format delimited
collection items terminated by ','
map keys terminated by ':'
lines terminated by '\n'
stored as textfile;
drop table union_testnew;

-- function 测试点
-- 1. session 中的 jar 文件是否能自动删除。

add jar /opt/bmr/hive/lib/hive-exec-${hivevar:version}.jar;
create function baidu_length as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFLength';
select c2,baidu_length(c2) from t1 group by c2;
drop function baidu_length;

! hadoop fs -put /opt/bmr/hive/lib/hive-exec-${hivevar:version}.jar /tmp/hive-exec-${hivevar:version}.jar;
create function  table_test.length_u as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFLength'
using jar '${hiveconf:fs.defaultFS}/tmp/hive-exec-${hivevar:version}.jar';
select table_test.length_u("abc");
drop function table_test.length_u;

-- analyze table
analyze table t1 compute statistics;
analyze table t1 compute statistics for columns;
drop table t1;

drop database table_test;