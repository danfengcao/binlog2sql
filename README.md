# binlog read
forked from [danfengcao/binlog2sq](https://github.com/danfengcao/binlog2sql)

REF: https://www.percona.com/blog/2020/07/09/binlog2sql-binlog-to-raw-sql-conversion-and-point-in-time-recovery/

other refs:
* https://dev.mysql.com/doc/refman/5.7/en/mysqlbinlog.html
* https://github.com/meetup/chapstick-rds-upgrade-to-mysql57

# 
## Prerequisites
```
pyenv local 3.10.3
python -m venv env
source env/bin/activate
```
## get sql for a binlog

```
# terminal 1: find binlog name
## look for binlogs you might want to download
## head to bastion
ssh -L 3306:db.int.meetup.com:3306 prod_bastion
## log into db
mysql -u meetup -p -h db.int.meetup.com chapstick
## show binary logs (most recent logs shown last)
mysql> SHOW BINARY LOGS;
## e.g
...
| mysql-bin-changelog.484262 | 134223333 |
| mysql-bin-changelog.484263 | 134217869 |
| mysql-bin-changelog.484264 | 134235871 |
| mysql-bin-changelog.484265 |  97875800 |
+----------------------------+-----------+
748 rows in set (0.08 sec)
### 
# terminal 2: read binlog and translate to SQL file
BIN_LOG_NAME=mysql-bin-changelog.484263
./binlog2sql/binlog2sql.py --only-dml -h db.int.meetup.com -d chapstick -umeetup -p --start-file ${BIN_LOG_NAME} > work/PROD.${BIN_LOG_NAME}.sql
```

# Set Notes
This was my setup. It may not apply to you.
```
# for local testing
cp work/my.cnf /usr/local/var/mysql
/usr/local/opt/mysql/bin/mysqld_safe --datadir=/usr/local/var/mysql
```