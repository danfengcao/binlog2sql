binlog2sql
========================

从MySQL binlog解析出你要的SQL。根据不同选项，你可以得到原始SQL、回滚SQL、去除主键的INSERT SQL等。

用途
===========

* 数据快速回滚(闪回)
* 主从切换后数据不一致的修复
* 从binlog生成标准SQL，带来的衍生功能


项目状态
===
正常维护。应用于大众点评线上环境。线上环境的操作，请在对MySQL**相当熟悉**的同学指导下进行

* 已测试环境
    * Python 2.6, 2.7
    * MySQL 5.6


安装
==============

```
shell> git clone https://github.com/danfengcao/binlog2sql.git && cd binlog2sql
shell> pip install -r requirements.txt
```
git与pip的安装问题请自行搜索解决。

使用
=========

### MySQL server必须设置以下参数:

    [mysqld]
    server_id = 1
    log_bin = /var/log/mysql/mysql-bin.log
    max_binlog_size = 1G
    binlog_format = row
    binlog_row_image = full

### user需要的最小权限集合：

    select, super/replication client, replication slave
    
    建议授权
    GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 

**权限说明**

* select：需要读取server端information_schema.COLUMNS表，获取表结构的元信息，拼接成可视化的sql语句
* super/replication client：两个权限都可以，需要执行'SHOW MASTER STATUS', 获取server端的binlog列表
* replication slave：通过BINLOG_DUMP协议获取binlog内容的权限

###基本用法

**解析出标准SQL**

```bash
shell> python binlog2sql.py -h127.0.0.1 -P3306 -uadmin -p'admin' -dtest -t test3 test4 --start-file='mysql-bin.000002'

输出：
INSERT INTO `test`.`test3`(`addtime`, `data`, `id`) VALUES ('2016-12-10 13:03:38', 'english', 4); #start 570 end 736
UPDATE `test`.`test3` SET `addtime`='2016-12-10 12:00:00', `data`='中文', `id`=3 WHERE `addtime`='2016-12-10 13:03:22' AND `data`='中文' AND `id`=3 LIMIT 1; #start 763 end 954
DELETE FROM `test`.`test3` WHERE `addtime`='2016-12-10 13:03:38' AND `data`='english' AND `id`=4 LIMIT 1; #start 981 end 1147
```

**解析出回滚SQL**

```bash

shell> python binlog2sql.py --flashback -h127.0.0.1 -P3306 -uadmin -p'admin' -dtest -ttest3 --start-file='mysql-bin.000002' --start-position=763 --stop-position=1147

输出：
INSERT INTO `test`.`test3`(`addtime`, `data`, `id`) VALUES ('2016-12-10 13:03:38', 'english', 4); #start 981 end 1147
UPDATE `test`.`test3` SET `addtime`='2016-12-10 13:03:22', `data`='中文', `id`=3 WHERE `addtime`='2016-12-10 12:00:00' AND `data`='中文' AND `id`=3 LIMIT 1; #start 763 end 954
```
###选项
**mysql连接配置**

-h host; -P port; -u user; -p password

**解析模式**

--stop-never 持续同步binlog。可选。不加则同步至执行命令时最新的binlog位置。

-K, --no-primary-key 对INSERT语句去除主键。可选。

-B, --flashback 生成回滚语句，可解析大文件，不受内存限制，每打印一千行加一句SLEEP SELECT(1)。可选。与stop-never或no-primary-key不能同时添加。

**解析范围控制**

--start-file 起始解析文件。必须。

--start-position/--start-pos start-file的起始解析位置。可选。默认为start-file的起始位置。

--stop-file/--end-file 末尾解析文件。可选。默认为start-file同一个文件。若解析模式为stop-never，此选项失效。

--stop-position/--end-pos stop-file的末尾解析位置。可选。默认为stop-file的最末位置；若解析模式为stop-never，此选项失效。

--start-datetime 从哪个时间点的binlog开始解析，格式必须为datetime，如'2016-11-11 11:11:11'。可选。默认不过滤。

--stop-datetime 到哪个时间点的binlog停止解析，格式必须为datetime，如'2016-11-11 11:11:11'。可选。默认不过滤。

**对象过滤**

-d, --databases 只输出目标db的sql。可选。默认为空。

-t, --tables 只输出目标tables的sql。可选。默认为空。

###应用案例

#### **误删整张表数据，需要紧急回滚**

闪回详细介绍可参见example目录下《闪回原理与实战》[example/mysql-flashback-priciple-and-practice.md](./example/mysql-flashback-priciple-and-practice.md)

```bash
test库tbl表原有数据
mysql> select * from tbl;
+----+--------+---------------------+
| id | name   | addtime             |
+----+--------+---------------------+
|  1 | 小赵   | 2016-12-10 00:04:33 |
|  2 | 小钱   | 2016-12-10 00:04:48 |
|  3 | 小孙   | 2016-12-13 20:25:00 |
|  4 | 小李   | 2016-12-12 00:00:00 |
+----+--------+---------------------+
4 rows in set (0.00 sec)

mysql> delete from tbl;
Query OK, 4 rows affected (0.00 sec)

20:28时，tbl表误操作被清空
mysql> select * from tbl;
Empty set (0.00 sec)
```

**恢复数据步骤**：

1. 登录mysql，查看目前的binlog文件

	```bash
mysql> show master status;
+------------------+-----------+
| Log_name         | File_size |
+------------------+-----------+
| mysql-bin.000051 |       967 |
| mysql-bin.000052 |       965 |
+------------------+-----------+
```

2. 最新的binlog文件是mysql-bin.000052，我们再定位误操作SQL的binlog位置。误操作人只能知道大致的误操作时间，我们根据大致时间过滤数据。

	```bash
shell> python binlog2sql/binlog2sql.py -h127.0.0.1 -P3306 -uadmin -p'admin' -dtest -ttbl --start-file='mysql-bin.000052' --start-datetime='2016-12-13 20:25:00' --stop-datetime='2016-12-13 20:30:00'
输出：
INSERT INTO `test`.`tbl`(`addtime`, `id`, `name`) VALUES ('2016-12-13 20:26:00', 4, '小李'); #start 317 end 487 time 2016-12-13 20:26:26
UPDATE `test`.`tbl` SET `addtime`='2016-12-12 00:00:00', `id`=4, `name`='小李' WHERE `addtime`='2016-12-13 20:26:00' AND `id`=4 AND `name`='小李' LIMIT 1; #start 514 end 701 time 2016-12-13 20:27:07
DELETE FROM `test`.`tbl` WHERE `addtime`='2016-12-10 00:04:33' AND `id`=1 AND `name`='小赵' LIMIT 1; #start 728 end 938 time 2016-12-13 20:28:05
DELETE FROM `test`.`tbl` WHERE `addtime`='2016-12-10 00:04:48' AND `id`=2 AND `name`='小钱' LIMIT 1; #start 728 end 938 time 2016-12-13 20:28:05
DELETE FROM `test`.`tbl` WHERE `addtime`='2016-12-13 20:25:00' AND `id`=3 AND `name`='小孙' LIMIT 1; #start 728 end 938 time 2016-12-13 20:28:05
DELETE FROM `test`.`tbl` WHERE `addtime`='2016-12-12 00:00:00' AND `id`=4 AND `name`='小李' LIMIT 1; #start 728 end 938 time 2016-12-13 20:28:05
```

3. 我们得到了误操作sql的准确位置在728-938之间，再根据位置进一步过滤，使用flashback模式生成回滚sql，检查回滚sql是否正确(注：真实环境下，此步经常会进一步筛选出需要的sql。结合grep、编辑器等)

	```bash
shell> python binlog2sql/binlog2sql.py -h127.0.0.1 -P3306 -uadmin -p'admin' -dtest -ttbl --start-file='mysql-bin.000052' --start-position=3346 --stop-position=3556 -B > rollback.sql | cat
输出：
INSERT INTO `test`.`tbl`(`addtime`, `id`, `name`) VALUES ('2016-12-12 00:00:00', 4, '小李'); #start 728 end 938 time 2016-12-13 20:28:05
INSERT INTO `test`.`tbl`(`addtime`, `id`, `name`) VALUES ('2016-12-13 20:25:00', 3, '小孙'); #start 728 end 938 time 2016-12-13 20:28:05
INSERT INTO `test`.`tbl`(`addtime`, `id`, `name`) VALUES ('2016-12-10 00:04:48', 2, '小钱'); #start 728 end 938 time 2016-12-13 20:28:05
INSERT INTO `test`.`tbl`(`addtime`, `id`, `name`) VALUES ('2016-12-10 00:04:33', 1, '小赵'); #start 728 end 938 time 2016-12-13 20:28:05
```

4. 确认回滚sql正确，执行回滚语句。登录mysql确认，数据回滚成功。

	```bash
	shell> mysql -h127.0.0.1 -P3306 -uadmin -p'admin' < rollback.sql

	mysql> select * from tbl;
+----+--------+---------------------+
| id | name   | addtime             |
+----+--------+---------------------+
|  1 | 小赵   | 2016-12-10 00:04:33 |
|  2 | 小钱   | 2016-12-10 00:04:48 |
|  3 | 小孙   | 2016-12-13 20:25:00 |
|  4 | 小李   | 2016-12-12 00:00:00 |
+----+--------+---------------------+
```

###限制
* mysql server必须开启，离线模式下不能解析
* 参数 _binlog\_row\_image_ 必须为FULL，暂不支持MINIMAL

###优点（对比mysqlbinlog）

* 纯Python开发，安装与使用都很简单
* 自带flashback、no-primary-key解析模式，无需再装补丁
* flashback模式下，更适合闪回[实战](./example/mysql-flashback-priciple-and-practice.md)
* 解析为标准SQL，方便理解、调试
* 代码容易改造，可以支持更多个性化解析

###贡献者

* danfengcao 维护者 [https://github.com/danfengcao](https://github.com/danfengcao)
* 大众点评DBA团队 想法交流，使用体验 [dba_op@dianping.com](dba_op@dianping.com)
* 赵承勇 pymysqlreplication权限bug [https://github.com/imzcy1987](https://github.com/imzcy1987)
* 陈路炳 bug报告(字段值为空时的处理)，使用体验 [https://github.com/bingluchen](https://github.com/bingluchen)

###联系我
有任何问题，请与我联系。微信：danfeng053005 邮箱：[danfengcao.info@gmail.com](danfengcao.info@gmail.com)

欢迎提问题提需求，欢迎pull requests！


bin2sql
========================

Usage:

```bash
python bin2sql.py -h 127.0.0.1 --start-datetime='2017-04-05 21:44:54' --stop-datetime='2017-04-05 21:45:00' -f mysql-bin.000001
```

