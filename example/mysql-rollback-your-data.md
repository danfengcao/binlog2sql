MySQL误操作后如何快速回滚
========================

基本上每个跟数据库打交道的程序员（当然也可能是你同事）都会碰一个问题，MySQL误操作后如何快速回滚？比如，delete一张表，忘加限制条件，整张表没了。假如这还是线上环境核心业务数据，那这事就闹大了。误操作后，能快速回滚数据是非常重要的。

binlog2sql快速回滚
===
首先，确认你的MySQL server开启了binlog，设置了以下参数:

	[mysqld]
	server-id = 1
	log_bin = /var/log/mysql/mysql-bin.log
	max_binlog_size = 1000M
	binlog-format = row
如果没有开启binlog，也没有预先生成回滚SQL，那真的无法快速回滚了。对存放重要业务数据的MySQL，强烈建议开启binlog。

随后，安装开源工具[binlog2sql](https://github.com/danfengcao/binlog2sql)。binlog2sql是一款简单易用的binlog解析工具，其中一个功能就是生成回滚SQL。

```
git clone https://github.com/danfengcao/binlog2sql.git
pip install -r requirements.txt
```

然后，我们就可以生成回滚SQL了。

**背景**：误删了test库f表整张表的数据，需要紧急回滚。

```bash
原有数据
mysql> select * from f;
+-----+-----+---------------------+
| uid | did | updateTime          |
+-----+-----+---------------------+
|   1 |  18 | 2016-12-06 12:28:18 |
|   2 |  19 | 2016-12-06 12:55:56 |
|   3 |  20 | 2016-12-07 14:00:58 |
|   4 |  21 | 2016-12-07 14:01:00 |
+-----+-----+---------------------+

误操作
mysql> delete from f;
Query OK, 4 rows affected (0.00 sec)

f表被清空
mysql> select * from f;
Empty set (0.00 sec)
```

**回滚步骤**：

1. 登录mysql，查看目前的binlog文件

	```bash
	mysql> show master logs;
	+------------------+-----------+
	| Log_name         | File_size |
	+------------------+-----------+
	| mysql-bin.000001 |  12262268 |
	| mysql-bin.000002 |    132776 |
	+------------------+-----------+
	```

2. 最新的binlog文件是mysql-bin.000002，我们再定位误操作SQL的binlog位置

	```bash
	$ python binlog2sql/binlog2sql.py -h127.0.0.1 -P3306 -uadmin -p'admin' -dtest -t f --start-file='mysql-bin.000002'

	输出：
	DELETE FROM `test`.`f` WHERE `did`=18 AND `updateTime`='2016-12-06 12:28:18' AND `uid`=1 LIMIT 1; #start 4 end 314
	DELETE FROM `test`.`f` WHERE `did`=19 AND `updateTime`='2016-12-06 12:55:56' AND `uid`=2 LIMIT 1; #start 4 end 314
	DELETE FROM `test`.`f` WHERE `did`=20 AND `updateTime`='2016-12-07 14:00:58' AND `uid`=3 LIMIT 1; #start 4 end 314
	DELETE FROM `test`.`f` WHERE `did`=21 AND `updateTime`='2016-12-07 14:01:00' AND `uid`=4 LIMIT 1; #start 4 end 314
	```
2. 生成回滚sql，并检查回滚sql是否正确

	```bash
	$ python binlog2sql/binlog2sql.py -h127.0.0.1 -P3306 -uadmin -p'admin' -dtest -t f --start-file='mysql-bin.000002' --start-pos=4 --end-pos=314 -B

	输出：
	INSERT INTO `test`.`f`(`did`, `updateTime`, `uid`) VALUES (21, '2016-12-07 14:01:00', 4); #start 4 end 314
	INSERT INTO `test`.`f`(`did`, `updateTime`, `uid`) VALUES (20, '2016-12-07 14:00:58', 3); #start 4 end 314
INSERT INTO `test`.`f`(`did`, `updateTime`, `uid`) VALUES (19, '2016-12-06 12:55:56', 2); #start 4 end 314
	INSERT INTO `test`.`f`(`did`, `updateTime`, `uid`) VALUES (18, '2016-12-06 12:28:18', 1); #start 4 end 314
	```
3. 确认回滚sql正确，执行回滚语句。登录mysql，数据回滚成功。

	```bash
	$ python binlog2sql.py -h127.0.0.1 -P3306 -uadmin -p'admin' -dtest -t f --start-file='mysql-bin.000002' --start-pos=4 --end-pos=314 -B | mysql -h127.0.0.1 -P3306 -uadmin -p'admin'

	mysql> select * from f;
+-----+-----+---------------------+
| uid | did | updateTime          |
+-----+-----+---------------------+
|   1 |  18 | 2016-12-06 12:28:18 |
|   2 |  19 | 2016-12-06 12:55:56 |
|   3 |  20 | 2016-12-07 14:00:58 |
|   4 |  21 | 2016-12-07 14:01:00 |
+-----+-----+---------------------+
	
	```

至此，不用再担心被炒鱿鱼了。

常见问题
===
* 有人会问，我DDL误操作了怎么快速回滚？比如drop了一张大表。

 > 很难做到。因为即使在在row模式下，DDL操作也不会把每行数据的变化记录到binlog，所以DDL无法通过binlog回滚。实现DDL回滚，必须要在执行DDL前先备份老数据。确实有人通过修改mysql server源码实现了DDL的快速回滚，我找到阿里的xiaobin lin提交了一个patch。但据我所知，国内很少有互联网公司应用了这个特性。原因的话，我认为最主要还是懒的去折腾，没必要搞这个低频功能，次要原因是会增加一些额外存储。
 > 
 所以，DDL误操作的话一般只能通过备份来恢复。如果公司连备份也不能用了，那真的建议去买张飞机票了。干啥？跑呗

* mysql除了[binlog2sql](https://github.com/danfengcao/binlog2sql)，是否还有其他回滚工具？

	>当然有。阿里彭立勋对mysqlbinlog增加了flashback的特性，这应该是mysql最早有的flashback功能，彭解决的是DML的回滚，并说明了利用binlog进行DML闪回的设计思路。DDL回滚特性也是由阿里团队提出并实现的。这两个功能是有创新精神的，此后出现的闪回工具基本都是对上面两者的模仿。另外，去哪儿开源的Inception是一套MySQL自动化运维工具，这个就比较重了，支持DML回滚，还不是从binlog回滚的，是从备份回滚的，也支持DDL回滚表结构，数据是回滚不了滴~

本文如有任何错误，烦请告知。如有mysql回滚相关的优秀工具优秀文章遗漏，更烦请告知

我的邮箱 danfengcao.info@gmail.com


参考资料
==============
[1] 彭立勋, [MySQL下实现闪回的设计思路](http://www.penglixun.com/tech/database/mysql_flashback_feature.html)

[2] Lixun Peng, [Provide the flashback feature by binlog](https://bugs.mysql.com/bug.php?id=65178)

[3] 丁奇, [MySQL闪回方案讨论及实现](http://dinglin.iteye.com/blog/1539167)

[4] xiaobin lin, [flashback from binlog for MySQL](https://bugs.mysql.com/bug.php?id=65861)

[5] 王竹峰, [去哪儿inception](https://github.com/mysql-inception/inception)

[6] danfengcao, [binlog2sql: Parse MySQL binlog to SQL you want](https://github.com/danfengcao/binlog2sql)

