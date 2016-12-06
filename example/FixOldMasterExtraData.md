MySQL主从切换后如何进行数据修复
========================

目前的MySQL高可用方案中，主从切换后数据不一致是一个常见问题。以业界常用的MHA为例，当出现网络故障或硬件故障，无法通过SSH访问到老master，则有可能出现数据丢失。本文对主从切换后的数据不一致提供了一种解决方案。

### 主从切换后的状态

old master(10.1.1.1:3306)有一部分binlog(unreplicated binlog)未同步到new master(10.1.1.2:3306)，切换完成后new master开始有新数据进入。

![](./master-slave-inconsistency.jpg)

### 笨办法1
直接将old master连接new master，开启同步。此时一般会报错duplicate entry for key 'primary'。此时可选择， 

1. 一直跳过错误直到连接正常
 
	缺点：主从数据不一致，留下安全隐患

2. 根据报错，逐个修old master数据

	缺点：unreplicated binlog丢失，手工修复起来相当繁琐

### 笨办法2
对old master进行flashback至mysql-bin.00040 120，再开启同步。同步正常。 
 
	缺点：unreplicated binlog丢失

### 新思路
1. 提取old master未同步的数据，并对其中的insert去除主键；
2. 对old master进行flashback至mysql-bin.00040 120，开启同步；
3. 在new master重新导入改造后的sql；

```bash；
python binlog2sql.py --popPk --host='10.1.1.1' -P3306 -uadmin -p'admin' --start-file='mysql-bin.000002' --start-pos=1240 > oldMaster.sql

python binlog2sql.py --flashback --host='10.1.1.1' -P3306 -uadmin -p'admin' --start-file='mysql-bin.000002' --start-pos=1240 | mysql -h10.1.1.1 -P3306 -uadmin -p'admin'

mysql -h10.1.1.2 -P3306 -uadmin -p'admin' < oldMaster.sql
```

**优点**

数据丢失最少，操作简单快捷。

**注意**

修完数据后，表数据可能会与业务逻辑预想的有区别。需要与业务方沟通后再做修复操作。






