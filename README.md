# hive-metastore-api-test

## 1.用 testcase 执行测试
### 1.1 创建测试的 resources 目录
```bash
mkdir -p src/test/resources
```
### 1.2 拷贝 hadoop 和 hive 的配置文件
拷贝 hadoop 和 hive 的配置文件到 src/test/resources。
### 1.3 执行测试
```bash
mvn clean package
```
如果没有异常，则说明数据库，表和分区测试成功。

## 2. 到服务器上执行测试
### 2.1 编译
```
mvn clean package  -DskipTests
```
### 2.2 拷贝 target/hive-metastore-api-test-0.1.0-tests.jar 到服务器
```bash
scp target/hive-metastore-api-test-0.1.0-tests.jar
```
### 2.3 在服务器上执行
使用 hive 的配置文件 hive-site.xml, AllMetastoreApiTest 使用所有 Hive 支持的数据类型。
```bash
hive --service jar hive-metastore-api-test-0.1.0-tests.jar  com.baidu.hive.metastore.AllMetastoreApiTest
```

使用 hive 的配置文件 hive-site.xml, EdapMetastoreApiTest 使用所有 Edap 支持的数据类型。
```bash
hive --service jar hive-metastore-api-test-0.1.0-tests.jar  com.baidu.hive.metastore.EdapMetastoreApiTest
```