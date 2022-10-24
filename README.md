# hive-metastore-api-test

## 执行测试的步骤
### 1. 创建测试的 resources 目录
```bash
mkdir -p src/test/resources
```
### 2. 拷贝 hadoop 和 hive 的配置文件
拷贝 hadoop 和 hive 的配置文件到 src/test/resources。
### 3. 执行测试
```bash
mvn clean package
```
如果没有异常，则说明数据库，表和分区测试成功。
