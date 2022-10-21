package com.baidu.hive.metastore;

import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class PartitionMetastoreApiTest extends TableMetastoreAPITest {

    @Test
    public void testPartitionTable() throws TException {
        String dbName = "partition_test";
        dropDatabase(dbName, true);
        createDatabase(dbName);

        Table table = createPartitionTable(dbName, "ptable", 10, 2, 100);

        testShowPartitons(table, 2, 100);
        this.dropDatabase(dbName, false);
    }

    private void testShowPartitons(Table table, int partitionColumnCount, int partitionCount) throws TException {
        // List all partitons -- Arguments:[partition_test, ptable, -1]
        List<Partition> allPartitions = this.client.listPartitions(table.getDbName(), table.getTableName(), (short) -1);
        Assert.assertEquals(partitionCount, allPartitions.size());

        // List partitions with specified first partition column.
        int pk1Count = Math.min(10, partitionCount);
        for (int i = 0; i < pk1Count; i++) {
            int expectedPartitionCount = partitionCount / 10;
            int mod = partitionCount % 10;
            if (mod != 0 && i < mod) {
                expectedPartitionCount++;
            }
            List<String> pk1Pattern = new ArrayList<>(1);
            pk1Pattern.add("pc0" + i);
            List<Partition> pk1Partitions = this.client.listPartitions(table.getDbName(),
                    table.getTableName(), pk1Pattern, (short) -1);
            Assert.assertEquals(expectedPartitionCount, pk1Partitions.size());
        }

        // List partition with all partition column specified.
        List<FieldSchema> fieldSchemas = table.getPartitionKeys();
        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            List values = new ArrayList(fieldSchemas.size());
            for (int i = 0; i < fieldSchemas.size(); i++) {
                if (i ==0) {
                    values.add(fieldSchemas.get(i).getName() + (partitionIndex % 10));
                } else {
                    values.add(fieldSchemas.get(i).getName() + partitionIndex);
                }

            }
            List<Partition> partitions = this.client.listPartitions(table.getDbName(),
                    table.getTableName(), values, (short) -1);
            Assert.assertEquals(1, partitions.size());
        }
    }

    protected Table createPartitionTable(String dbName, String tbName, int columnCount,
                                         int partitionColumnCount, int partitionCount) throws TException {
        Table table = this.createPartitionTable(dbName, tbName, columnCount, partitionColumnCount);

        List<FieldSchema> fieldSchemas = table.getPartitionKeys();
        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            List<Partition> partitions = new ArrayList<>(partitionCount);
            List values = new ArrayList(fieldSchemas.size());
            for (int i = 0; i < fieldSchemas.size(); i++) {
                if (i ==0) {
                    values.add(fieldSchemas.get(i).getName() + (partitionIndex % 10));
                } else {
                    values.add(fieldSchemas.get(i).getName() + partitionIndex);
                }

            }
            Partition partition = createPartition4Table(table, values);
            partitions.add(partition);
            this.client.add_partitions(partitions, false, true);
        }
        return table;
    }

    private Partition createPartition4Table(Table table, List<String> values) {
        Partition partition = new Partition();
        partition.setValues(values);
        partition.setDbName(table.getDbName());
        partition.setTableName(table.getTableName());
        StorageDescriptor sd = table.getSd().deepCopy();
        sd.setLocation(getPartitionLocation4Table(table, values));
        partition.setSd(sd);
        return partition;
    }

    private String getPartitionLocation4Table(Table table, List<String> values) {
        StringBuilder sb = new StringBuilder();
        if ("default".equals(table.getDbName())) {
            sb.append(this.catalogLocation + "/" + table.getTableName());
        } else {
            sb.append(this.catalogLocation + "/" + table.getDbName() + ".db/" + table.getTableName());
        }

        if (sb.charAt(sb.length() - 1) == '/') {
            sb.deleteCharAt(sb.length() - 1);
        }
        List<FieldSchema> fieldSchemas = table.getPartitionKeys();
        Assert.assertEquals(fieldSchemas.size(), values.size());
        for (int i = 0; i < fieldSchemas.size(); i++) {
            FieldSchema field = fieldSchemas.get(i);

            sb.append("/").append(field.getName()).append("=").append(values.get(i));
        }
        return sb.toString();
    }

    protected Table createPartitionTable(String dbName, String tbName, int columnCount,
                                        int partitionColumnCount) throws TException {
        // create table tp(c1 int, c2 int) partitioned by (pc1 string, pc2 string) stored as textfile;
        Table table = this.createTableObject(dbName, tbName, columnCount);
        List<FieldSchema> partitionCols = new ArrayList<>();

        for (int i = 0; i < partitionColumnCount; i++) {
            FieldSchema field = new FieldSchema();
            field.setName("pc" + i);
            field.setType("string");
            partitionCols.add(field);
        }

        table.setPartitionKeys(partitionCols);

        client.createTable(table);
        this.checkTable(table, dbName, tbName);
        return table;
    }

    public static void main(String[] args) {
        PartitionMetastoreApiTest test = new PartitionMetastoreApiTest();
        try {
            test.init();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        try {
            test.testDatabase();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        try {
            test.testOthers();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        try {
            test.testTable();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        try {
            test.testPartitionTable();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        test.close();
    }
}
