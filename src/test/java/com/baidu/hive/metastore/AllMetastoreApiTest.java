package com.baidu.hive.metastore;

public class AllMetastoreApiTest {
    
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
        try {
            test.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        ViewMetastoreApiTest viewMetastoreApiTest = new ViewMetastoreApiTest();
        try {
            viewMetastoreApiTest.init();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        try {
            viewMetastoreApiTest.testView();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        try {
            viewMetastoreApiTest.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        FunctionMetastoreTest functionMetastoreTest = new FunctionMetastoreTest();
        try {
            functionMetastoreTest.init();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        try {
            functionMetastoreTest.testFunction();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        try {
            functionMetastoreTest.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
