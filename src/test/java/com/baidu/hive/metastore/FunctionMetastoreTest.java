package com.baidu.hive.metastore;

import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 1. On linux shell put hive-util-0.1.0.jar to /apps/hive-util-0.1.0.jar, create /apps if not exists.
 * hadoop fs -put hive-util-0.1.0.jar /apps/hive-util-0.1.0.jar;
 * 2. execute the following sql on hive.
 * create function test_function.addint as 'com.baidu.hive.func.AddInt' using jar 'hdfs://localhost:9000/apps/hive-util-0.1.0.jar';
 */
public class FunctionMetastoreTest extends DbMetastoreAPITest  {


    @Test
    public void testFunction() throws TException {
        log("begin testFunction");

        String dbName = "function_test";
        dropDatabase(dbName, true);
        createDatabase(dbName);



        List<Function> functions = testCreateFunctions(dbName, 10);

        testShowFunctions(dbName, functions);
        testDropFunctions(dbName, 10);
        testShowFunctions(dbName, Collections.EMPTY_LIST);
        dropDatabase(dbName, false);
        log("end testFunction");
    }

    private void testShowFunctions(String dbName, List<Function> expectedFunctions) throws TException {
        List<Function> functions2 = this.client.getAllFunctions().getFunctions().stream().filter(new Predicate<Function>() {
            @Override
            public boolean test(Function function) {
                return function.getDbName().equals(dbName);
            }
        }).collect(Collectors.toList());
        log("getAllFunctions: dbName=" + dbName + ", get " + functions2.toString());
        assertEquals(expectedFunctions.size(), functions2.size());
        for (int i = 0; i < expectedFunctions.size(); i++) {
            Function f1 = expectedFunctions.get(i);
            Function f2 = functions2.get(i);
            f1.setCreateTime(1);
            f2.setCreateTime(1);
            assertEquals(f1, f2);
        }
    }

    private List<Function> testCreateFunctions(String dbName, int functionCount) throws TException {
        log("testCreateFunctions begin");
        List<Function> functions = new ArrayList<>();
        for (int i = 0; i < functionCount; i++) {
            Function func = new Function();
            func.setFunctionName("addint_" + i);
            func.setDbName(dbName);
            func.setClassName("com.baidu.hive.func.AddInt");
            func.setOwnerName("houzhizhen");
            func.setOwnerType(PrincipalType.USER);
            func.setCreateTime(1667295875);
            func.setFunctionType(FunctionType.JAVA);
            List<ResourceUri> resourceUris = new ArrayList<>(1);
            ResourceUri uri = new ResourceUri();
            uri.setResourceType(ResourceType.JAR);
            uri.setUri("hdfs://localhost:9000/apps/hive-util-0.1.0.jar");
            resourceUris.add(uri);
            func.setResourceUris(resourceUris);
            this.client.createFunction(func);
            log("create function " + func.getDbName() + "." + func.getFunctionName());
            functions.add(func);
        }

        log("testCreateFunctions end");
        return functions;
    }

    private void testDropFunctions(String dbName, int functionCount) throws TException {
        log("testDropFunctions begin");
        for (int i = 0; i < functionCount; i++) {
            log("drop function " + dbName + "." + "addint_" + i);
            this.client.dropFunction(dbName, "addint_" + i);
        }
        log("testDropFunctions end");
    }
}
