package com.baidu.hive.metastore;

import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DbMetastoreAPITest extends MetastoreAPITestBase {

    @Test
    public void testDatabase() throws TException {
        log("begin testDatabase");
        String[] dbArray = new String[] {"meta_test11", "meta_test12"};

        for (int i = 0; i < dbArray.length; i++) {
            dropDatabase("meta_test11", true);
            dropDatabase("meta_test12", true);
        }

        for (String dbName: dbArray) {
            createDatabase(dbName);
        }

        for (String dbName: dbArray) {
            dropDatabase(dbName, false);
        }

        for (String dbName: dbArray) {
            createDatabase(dbName, generateParameters());
        }
        showDatabases(dbArray);
        for (String dbName: dbArray) {
            dropDatabase(dbName, false);
        }
        log("end testDatabase");
    }


    @Test
    public void testOthers() {
        log("begin testOthers");
        // testIsCompatibleWith();
        testFlushCache();
        log("end testOthers");
    }

    private void testIsCompatibleWith() {
        boolean isCompatibleWith = this.client.isCompatibleWith(this.conf);
        assertTrue(isCompatibleWith);
        boolean isCompatibleWith2 = this.client.isCompatibleWith(new HiveConf());
        assertFalse(isCompatibleWith2);
    }

    private void testFlushCache() {
        this.client.flushCache();
    }

    /**
     * Create database always with location even if there is no location in create database sql statemetn.
     * @param dbName the database name
     * @throws TException if metastore has exception
     */
    protected void createDatabase(String dbName) throws TException {
        String location = this.catalogLocation + "/" + dbName + ".db";
        log("create database location " + location);
        String ownerName = "houzhizhen";
        Database database = new Database();
        database.setName(dbName);
        database.setDescription(null);
        database.setLocationUri(location);
        database.setParameters(null);
        database.setOwnerName(ownerName);
        database.setOwnerType(PrincipalType.USER);
        this.client.createDatabase(database);

        Database db = this.client.getDatabase(dbName);

        assertEquals(dbName, db.getName());
        assertNull(db.getDescription());
        assertEquals(location, db.getLocationUri());
        assertTrue(db.getParameters().isEmpty());
        assertEquals(location, db.getLocationUri());
        assertEquals(ownerName, db.getOwnerName());
        assertEquals(PrincipalType.USER, db.getOwnerType());
    }

    /**
     * Create database with parameters.
     */
    private void createDatabase(String dbName, Map<String, String> parameters) throws TException {
        String location = this.catalogLocation + "/" + dbName + ".db";
        String ownerName = "houzhizhen";
        Database database = new Database();
        database.setName(dbName);
        String desc = generateDescription();
        database.setDescription(desc);
        database.setLocationUri(location);
        database.setParameters(parameters);
        database.setOwnerName(ownerName);
        database.setOwnerType(PrincipalType.USER);
        this.client.createDatabase(database);

        Database db = this.client.getDatabase(dbName);

        assertEquals(dbName, db.getName());
        assertEquals(desc, db.getDescription());
        assertEquals(location, db.getLocationUri());
        assertEquals(parameters, db.getParameters());
        assertEquals(location, db.getLocationUri());
        assertEquals(ownerName, db.getOwnerName());
        assertEquals(PrincipalType.USER, db.getOwnerType());

        assertEquals(desc, db.getDescription());
        assertEquals(location, db.getLocationUri());
    }

    protected void dropDatabase(String dbName, boolean ignoreUnknownDb) {
        try {
            if (databaseExist(dbName)) {
                clean(dbName);
            }
            this.client.dropDatabase(dbName, true, ignoreUnknownDb, true);
        } catch (TException e) {
            if (!ignoreUnknownDb) {
                throw new RuntimeException(e);
            }
        }
    }

    private void showDatabases(String[] dbArray) throws TException {
        List<String> dbs = this.client.getAllDatabases();
        for (String dbName : dbArray) {
            assertTrue(dbs.contains(dbName));
        }
    }

    private boolean databaseExist(String db) throws TException {
        List<String> dbs = this.client.getAllDatabases();
        return dbs.contains(db);
    }

    final void clean(String dbName) throws TException {
        List<String> tables = client.getTables(dbName, "*");
        for (String table : tables) {
            System.out.println("drop table " + dbName + "." + table +
                    " from DbMetastoreTest");
            client.dropTable(dbName, table);
        }

        List<String> functions = client.getFunctions(dbName, "*");
        for (String function : functions) {
            System.out.println("drop function " + dbName + "." + function +
                    " from DbMetastoreTest");
            client.dropFunction(dbName, function);
        }
    }
}
