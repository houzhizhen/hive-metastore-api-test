package com.baidu.hive.metastore;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DbMetastoreAPITest extends MetastoreAPITestBase {

    @Test
    public void testDatabase() throws TException {
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
    }

    private void createDatabase(String dbName) throws TException {
        String location = this.catalogLocation + "/" + dbName + ".db";
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

        Assert.assertEquals(dbName, db.getName());
        Assert.assertNull(db.getDescription());
        Assert.assertEquals(location, db.getLocationUri());
        Assert.assertTrue(db.getParameters().isEmpty());
        Assert.assertEquals(location, db.getLocationUri());
        Assert.assertEquals(ownerName, db.getOwnerName());
        Assert.assertEquals(PrincipalType.USER, db.getOwnerType());
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

        Assert.assertEquals(dbName, db.getName());
        Assert.assertEquals(desc, db.getDescription());
        Assert.assertEquals(location, db.getLocationUri());
        Assert.assertEquals(parameters, db.getParameters());
        Assert.assertEquals(location, db.getLocationUri());
        Assert.assertEquals(ownerName, db.getOwnerName());
        Assert.assertEquals(PrincipalType.USER, db.getOwnerType());

        Assert.assertEquals(desc, db.getDescription());
        Assert.assertEquals(location, db.getLocationUri());
    }

    private void dropDatabase(String dbName, boolean ifExists) {
        try {
            this.client.dropDatabase(dbName);
        } catch (TException e) {
            if (!ifExists) {
                throw new RuntimeException(e);
            }
        }
    }

    private void showDatabases(String[] dbArray) throws TException {
        List<String> dbs = this.client.getAllDatabases();
        for (String dbName : dbArray) {
            Assert.assertTrue(dbs.contains(dbName));
        }
    }
}