package com.baidu.hive.metastore;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.*;

public class TableMetastoreAPITest extends DbMetastoreAPITest {

    @Test
    public void testTable() throws TException {
        log("begin testTable");
        // default database
        dropTable("default", "t1", true);
        createTableWithVariousColumnType("default", "t1", 1);
        dropTable("default", "t1", false);

        String dbName = "table_test";
        dropDatabase(dbName, true);
        createDatabase(dbName);

        dropTable(dbName, "t1", true);
        createTableWithVariousColumnType(dbName, "t1", 2000);
        dropTable(dbName, "t1", false);

        testShowTables(dbName);

        testAlterTable(dbName);

        testAlterColumns(dbName);
        // TODO: testExteranlTable(dbName);
        dropDatabase(dbName, false);
        log("end testTable");
    }

    protected void testAlterTable(String dbName) throws TException {
        String originalTableName = "original_name";
        String renamedTableName = "renamed_name";
        createTableWithVariousColumnType(dbName, originalTableName, 2);
        Table table = this.client.getTable(dbName, originalTableName);
        table.setTableName(renamedTableName);
        EnvironmentContext environmentContext = new EnvironmentContext();
        environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
        environmentContext.putToProperties(HiveMetaHook.ALTER_TABLE_OPERATION_TYPE, AlterTableDesc.AlterTableTypes.RENAME.name());

        this.client.alter_table_with_environmentContext(dbName,originalTableName, table, environmentContext);
        Table renamedTable = this.client.getTable(dbName, renamedTableName);
        checkLocation(renamedTable);
    }

    protected List<FieldSchema> genCols(int columnCount) {
        List<FieldSchema> cols = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            FieldSchema field = new FieldSchema();
            ColType colType = types.get(i % types.size());
            field.setName("c" + i);
            StringBuilder typeName = new StringBuilder(colType.typeName);
            if (colType.hasPrecision) {
                typeName.append("(").append(colType.precision);
                if (colType.hasScale) {
                    typeName.append(",").append(colType.scale);
                }
                typeName.append(")");
            }
            field.setType(typeName.toString());
            cols.add(field);
        }
        return cols;
    }
    
    public void testAlterColumns(String dbName) throws TException {
        String tableName = "tab_change_columns";
        this.dropTable(dbName, tableName, true);
        // create table tab_change_columns(c1 int, c2 int, c3 int) stored as textfile;
        this.createTableAllColumnsIntType(dbName, tableName, 3);
        Table table = this.client.getTable(dbName, tableName);
        // alter table tab_change_columns change column c1 c1_new string last;
        List<FieldSchema> cols = table.getSd().getCols();
        List<FieldSchema> newCols = new ArrayList<>();
        newCols.add(cols.get(1));
        newCols.add(cols.get(2));
        FieldSchema new_c1 = cols.get(0).deepCopy();
        new_c1.setType("string");
        new_c1.setName("c1_new");
        newCols.add(new_c1);
        table.getSd().setCols(newCols);

        EnvironmentContext environmentContext = new EnvironmentContext();
        environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
        environmentContext.putToProperties(HiveMetaHook.ALTER_TABLE_OPERATION_TYPE, AlterTableDesc.AlterTableTypes.RENAMECOLUMN.name());
        this.client.alter_table_with_environmentContext(dbName,tableName, table, environmentContext);

        // Verify
        Table newTable = this.client.getTable(dbName,tableName);
        assertEquals(newCols, newTable.getSd().getCols());
        this.dropTable(dbName, tableName, false);
    }

    public void testShowTables(String dbName) throws TException {
        for (int i = 1; i <= types.size(); i++) {
            createTableWithVariousColumnType(dbName, "t2_" + i, i);
        }
        List<String> tables = client.getTables(dbName, "t2_*");
        assertEquals(types.size(), tables.size());
        for (int i = 1; i <= types.size(); i++) {
            assertTrue(tables.contains("t2_" + i));
        }
        for (int i = 1; i <= types.size(); i++) {
            dropTable(dbName, "t2_" + i, false);
        }
    }

    protected void dropTable(String dbName, String tbName, boolean ignoreUnknownTab) throws TException {
        client.dropTable(dbName, tbName, true, ignoreUnknownTab);
    }

    protected static List<ColType> types = new ArrayList<>();

    static {
        types.add(new ColType("boolean"));
        types.add(new ColType("tinyint"));
        types.add(new ColType("smallint"));
        types.add(new ColType("int"));
        types.add(new ColType("bigint"));
        types.add(new ColType("float"));
        types.add(new ColType("double"));
        types.add(new ColType("char", 10));
        types.add(new ColType("varchar", 20));
        types.add(new ColType("string"));
        types.add(new ColType("date"));
//        types.add(new ColType("datetime"));
        types.add(new ColType("timestamp"));
        types.add(new ColType("decimal", 18, 2));
        types.add(new ColType("binary"));
//        types.add(new ColType("interval_year_month"));
//        types.add(new ColType("interval_day_time"));
//        types.add(new ColType("timestamp with time zone"));
        types.add(new ColType("map<string,string>"));
        // types.add(new CloumnType("list[string]"));
        types.add(new ColType("struct<id:int,address:string>"));
        types.add(new ColType("uniontype<int,double>"));
    }

    static class ColType {
        final String typeName;
        final boolean hasPrecision;
        final int precision;
        final boolean hasScale;
        final int scale;

        public ColType(String typeName) {
            this (typeName, false, false);
        }
        public ColType(String typeName, boolean hasPrecision, boolean hasScale) {
            this.typeName = typeName;
            this.hasPrecision = hasPrecision;
            this.hasScale = hasScale;
            this.precision = 0;
            this.scale = 0;
        }

        public ColType(String typeName, int precision) {
            this.typeName = typeName;
            this.precision = precision;
            this.hasPrecision = true;
            this.hasScale = false;
            this.scale = 0;
        }
        public ColType(String typeName, int precision, int scale) {
            this.typeName = typeName;
            this.precision = precision;
            this.hasPrecision = true;
            this.hasScale = true;
            this.scale = scale;
        }
    }

    @Test
    public void testTypes() {
        StringBuilder sb = new StringBuilder();
        sb.append("create table t22(");
        for (int i = 0; i < types.size(); i++) {
            ColType colType = types.get(i);
            if (i != 0) {
                sb.append(",");
            }
            sb.append("c").append(i).append(" ").append(colType.typeName);
            if (colType.hasPrecision) {
                sb.append("(").append(colType.precision);
                if (colType.hasScale) {
                    sb.append(",").append(colType.scale);
                }
                sb.append(")");
            }
        }
        sb.append(")");
        log(sb.toString());
    }

    protected void createTableWithVariousColumnType(String dbName, String tbName, int columnCount) throws TException {
        Table table = this.createTableObject(dbName, tbName, columnCount);

        table.getSd().setCols(genCols(columnCount));
        client.createTable(table);
        checkTable(table, dbName, tbName);
    }

    protected Table createTableObject(String dbName, String tbName, int columnCount) {
        Table table = new Table();
        table.setDbName(dbName);
        table.setTableName(tbName);
        table.setOwner("houzhizhen");
        table.setCreateTime(1661331824);
        table.setLastAccessTime(0);
        table.setRetention(0);

        StorageDescriptor sd = new StorageDescriptor();
        List<FieldSchema> cols = new ArrayList<>();

        for (int i = 0; i < columnCount; i++) {
            FieldSchema field = new FieldSchema();
            field.setName("c" + i);
            field.setType("int");
            cols.add(field);
        }

        sd.setCols(cols);
        // location:null,
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
        sd.setNumBuckets(-1);
        /**
         * serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.ql.io.orc.OrcSerde,
         *          * parameters:{serialization.format=1})
         */
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        HashMap<String, String> serdeInfoParameters = new HashMap<>();
        serdeInfoParameters.put("serialization.format", "1");
        serDeInfo.setParameters(serdeInfoParameters);
        sd.setSerdeInfo(serDeInfo);

        sd.setBucketCols(Collections.EMPTY_LIST);
        sd.setSortCols(Collections.EMPTY_LIST);
        sd.setParameters(Collections.EMPTY_MAP);

        SkewedInfo skewedInfo = new SkewedInfo();
        skewedInfo.setSkewedColNames(Collections.EMPTY_LIST);
        skewedInfo.setSkewedColValues(Collections.EMPTY_LIST);
        skewedInfo.setSkewedColValueLocationMaps(Collections.EMPTY_MAP);
        sd.setSkewedInfo(skewedInfo);

        sd.setStoredAsSubDirectories(false);
        table.setSd(sd);

        table.setPartitionKeys(Collections.EMPTY_LIST);

        table.setParameters(newMap("totalSize", "0", "numRows", "0", "rawDataSize", "0",
                "COLUMN_STATS_ACCURATE", "{\"BASIC_STATS\":\"true\",\"COLUMN_STATS\":{\"c1\":\"true\"}}",
                "numFiles", "0", "bucketing_version", "2"));
        table.setViewOriginalText(null);
        table.setViewExpandedText(null);
        table.setTableType("MANAGED_TABLE");

        PrincipalPrivilegeSet privilegeSet = new PrincipalPrivilegeSet();
        Map<String, List<PrivilegeGrantInfo>> userPrivileges = new HashMap<>();
        List<PrivilegeGrantInfo> privilegeGrantInfoList = new ArrayList<>();

        PrivilegeGrantInfo grantInfo1 = new PrivilegeGrantInfo();
        grantInfo1.setPrivilege("INSERT");
        grantInfo1.setCreateTime(-1);
        grantInfo1.setGrantor("houzhizhen");
        grantInfo1.setGrantorType(PrincipalType.USER);
        grantInfo1.setGrantOption(true);
        privilegeGrantInfoList.add(grantInfo1);

        PrivilegeGrantInfo grantInfo2 = grantInfo1.deepCopy();
        grantInfo2.setPrivilege("SELECT");
        privilegeGrantInfoList.add(grantInfo2);

        PrivilegeGrantInfo grantInfo3 = grantInfo1.deepCopy();
        grantInfo3.setPrivilege("UPDATE");
        privilegeGrantInfoList.add(grantInfo3);

        PrivilegeGrantInfo grantInfo4 = grantInfo1.deepCopy();
        grantInfo4.setPrivilege("DELETE");
        privilegeGrantInfoList.add(grantInfo4);

        userPrivileges.put("houzhizhen",privilegeGrantInfoList);

        privilegeSet.setUserPrivileges(userPrivileges);
        privilegeSet.setGroupPrivileges(null);
        privilegeSet.setRolePrivileges(null);
        table.setPrivileges(privilegeSet);
        table.setTemporary(false);
        table.setCatName("hive");
        table.setOwnerType(PrincipalType.USER);
        return table;
    }
    protected void createTableAllColumnsIntType(String dbName, String tbName, int columnCount) throws TException {
        Table table = createTableObject(dbName, tbName, columnCount);
        client.createTable(table);
        checkTable(table, dbName, tbName);
    }

    protected void checkTable(Table table, String dbName, String tbName) throws TException {
        Table table2 = client.getTable(dbName, tbName);

        assertEquals(table.getTableName(), table2.getTableName());
        assertEquals(table.getDbName(), table2.getDbName());
        assertEquals(table.getOwner(), table2.getOwner());
        // assertEquals(table.getCreateTime(), table2.getCreateTime());
        assertEquals(table.getLastAccessTime(), table2.getLastAccessTime());
        assertEquals(table.getRetention(), table2.getRetention());
        StorageDescriptor sd = table.getSd();
        StorageDescriptor sd2 = table2.getSd();
        assertEquals(sd.getCols(), sd2.getCols());
        checkLocation(table2);
        sd2.setLocation(null);
        assertEquals(sd.getCols(), sd2.getCols());
        assertEquals(table.getPartitionKeys(), table2.getPartitionKeys());
        Map<String, String> table2Parameters = table2.getParameters();
        table2Parameters.remove("transient_lastDdlTime");
        table2Parameters.remove("last_modified_time");

        Map<String, String> tableParameters = table.getParameters();
        tableParameters.remove("transient_lastDdlTime");
        tableParameters.remove("last_modified_time");
        assertEquals(tableParameters, table2Parameters);
        PrincipalPrivilegeSet privilegeSet2 = table2.getPrivileges();
        // TODO: table2.getPrivileges() == null, not checked
        // assertEquals(table.getPrivileges(), privilegeSet2);
        assertEquals(table.isTemporary(), table2.isTemporary());
        System.out.println("table.getCatName():" + table.getCatName());
        System.out.println("table2.getCatName():" + table2.getCatName());
        // assertEquals(table.getCatName(), table2.getCatName());
        assertEquals(table.getOwnerType(), table2.getOwnerType());
        assertEquals(table.getTableType(), table2.getTableType());
        if (table.getTableType().equals(TableType.VIRTUAL_VIEW)) {
            assertEquals(table.getViewOriginalText(), table2.getViewOriginalText());
            assertEquals(table.getViewExpandedText(), table2.getViewExpandedText());
        }
    }

    protected void checkLocation(Table table) {
        if (table.getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
            return;
        }
        String location = table.getSd().getLocation();
        String expectedLocation;
        if ("default".equals(table.getDbName())) {
            expectedLocation =  this.catalogLocation + "/" + table.getTableName();
        } else {
            expectedLocation =  this.catalogLocation + "/" + table.getDbName() + ".db/" + table.getTableName();
        }

        assertEquals(expectedLocation, location);
    }
}
