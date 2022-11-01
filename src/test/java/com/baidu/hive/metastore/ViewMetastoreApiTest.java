package com.baidu.hive.metastore;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.*;

public class ViewMetastoreApiTest extends TableMetastoreAPITest {

    @Test
    public void testView() throws TException {
        log("begin testView");

        String dbName = "view_test";
        dropDatabase(dbName, true);
        createDatabase(dbName);

        dropTable(dbName, "t1", true);
        createTableWithVariousColumnType(dbName, "t1", types.size());

        testCreateViews(dbName, "t1");
        testShowViews(dbName);
        testAlterView(dbName, "t1");

        testDropViews(dbName);
        dropDatabase(dbName, false);
        log("end testView");
    }

    public void testCreateViews(String dbName, String originalTbName) throws TException {
        log("testCreateViews begin");
        for (int i = 1; i <= types.size(); i++) {
            createViewWithVariousColumnType(dbName, originalTbName ,"v2_" + i, i);
        }
        log("testCreateViews end");
    }

    public void testShowViews(String dbName) throws TException {
        List<String> tables = client.getTables(dbName, "v2_*", TableType.VIRTUAL_VIEW);
        assertEquals(types.size(), tables.size());
        for (int i = 1; i <= types.size(); i++) {
            assertTrue(tables.contains("v2_" + i));
        }
    }

      public void testAlterView(String dbName, String originalTbName) throws TException {
        // Every view add one column, and last view cannot add one column.
        for (int i = 1; i <= types.size() - 1; i++) {
            Table view = client.getTable(dbName,"v2_" + i);
            view.getSd().setCols(genCols(i + 1));
            view.setViewOriginalText(createOriginalText(originalTbName, i + 1));
            view.setViewExpandedText(createExpandedText(dbName, originalTbName, i + 1));
            EnvironmentContext environmentContext = new EnvironmentContext();
            client.alter_table_with_environmentContext(dbName, view.getTableName(), view, environmentContext);
            checkTable(view, dbName, view.getTableName());
        }
    }

    public void testDropViews(String dbName) throws TException {
        for (int i = 1; i <= types.size(); i++) {
            this.client.dropTable(dbName, "v2_" + i, true, true, false);
        }
    }

    protected void createViewWithVariousColumnType(String dbName, String originalTableName, String viewName, int columnCount) throws TException {
        Table view = this.createViewObject(dbName, originalTableName, viewName, columnCount);
        client.createTable(view);
        checkTable(view, dbName, viewName);
    }


    private String createOriginalText(String tbName, int columnCount) {
        //  viewOriginalText:select c1 from t1,
        StringBuilder originText = new StringBuilder("select ");
        for (int i = 0; i < columnCount; i++) {
            originText.append("c").append(i);
            if (i != columnCount - 1) {
                originText.append(", ");
            }
        }
        originText.append(" from ").append(tbName);
        // "originText:" + originText);
        return originText.toString();
    }

    private String createExpandedText(String dbName, String originalTableName, int columnCount) {
        // viewExpandedText:select `t1`.`c1` from `view_test`.`t1`,
        StringBuilder viewExpandedText = new StringBuilder("select ");
        for (int i = 0; i < columnCount; i++) {
            viewExpandedText.append("`");
            viewExpandedText.append(originalTableName);
            viewExpandedText.append("`");
            viewExpandedText.append(".`");
            viewExpandedText.append("c").append(i);
            viewExpandedText.append("`");
            if (i != columnCount - 1) {
                viewExpandedText.append(", ");
            }
        }

        viewExpandedText.append(" from `").append(dbName).append("`.`")
                .append(originalTableName).append("`");
        // log("viewExpandedText:" + viewExpandedText);
        return viewExpandedText.toString();
    }

    protected Table createViewObject(String dbName, String originalTableName, String viewName, int columnCount) throws TException {
        Table view = createTableObject(dbName, viewName, columnCount);
        view.setTableType(TableType.VIRTUAL_VIEW.name());
        view.setViewOriginalText(createOriginalText(originalTableName, columnCount));
        view.setViewExpandedText(createExpandedText(dbName, originalTableName, columnCount));
        return view;
    }
}
