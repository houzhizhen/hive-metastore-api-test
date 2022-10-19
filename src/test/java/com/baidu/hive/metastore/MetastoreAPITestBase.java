package com.baidu.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Before;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MetastoreAPITestBase {

    protected static String CATALOG_LOCATION_KEY="hive.catalog.location";
    protected static String CATALOG_LOCATION_DEFAULT = "hdfs://localhost:9000/user/hive/warehouse";

    protected String catalogLocation;

    protected Configuration conf;
    protected IMetaStoreClient client;

    @Before
    public void init() throws MetaException {
        this.conf = MetastoreConf.newMetastoreConf();;
        // Disable txn
        this.conf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname,
                "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_SUPPORT_CONCURRENCY,
                HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.defaultBoolVal);
        logParameter(conf, HiveConf.ConfVars.HIVE_TXN_MANAGER.varname);
        logParameter(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname);
        this.catalogLocation = this.conf.get(CATALOG_LOCATION_KEY, CATALOG_LOCATION_DEFAULT);
        this.client = MetaStoreUtil.createMetaStoreClient(this.conf);
    }

    @After
    public void close() {
        this.client.close();
        log("MetaStoreApiTest closed");
    }

    private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static void log(String... x) {
        for (String s : x) {
            System.out.println(format.format(new Date()) + " " + s);
        }
    }

    public static void logParameter(Configuration hiveConf, String parameterName) {
        log(parameterName + "=" + hiveConf.get(parameterName));
    }


    public static Map<String, String> generateParameters() {
        Map<String, String> parameters = new HashMap<>(10);
        for (int i = 1; i < 5; i++) {
            parameters.put("key" + i, String.valueOf(i));
        }
        return parameters;
    }

    public static String generateDescription() {
        return "fixed description";
    }
}
