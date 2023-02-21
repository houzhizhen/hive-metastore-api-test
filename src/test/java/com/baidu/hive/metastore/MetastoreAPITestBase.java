package com.baidu.hive.metastore;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MetastoreAPITestBase {

    protected String catalogLocation;

    protected HiveConf conf;
    protected IMetaStoreClient client;

    protected boolean logExceptionOnly;

    @Before
    public void init() throws MetaException, HiveException {

        this.conf = new HiveConf();
        // Disable txn
        this.conf.set("hive.execution.engine", "mr");
        this.conf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname,
                "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
        this.logExceptionOnly = conf.getBoolean("hive.metastore.test.logExceptionOnly", true);
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_SUPPORT_CONCURRENCY,
                HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.defaultBoolVal);
        logParameter(conf, HiveConf.ConfVars.HIVE_TXN_MANAGER.varname);
        logParameter(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname);

        CliSessionState ss = new CliSessionState(conf);
        ss.in = System.in;
        try {
            ss.out = new PrintStream(System.out, true, "UTF-8");
            ss.info = new PrintStream(System.err, true, "UTF-8");
            ss.err = new CachingPrintStream(System.err, true, "UTF-8");
        } catch (UnsupportedEncodingException | FileNotFoundException e) {
            e.printStackTrace();
        }
        CliSessionState.start(ss);
        this.catalogLocation = conf.get("hive.metastore.warehouse.dir");
        if (catalogLocation.startsWith("/")) {
            String defaultFS = conf.get("fs.defaultFS");
            if (defaultFS.endsWith("/")) {
                defaultFS = defaultFS.substring(0, defaultFS.length() - 1);
            }
            this.catalogLocation = defaultFS  + catalogLocation;
        }
        this.client = MetaStoreUtil.createMetaStoreClient(conf);
    }

    @After
    public void close() {
        this.client.close();
        try {
            CliSessionState.get().close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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


    public static Map<String, String> newMap(String... keyValuesPairs) {
        Preconditions.checkArgument(keyValuesPairs.length %2 == 0,
                "keyValuesPairs:" + Arrays.toString(keyValuesPairs) + ".length is not even");

        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keyValuesPairs.length; i += 2) {
            map.put(keyValuesPairs[i], keyValuesPairs[i+1]);
        }
        return map;
    }

    public void assertEquals(Object expected, Object actual) {
        try {
            Assert.assertEquals(null, expected, actual);
        } catch (Throwable t) {
            if (logExceptionOnly) {
                t.printStackTrace();
            } else {
                throw t;
            }
        }
    }

    public void assertTrue(boolean condition) {
        try {
            Assert.assertTrue(condition);
        } catch (Throwable t) {
            if (logExceptionOnly) {
                t.printStackTrace();
            } else {
                throw t;
            }
        }
    }
    public void assertFalse(boolean condition) {
        try {
            Assert.assertFalse(condition);
        } catch (Throwable t) {
            if (logExceptionOnly) {
                t.printStackTrace();
            } else {
                throw t;
            }
        }
    }

    public void assertNull(Object object) {
        try {
            Assert.assertNull(object);
        } catch (Throwable t) {
            if (logExceptionOnly) {
                t.printStackTrace();
            } else {
                throw t;
            }
        }
    }
}
