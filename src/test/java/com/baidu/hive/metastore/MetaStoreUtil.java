package com.baidu.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;

public class MetaStoreUtil {
    private static final String METASTORE_CLIENT_CLASS = "hive.metastore.client.class";  //  The name of the class that implementing the IMetaStoreClient interface.
    private static final String METASTORE_CLIENT_CLASS_DEFAULT = "org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient";

    public static IMetaStoreClient createMetaStoreClient(Configuration hiveConf) throws MetaException {
        String mscClassName = hiveConf.get(METASTORE_CLIENT_CLASS, METASTORE_CLIENT_CLASS_DEFAULT);
        Class<? extends IMetaStoreClient> baseClass = JavaUtils.getClass(mscClassName, IMetaStoreClient.class);
        return JavaUtils.newInstance(baseClass,
                                     new Class[]{Configuration.class, HiveMetaHookLoader.class, Boolean.class},
                                     new Object[]{hiveConf, null, false});
    }
}
