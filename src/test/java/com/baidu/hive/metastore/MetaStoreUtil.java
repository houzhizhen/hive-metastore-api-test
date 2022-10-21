package com.baidu.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class MetaStoreUtil {

    public static IMetaStoreClient createMetaStoreClient(HiveConf hiveConf) throws MetaException, HiveException {
        Hive hive = Hive.get(hiveConf);
        return hive.getMSC();
    }
}
