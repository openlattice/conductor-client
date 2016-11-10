package com.kryptnostic.datastore.cassandra;

import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;

public class ShuttleTestBootstrap {

    protected static SparkSession sparkSession;

    @BeforeClass
    public static void init() {

        sparkSession = SparkSession.builder()
                .master( "local" )
                .appName( "test" )
                .getOrCreate();
    }
}
