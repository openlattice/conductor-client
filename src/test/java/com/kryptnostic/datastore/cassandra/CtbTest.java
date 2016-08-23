package com.kryptnostic.datastore.cassandra;

import org.junit.Test;

import com.kryptnostic.conductor.rpc.odata.Tables;

public class CtbTest {

    @Test
    public void testTableQuery() {
        System.out.println( new CassandraTableBuilder( Tables.ENTITY_SETS.getTableName() ).ifNotExists()
                .partitionKey( CommonColumns.TYPE ,CommonColumns.ACLID )
                .clusteringColumns( CommonColumns.NAME )
                .columns( CommonColumns.TITLE ).buildQuery() );
    }
}
