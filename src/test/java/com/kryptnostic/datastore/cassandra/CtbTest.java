package com.kryptnostic.datastore.cassandra;

import org.junit.Test;

import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;

public class CtbTest {

    //TODO: Move the test for this into Rhizome as the Table Builder lives there now.
    @Test
    public void testTableQuery() {
        System.out.println( new CassandraTableBuilder( Tables.ENTITY_SETS.getName() ).ifNotExists()
                .partitionKey( CommonColumns.TYPE ,CommonColumns.ACLID )
                .clusteringColumns( CommonColumns.NAME )
                .columns( CommonColumns.TITLE ).buildCreateTableQuery() );
    }
}
