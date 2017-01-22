package com.dataloom.edm.mapstores;

import java.util.UUID;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class AclKeysMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<String, UUID> {
    private static final CassandraTableBuilder ctb = Tables.ACL_KEYS.getBuilder();

    public AclKeysMapstore( Session session ) {
        super( HazelcastMap.ACL_KEYS.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( String key, BoundStatement bs ) {
        return bs.setString( CommonColumns.NAME.cql(), key );
    }

    @Override
    protected BoundStatement bind( String key, UUID value, BoundStatement bs ) {
        return bs
                .setString( CommonColumns.NAME.cql(), key )
                .setUUID( CommonColumns.SECURABLE_OBJECTID.cql(), value );
    }

    @Override
    protected String mapKey( Row rs ) {
        return rs == null ? null : rs.getString( CommonColumns.NAME.cql() );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return row.getUUID( CommonColumns.SECURABLE_OBJECTID.cql() );
    }

    @Override
    public String generateTestKey() {
        return TestDataFactory.name();
    }

    @Override
    public UUID generateTestValue()  {
        return UUID.randomUUID();
    }

}
