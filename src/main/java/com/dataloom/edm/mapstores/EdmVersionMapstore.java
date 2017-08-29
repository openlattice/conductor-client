package com.dataloom.edm.mapstores;

import java.util.UUID;

import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class EdmVersionMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<String, UUID> {
    private static final CassandraTableBuilder ctb = Table.EDM_VERSIONS.getBuilder();

    public EdmVersionMapstore( Session session ) {
        super( HazelcastMap.EDM_VERSIONS.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( String key, BoundStatement bs ) {
        return bs.setString( CommonColumns.EDM_VERSION_NAME.cql(), key );
    }

    @Override
    protected BoundStatement bind( String key, UUID value, BoundStatement bs ) {
        return bs.setString( CommonColumns.EDM_VERSION_NAME.cql(), key )
                .setUUID( CommonColumns.EDM_VERSION.cql(), value );
    }

    @Override
    protected String mapKey( Row rs ) {
        return rs == null ? null : rs.getString( CommonColumns.EDM_VERSION_NAME.cql() );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return row.getUUID( CommonColumns.EDM_VERSION.cql() );
    }

    @Override
    public String generateTestKey() {
        return "edm";
    }

    @Override
    public UUID generateTestValue() {
        return UUIDs.timeBased();
    }
}
