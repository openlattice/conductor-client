package com.dataloom.data.mapstores;

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

public class SyncIdsMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, UUID> {
    private static final CassandraTableBuilder ctb = Table.SYNC_IDS.getBuilder();

    public SyncIdsMapstore( Session session ) {
        super( HazelcastMap.SYNC_IDS.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, UUID value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key )
                .setUUID( CommonColumns.SYNCID.cql(), value )
                .setUUID( CommonColumns.LATEST_SYNC_ID.cql(), value );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.ENTITY_SET_ID.cql() );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return row.getUUID( CommonColumns.LATEST_SYNC_ID.cql() );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public UUID generateTestValue() {
        return UUIDs.timeBased();
    }
}
