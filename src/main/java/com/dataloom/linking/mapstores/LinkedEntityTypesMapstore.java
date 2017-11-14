package com.dataloom.linking.mapstores;

import java.util.UUID;

import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class LinkedEntityTypesMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, DelegatedUUIDSet> {

    public LinkedEntityTypesMapstore( Session session ) {
        super( HazelcastMap.LINKED_ENTITY_TYPES.name(), session, Table.LINKED_ENTITY_TYPES.getBuilder() );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public DelegatedUUIDSet generateTestValue() {
        return DelegatedUUIDSet.wrap( ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID() ) );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, DelegatedUUIDSet value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key )
                .setSet( CommonColumns.ENTITY_TYPE_IDS.cql(), value, UUID.class );
    }

    @Override
    protected UUID mapKey( Row row ) {
        return row == null ? null : row.getUUID( CommonColumns.ID.cql() );
    }

    @Override
    protected DelegatedUUIDSet mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null
                : DelegatedUUIDSet.wrap( row.getSet( CommonColumns.ENTITY_TYPE_IDS.cql(), UUID.class ) );

    }

}
