package com.dataloom.edm.mapstores;

import java.util.UUID;

import com.dataloom.edm.set.EntitySetPropertyKey;
import com.dataloom.edm.set.EntitySetPropertyMetadata;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class EntitySetPropertyMetadataMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<EntitySetPropertyKey, EntitySetPropertyMetadata> {
    private static final CassandraTableBuilder ctb = Table.ENTITY_SET_PROPERTY_METADATA.getBuilder();

    public EntitySetPropertyMetadataMapstore( Session session ) {
        super( HazelcastMap.ENTITY_SET_PROPERTY_METADATA.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( EntitySetPropertyKey key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key.getEntitySetId() )
                .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), key.getPropertyTypeId() );
    }

    @Override
    protected BoundStatement bind( EntitySetPropertyKey key, EntitySetPropertyMetadata value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key.getEntitySetId() )
                .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), key.getPropertyTypeId() )
                .setString( CommonColumns.TITLE.cql(), value.getTitle() )
                .setString( CommonColumns.DESCRIPTION.cql(), value.getDescription() )
                .setBool( CommonColumns.SHOW.cql(), value.getDefaultShow() );
    }

    @Override
    protected EntitySetPropertyKey mapKey( Row rs ) {
        return rs == null ? null : RowAdapters.entitySetPropertyKey( rs );
    }

    @Override
    protected EntitySetPropertyMetadata mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.entitySetPropertyMetadata( row );
    }

    @Override
    public EntitySetPropertyKey generateTestKey() {
        return new EntitySetPropertyKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override
    public EntitySetPropertyMetadata generateTestValue() {
        return new EntitySetPropertyMetadata( "title", "description", true );
    }

}
