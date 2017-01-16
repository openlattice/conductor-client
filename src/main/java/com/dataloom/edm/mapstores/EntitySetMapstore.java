package com.dataloom.edm.mapstores;

import java.util.UUID;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;


public class EntitySetMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, EntitySet> {
    private static final CassandraTableBuilder ctb = Tables.ENTITY_SETS.getBuilder();
    
    public EntitySetMapstore( Session session ) {
        super( HazelcastMap.ENTITY_SETS.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, EntitySet value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key )
                .setString( CommonColumns.NAME.cql(), value.getName() )
                .set( CommonColumns.TYPE.cql(), value.getType(), FullQualifiedName.class )
                .setUUID( CommonColumns.ENTITY_TYPE_ID.cql(), value.getEntityTypeId() )
                .setString( CommonColumns.TITLE.cql(), value.getTitle() )
                .setString( CommonColumns.DESCRIPTION.cql(), value.getDescription() );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.ID.cql() );
    }

    @Override
    protected EntitySet mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return new EntitySet(
                row.getUUID( CommonColumns.ID.cql() ),
                row.get( CommonColumns.TYPE.cql(), FullQualifiedName.class ),
                row.getUUID( CommonColumns.ENTITY_TYPE_ID.cql() ),
                row.getString( CommonColumns.NAME.cql() ),
                row.getString( CommonColumns.TITLE.cql() ),
                Optional.of( row.getString( CommonColumns.DESCRIPTION.cql() ) ) );
    }
    
    @Override
    public UUID generateTestKey() {
        throw new NotImplementedException( "GENERATION OF TEST KEY NOT IMPLEMENTED FOR ENTITY SET MAPSTORE." );
    }

    @Override
    public EntitySet generateTestValue() throws Exception {
        throw new NotImplementedException( "GENERATION OF TEST VALUE NOT IMPLEMENTED FOR ENTITY SET MAPSTORE." );
    }

    
}
