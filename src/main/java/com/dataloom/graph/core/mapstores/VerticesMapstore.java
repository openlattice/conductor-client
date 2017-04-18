package com.dataloom.graph.core.mapstores;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class VerticesMapstore
        extends AbstractStructuredCassandraMapstore<EntityKey, UUID> {

    public VerticesMapstore( Session session ) {
        super( HazelcastMap.VERTICES.name(), session, Table.VERTICES.getBuilder() );
    }

    @Override
    protected BoundStatement bind( EntityKey key, BoundStatement bs ) {
        return bs.set( CommonColumns.ENTITY_KEY.cql(), key, EntityKey.class );
    }

    @Override
    protected BoundStatement bind( EntityKey key, UUID value, BoundStatement bs ) {
        return bind( key, bs ).setUUID( CommonColumns.VERTEX_ID.cql(), value );
    }

    @Override
    protected EntityKey mapKey( Row row ) {
        return RowAdapters.entityKey( row );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return row.getUUID( CommonColumns.VERTEX_ID.cql() );
    }

    @Override
    public EntityKey generateTestKey() {
        return TestDataFactory.entityKey();
    }

    @Override
    public UUID generateTestValue() {
        return UUID.randomUUID();
    }

}
