package com.dataloom.graph.core.mapstores;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.GraphWrappedEntityKey;
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

public class VerticesLookupMapstore
        extends AbstractStructuredCassandraMapstore<GraphWrappedEntityKey, UUID> {

    public VerticesLookupMapstore( Session session ) {
        super( HazelcastMap.VERTICES_LOOKUP.name(), session, Table.VERTICES_LOOKUP.getBuilder() );
    }

    @Override
    protected BoundStatement bind( GraphWrappedEntityKey key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
                .set( CommonColumns.ENTITY_KEY.cql(), key.getEntityKey(), EntityKey.class );
    }

    @Override
    protected BoundStatement bind( GraphWrappedEntityKey key, UUID value, BoundStatement bs ) {
        return bind( key, bs ).setUUID( CommonColumns.VERTEX_ID.cql(), value );
    }

    @Override
    protected GraphWrappedEntityKey mapKey( Row row ) {
        return RowAdapters.graphWrappedEntityKey( row );
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
    public GraphWrappedEntityKey generateTestKey() {
        return new GraphWrappedEntityKey( UUID.randomUUID(), TestDataFactory.entityKey() );
    }

    @Override
    public UUID generateTestValue() {
        return UUID.randomUUID();
    }

}
