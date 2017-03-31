package com.dataloom.graph.core.mapstores;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.GraphWrappedVertexId;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.graph.core.objects.VertexLabel;
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

public class VerticesMapstore extends AbstractStructuredCassandraMapstore<GraphWrappedVertexId, LoomVertex> {
    private static LoomVertex testValue = new LoomVertex( UUID.randomUUID(), UUID.randomUUID(), new VertexLabel( TestDataFactory.entityKey() ) );
    private static GraphWrappedVertexId testKey = new GraphWrappedVertexId( testValue.getGraphId(), testValue.getKey() );
    
    public VerticesMapstore( Session session ) {
        super( HazelcastMap.VERTICES.name(), session, Table.VERTICES.getBuilder() );
    }

    @Override
    protected BoundStatement bind( GraphWrappedVertexId key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
                .setUUID( CommonColumns.VERTEX_ID.cql(), key.getVertexId() );
    }

    @Override
    protected BoundStatement bind( GraphWrappedVertexId key, LoomVertex value, BoundStatement bs ) {
        return bind( key, bs ).set( CommonColumns.ENTITY_KEY.cql(), value.getLabel().getReference(), EntityKey.class );
    }

    @Override
    protected GraphWrappedVertexId mapKey( Row row ) {
        return RowAdapters.graphWrappedVertexId( row );
    }

    @Override
    protected LoomVertex mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.loomVertex( row );
    }

    @Override
    public GraphWrappedVertexId generateTestKey() {
        return testKey;
    }

    @Override
    public LoomVertex generateTestValue() {
        return testValue;
    }

}
