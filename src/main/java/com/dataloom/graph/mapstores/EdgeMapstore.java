package com.dataloom.graph.mapstores;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeLabel;
import com.dataloom.graph.core.objects.GraphWrappedEdgeKey;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class EdgeMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<GraphWrappedEdgeKey, LoomEdge> {

    public EdgeMapstore( Session session ) {
        super( HazelcastMap.EDGES.name(), session, Table.EDGES.getBuilder() );
    }

    @Override
    protected BoundStatement bind( GraphWrappedEdgeKey key, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
                .setUUID( CommonColumns.SRC_VERTEX_ID.cql(), key.getEdgeKey().getSrcId() )
                .setUUID( CommonColumns.DST_VERTEX_ID.cql(), key.getEdgeKey().getDstId() )
                .setUUID( CommonColumns.TIME_ID.cql(), key.getEdgeKey().getTimeId() );
    }

    @Override
    protected BoundStatement bind( GraphWrappedEdgeKey key, LoomEdge value, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
                .setUUID( CommonColumns.SRC_VERTEX_ID.cql(), key.getEdgeKey().getSrcId() )
                .setUUID( CommonColumns.DST_VERTEX_ID.cql(), key.getEdgeKey().getDstId() )
                .setUUID( CommonColumns.TIME_ID.cql(), key.getEdgeKey().getTimeId() )
                .setUUID( CommonColumns.SRC_VERTEX_TYPE_ID.cql(), value.getLabel().getSrcType() )
                .setUUID( CommonColumns.DST_VERTEX_TYPE_ID.cql(), value.getLabel().getDstType() )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), value.getLabel().getReference().getEntitySetId() )
                .setString( CommonColumns.EDGE_ID.cql(), value.getLabel().getReference().getEntityId() );
    }

    @Override
    protected GraphWrappedEdgeKey mapKey( Row rs ) {
        return new GraphWrappedEdgeKey(
                rs.getUUID( CommonColumns.GRAPH_ID.cql() ),
                new EdgeKey(
                        rs.getUUID( CommonColumns.SRC_VERTEX_ID.cql() ),
                        rs.getUUID( CommonColumns.DST_VERTEX_ID.cql() ),
                        rs.getUUID( CommonColumns.TIME_ID.cql() ) ) );
    }

    @Override
    protected LoomEdge mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        EdgeKey key = new EdgeKey(
                row.getUUID( CommonColumns.SRC_VERTEX_ID.cql() ),
                row.getUUID( CommonColumns.DST_VERTEX_ID.cql() ),
                row.getUUID( CommonColumns.TIME_ID.cql() ) );

        EdgeLabel label = new EdgeLabel(
                new EntityKey(
                        row.getUUID( CommonColumns.EDGE_TYPE_ID.cql() ),
                        row.getString( CommonColumns.EDGE_ID.cql() ) ),
                row.getUUID( CommonColumns.SRC_VERTEX_TYPE_ID.cql() ),
                row.getUUID( CommonColumns.DST_VERTEX_TYPE_ID.cql() ) );

        return new LoomEdge( row.getUUID( CommonColumns.GRAPH_ID.cql() ), key, label );
    }

    @Override
    public GraphWrappedEdgeKey generateTestKey() {
        return new GraphWrappedEdgeKey( UUID.randomUUID(), new EdgeKey( UUID.randomUUID(), UUID.randomUUID() ) );
    }

    @Override
    public LoomEdge generateTestValue() {
        return new LoomEdge(
                UUID.randomUUID(),
                new EdgeKey( UUID.randomUUID(), UUID.randomUUID() ),
                new EdgeLabel( new EntityKey( UUID.randomUUID(), "entityId" ), UUID.randomUUID(), UUID.randomUUID() ) );
    }

}
