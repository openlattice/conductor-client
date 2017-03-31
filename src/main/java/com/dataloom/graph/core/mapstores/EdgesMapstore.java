package com.dataloom.graph.core.mapstores;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeLabel;
import com.dataloom.graph.core.objects.GraphWrappedEdgeKey;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class EdgesMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<GraphWrappedEdgeKey, LoomEdge> {

    public EdgesMapstore( Session session ) {
        super( HazelcastMap.EDGES.name(), session, Table.EDGES.getBuilder() );
    }

    @Override
    protected BoundStatement bind( GraphWrappedEdgeKey key, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
                .setUUID( CommonColumns.SRC_VERTEX_ID.cql(), key.getEdgeKey().getSrcId() )
                .setUUID( CommonColumns.DST_VERTEX_ID.cql(), key.getEdgeKey().getDstId() )
                .setUUID( CommonColumns.TIME_UUID.cql(), key.getEdgeKey().getTimeId() );
    }

    @Override
    protected BoundStatement bind( GraphWrappedEdgeKey key, LoomEdge value, BoundStatement bs ) {
        return bs.bind( key, bs )
                .setUUID( CommonColumns.SRC_VERTEX_TYPE_ID.cql(), value.getLabel().getSrcType() )
                .setUUID( CommonColumns.DST_VERTEX_TYPE_ID.cql(), value.getLabel().getDstType() )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), value.getLabel().getReference().getEntitySetId() )
                .setString( CommonColumns.EDGE_ENTITYID.cql(), value.getLabel().getReference().getEntityId() );
    }

    @Override
    protected GraphWrappedEdgeKey mapKey( Row row ) {
        return RowAdapters.graphWrappedEdgeKey( row );
    }

    @Override
    protected LoomEdge mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.loomEdge( row );
    }

    @Override
    public GraphWrappedEdgeKey generateTestKey() {
        return new GraphWrappedEdgeKey( UUID.randomUUID(), generateTestEdgeKey() );
    }

    @Override
    public LoomEdge generateTestValue() {
        return new LoomEdge( UUID.randomUUID(), generateTestEdgeKey(), generateTestEdgeLabel() );
    }

    public static EdgeKey generateTestEdgeKey() {
        return new EdgeKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    public static EdgeLabel generateTestEdgeLabel() {
        return new EdgeLabel( TestDataFactory.entityKey(), UUID.randomUUID(), UUID.randomUUID() );
    }

}
