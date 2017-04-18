package com.dataloom.graph.core.mapstores;

import java.util.UUID;

import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.core.objects.LoomEdgeKey;
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

public class EdgesMapstore extends AbstractStructuredCassandraMapstore<EdgeKey, LoomEdgeKey> {

    private static EdgeKey     testKey   = generateTestEdgeKey();
    private static LoomEdgeKey testValue = new LoomEdgeKey(
            testKey,
            UUID.randomUUID(),
            UUID.randomUUID() );

    public EdgesMapstore( Session session ) {
        super( HazelcastMap.EDGES.name(), session, Table.EDGES.getBuilder() );
    }

    @Override
    protected BoundStatement bind( EdgeKey key, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), key.getSrcId() )
                .setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), key.getDstId() )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), key.getReference().getEntitySetId() )
                .setString( CommonColumns.EDGE_ENTITYID.cql(), key.getReference().getEntityId() )
                .setUUID( CommonColumns.SYNCID.cql(), key.getReference().getSyncId() );
    }

    @Override
    protected BoundStatement bind( EdgeKey key, LoomEdgeKey value, BoundStatement bs ) {
        return bind( key, bs )
                .setUUID( CommonColumns.SRC_VERTEX_TYPE_ID.cql(), value.getSrcType() )
                .setUUID( CommonColumns.DST_TYPE_ID.cql(), value.getDstType() );
    }

    @Override
    protected EdgeKey mapKey( Row row ) {
        return RowAdapters.edgeKey( row );
    }

    @Override
    protected LoomEdgeKey mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.loomEdge( row );
    }

    @Override
    public EdgeKey generateTestKey() {
        return testKey;
    }

    @Override
    public LoomEdgeKey generateTestValue() {
        return testValue;
    }

    public static EdgeKey generateTestEdgeKey() {
        return new EdgeKey( UUID.randomUUID(), UUID.randomUUID(), TestDataFactory.entityKey() );
    }

}
