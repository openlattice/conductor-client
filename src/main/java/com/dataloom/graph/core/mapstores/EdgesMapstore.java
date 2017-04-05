package com.dataloom.graph.core.mapstores;

import java.util.UUID;

import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.LoomEdge;
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

public class EdgesMapstore extends AbstractStructuredCassandraMapstore<EdgeKey, LoomEdge> {

    private static EdgeKey  testKey   = generateTestEdgeKey();
    private static LoomEdge testValue = new LoomEdge(
            testKey,
            TestDataFactory.entityKey(),
            UUID.randomUUID(),
            UUID.randomUUID() );

    public EdgesMapstore( Session session ) {
        super( HazelcastMap.EDGES.name(), session, Table.EDGES.getBuilder() );
    }

    @Override
    protected BoundStatement bind( EdgeKey key, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.SRC_VERTEX_ID.cql(), key.getSrcId() )
                .setUUID( CommonColumns.DST_VERTEX_ID.cql(), key.getDstId() )
                .setUUID( CommonColumns.SYNCID.cql(), key.getSyncId() );
    }

    @Override
    protected BoundStatement bind( EdgeKey key, LoomEdge value, BoundStatement bs ) {
        return bind( key, bs )
                .setUUID( CommonColumns.SRC_VERTEX_TYPE_ID.cql(), value.getSrcType() )
                .setUUID( CommonColumns.DST_VERTEX_TYPE_ID.cql(), value.getDstType() )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), value.getReference().getEntitySetId() )
                .setString( CommonColumns.EDGE_ENTITYID.cql(), value.getReference().getEntityId() );
    }

    @Override
    protected EdgeKey mapKey( Row row ) {
        return RowAdapters.edgeKey( row );
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
    public EdgeKey generateTestKey() {
        return testKey;
    }

    @Override
    public LoomEdge generateTestValue() {
        return testValue;
    }

    public static EdgeKey generateTestEdgeKey() {
        return new EdgeKey( UUID.randomUUID(), UUID.randomUUID() );
    }

}
