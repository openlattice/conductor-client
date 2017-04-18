package com.dataloom.graph.core.mapstores;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.LoomVertexKey;
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

public class VerticesMapstore extends AbstractStructuredCassandraMapstore<UUID, LoomVertexKey> {
    private static LoomVertexKey testValue = new LoomVertexKey(
            UUID.randomUUID(),
            TestDataFactory.entityKey() );
    private static UUID          testKey   = testValue.getKey();

    public VerticesMapstore( Session session ) {
        super( HazelcastMap.VERTICES.name(), session, Table.VERTICES.getBuilder() );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.VERTEX_ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, LoomVertexKey value, BoundStatement bs ) {
        return bind( key, bs ).set( CommonColumns.ENTITY_KEY.cql(), value.getReference(), EntityKey.class );
    }

    @Override
    protected UUID mapKey( Row row ) {
        return RowAdapters.vertexId( row );
    }

    @Override
    protected LoomVertexKey mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.loomVertex( row );
    }

    @Override
    public UUID generateTestKey() {
        return testKey;
    }

    @Override
    public LoomVertexKey generateTestValue() {
        return testValue;
    }

}
