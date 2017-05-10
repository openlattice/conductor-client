package com.dataloom.linking.mapstores;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.math.RandomUtils;

import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.LinkingVertex;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class VertexIdsAfterLinkingMapstore extends AbstractStructuredCassandraMapstore<LinkingVertexKey, UUID> {
    public VertexIdsAfterLinkingMapstore( Session session ) {
        super( HazelcastMap.VERTEX_IDS_AFTER_LINKING.name(), session, Table.VERTEX_IDS_AFTER_LINKING.getBuilder() );
    }

    @Override
    protected BoundStatement bind( LinkingVertexKey key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.VERTEX_ID.cql(), key.getVertexId() )
                .setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() );
    }

    @Override
    protected BoundStatement bind( LinkingVertexKey key, UUID value, BoundStatement bs ) {
        return bind( key, bs ).setUUID( CommonColumns.NEW_VERTEX_ID.cql(), value );
    }

    @Override
    protected LinkingVertexKey mapKey( Row row ) {
        if ( row == null ) {
            return null;
        }
        UUID graphId = row.getUUID( CommonColumns.GRAPH_ID.cql() );
        UUID vertexId = row.getUUID( CommonColumns.VERTEX_ID.cql() );
        return new LinkingVertexKey( graphId, vertexId );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return row.getUUID( CommonColumns.NEW_VERTEX_ID.cql() );
    }

    @Override
    public Iterable<LinkingVertexKey> loadAllKeys() {
        //lazy loading
        return null;
    }
    
    @Override
    public LinkingVertexKey generateTestKey() {
        return new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override
    public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}
