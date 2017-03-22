package com.dataloom.edm.mapstores;

import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.spark_project.guava.collect.Sets;

import com.dataloom.edm.type.EdgeType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class EdgeTypeMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, EdgeType> {
    private static final CassandraTableBuilder ctb = Table.EDGE_TYPES.getBuilder();

    public EdgeTypeMapstore( Session session ) {
        super( HazelcastMap.EDGE_TYPES.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, EdgeType value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key )
                .setSet( CommonColumns.SRC.cql(), value.getSrc(), UUID.class )
                .setSet( CommonColumns.DEST.cql(), value.getDest(), UUID.class )
                .setBool( CommonColumns.BIDIRECTIONAL.cql(), value.isBidirectional() );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.ID.cql() );
    }

    @Override
    protected EdgeType mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.edgeType( row );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public EdgeType generateTestValue() {
        return TestDataFactory.edgeType();
    }

}
