package com.dataloom.edm.mapstores;

import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class FqnsMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, FullQualifiedName> {
    private static final CassandraTableBuilder ctb = Tables.FQNS.getBuilder();

    public FqnsMapstore( Session session ) {
        super( HazelcastMap.FQNS.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.SECURABLE_OBJECTID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, FullQualifiedName value, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.SECURABLE_OBJECTID.cql(), key )
                .set( CommonColumns.FQN.cql(), value, FullQualifiedName.class );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.SECURABLE_OBJECTID.cql() );
    }

    @Override
    protected FullQualifiedName mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public FullQualifiedName generateTestValue() {
        return TestDataFactory.fqn();
    }

}
