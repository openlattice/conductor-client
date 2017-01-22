package com.dataloom.edm.mapstores;

import java.util.UUID;

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

public class NamesMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, String> {
    private static final CassandraTableBuilder ctb = Tables.NAMES.getBuilder();

    public NamesMapstore( Session session ) {
        super( HazelcastMap.NAMES.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.SECURABLE_OBJECTID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, String value, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.SECURABLE_OBJECTID.cql(), key )
                .setString( CommonColumns.NAME.cql(), value );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.SECURABLE_OBJECTID.cql() );
    }

    @Override
    protected String mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : row.getString( CommonColumns.NAME.cql() );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public String generateTestValue() {
        return TestDataFactory.name();
    }

}
