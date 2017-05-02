package com.dataloom.edm.mapstores;

import java.util.UUID;

import com.dataloom.edm.type.AssociationType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class AssociationTypeMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, AssociationType> {
    private static final CassandraTableBuilder ctb = Table.ASSOCIATION_TYPES.getBuilder();

    public AssociationTypeMapstore( Session session ) {
        super( HazelcastMap.ASSOCIATION_TYPES.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, AssociationType value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key )
                .setSet( CommonColumns.SRC.cql(), value.getSrc(), UUID.class )
                .setSet( CommonColumns.DST.cql(), value.getDst(), UUID.class )
                .setBool( CommonColumns.BIDIRECTIONAL.cql(), value.isBidirectional() );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.ID.cql() );
    }

    @Override
    protected AssociationType mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.associationType( row );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public AssociationType generateTestValue() {
        return TestDataFactory.associationType();
    }

}
