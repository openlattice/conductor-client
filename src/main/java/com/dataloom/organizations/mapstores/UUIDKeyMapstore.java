package com.dataloom.organizations.mapstores;

import java.util.UUID;

import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.rhizome.cassandra.ColumnDef;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public abstract class UUIDKeyMapstore<V> extends AbstractStructuredCassandraMapstore<UUID, V> {
    protected final ColumnDef keyCol;

    protected UUIDKeyMapstore( HazelcastMap map, Session session, Tables table, ColumnDef keyCol ) {
        super( map.name(), session, table.getBuilder() );
        this.keyCol = keyCol;
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs
                .setUUID( keyCol.cql(), key );
    }

    @Override
    protected UUID mapKey( Row row ) {
        return row.getUUID( keyCol.cql() );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }
}
