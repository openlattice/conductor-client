package com.dataloom.organizations.mapstores;

import java.util.UUID;

import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.rhizome.cassandra.ColumnDef;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDSet;

public class UUIDSetMapstore extends UUIDKeyMapstore<DelegatedUUIDSet> {
    private final ColumnDef valueCol;

    public UUIDSetMapstore(
            HazelcastMap map,
            Session session,
            Tables table,
            ColumnDef keyCol,
            ColumnDef valueCol ) {
        super( map, session, table, keyCol );
        this.valueCol = valueCol;
    }

    @Override
    public DelegatedUUIDSet generateTestValue() {
        return DelegatedUUIDSet.wrap( ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) );
    }

    @Override
    protected BoundStatement bind( UUID key, DelegatedUUIDSet value, BoundStatement bs ) {
        return bs
                .setUUID( keyCol.cql(), key )
                .setSet( valueCol.cql(), value, UUID.class );
    }

    @Override
    protected DelegatedUUIDSet mapValue( ResultSet rs ) {
        Row r = rs.one();
        return r == null ? null : DelegatedUUIDSet.wrap( r.getSet( valueCol.cql(), UUID.class ) );
    }

}
