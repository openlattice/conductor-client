package com.dataloom.organizations.mapstores;

import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;

import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.rhizome.cassandra.ColumnDef;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;

public class StringSetMapstore extends UUIDKeyMapstore<DelegatedStringSet> {
    private final ColumnDef valueCol;

    public StringSetMapstore(
            HazelcastMap map,
            Session session,
            Tables table,
            ColumnDef keyCol,
            ColumnDef valueCol ) {
        super( map, session, table, keyCol );
        this.valueCol = valueCol;
    }

    @Override
    public DelegatedStringSet generateTestValue() {
        return DelegatedStringSet.wrap( ImmutableSet.of( RandomStringUtils.random( 10 ),
                RandomStringUtils.random( 10 ),
                RandomStringUtils.random( 10 ) ) );
    }

    @Override
    protected BoundStatement bind( UUID key, DelegatedStringSet value, BoundStatement bs ) {
        return bs
                .setUUID( keyCol.cql(), key )
                .setSet( valueCol.cql(), value, String.class );
    }

    @Override
    protected DelegatedStringSet mapValue( ResultSet rs ) {
        Row r = rs.one();
        return r == null ? null : DelegatedStringSet.wrap( r.getSet( valueCol.cql(), String.class ) );
    }

}
