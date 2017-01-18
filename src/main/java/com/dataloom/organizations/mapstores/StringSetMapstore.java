package com.dataloom.organizations.mapstores;

import java.util.Set;
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

public class StringSetMapstore extends UUIDKeyMapstore<Set<String>> {
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
    public Set<String> generateTestValue() {
        return ImmutableSet.of( RandomStringUtils.random( 10 ),
                RandomStringUtils.random( 10 ),
                RandomStringUtils.random( 10 ) );
    }

    @Override
    protected BoundStatement bind( UUID key, Set<String> value, BoundStatement bs ) {
        return bs
                .setUUID( keyCol.cql(), key )
                .setSet( valueCol.cql(), value, String.class );
    }

    @Override
    protected Set<String> mapValue( ResultSet rs ) {
        Row r = rs.one();
        return r == null ? null : r.getSet( valueCol.cql(), String.class );
    }

}
