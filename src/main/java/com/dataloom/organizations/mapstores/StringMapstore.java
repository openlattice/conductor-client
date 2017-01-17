package com.dataloom.organizations.mapstores;

import java.util.UUID;

import com.auth0.jwt.internal.org.apache.commons.lang3.RandomStringUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.rhizome.cassandra.ColumnDef;

public class StringMapstore extends UUIDKeyMapstore<String> {
    private final ColumnDef valueCol;

    public StringMapstore( HazelcastMap map, Session session, Tables table, ColumnDef keyCol, ColumnDef valueCol ) {
        super( map, session, table, keyCol );
        this.valueCol = valueCol;
    }

    @Override
    public String generateTestValue() {
        return RandomStringUtils.random( 10 );
    }

    @Override
    protected BoundStatement bind( UUID key, String value, BoundStatement bs ) {
        return bs
                .setUUID( keyCol.cql(), key )
                .setString( valueCol.cql(), value );
    }

    @Override
    protected String mapValue( ResultSet rs ) {
        Row r = rs.one();
        return r == null ? null : r.getString( valueCol.cql() );
    }
}
