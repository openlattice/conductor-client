package com.dataloom.edm.schemas.mapstores;

import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class SchemaMapstore extends AbstractStructuredCassandraMapstore<String, Set<String>> {
    private static final CassandraTableBuilder ctb = Tables.SCHEMAS.getBuilder();

    public SchemaMapstore( Session session ) {
        super( HazelcastMap.SCHEMAS.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( String key, BoundStatement bs ) {
        return bs.setString( CommonColumns.NAMESPACE.cql(), key );
    }

    @Override
    protected BoundStatement bind( String key, Set<String> value, BoundStatement bs ) {
        return bs.setString( CommonColumns.NAMESPACE.cql(), key )
                .setSet( CommonColumns.NAME_SET.cql(), value, String.class );
    }

    @Override
    protected String mapKey( Row rs ) {
        return rs == null ? null : rs.getString( CommonColumns.NAMESPACE.cql() );
    }

    @Override
    protected Set<String> mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return row.getSet( CommonColumns.NAME_SET.cql(), String.class );
    }

    @Override
    public String generateTestKey() {
        return RandomStringUtils.randomAlphanumeric( 5 );
    }

    @Override
    public Set<String> generateTestValue() {
        return ImmutableSet.of( RandomStringUtils.randomAlphanumeric( 5 ), RandomStringUtils.randomAlphanumeric( 5 ) );
    }

}
