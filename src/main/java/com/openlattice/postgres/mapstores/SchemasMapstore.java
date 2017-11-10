package com.openlattice.postgres.mapstores;

import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.RandomStringUtils;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.openlattice.postgres.PostgresColumn.NAMESPACE;
import static com.openlattice.postgres.PostgresColumn.NAME_SET;
import static com.openlattice.postgres.PostgresTable.SCHEMA;

public class SchemasMapstore extends AbstractBasePostgresMapstore<String, DelegatedStringSet> {

    public SchemasMapstore( HikariDataSource hds ) {
        super( HazelcastMap.SCHEMAS.name(), SCHEMA, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( NAMESPACE );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( NAME_SET );
    }

    @Override protected void bind(
            PreparedStatement ps, String key, DelegatedStringSet value ) throws SQLException {
        ps.setString( 1, key );

        Array names = PostgresArrays.createTextArray( ps.getConnection(), value.stream() );
        ps.setArray( 2, names );

        // UPDATE
        ps.setArray( 3, names );
    }

    @Override protected void bind( PreparedStatement ps, String key ) throws SQLException {
        ps.setString( 1, key );
    }

    @Override protected DelegatedStringSet mapToValue( ResultSet rs ) throws SQLException {
        return DelegatedStringSet.wrap( Sets.newHashSet( (String[]) rs.getArray( NAME_SET.getName() ).getArray() ) );
    }

    @Override protected String mapToKey( ResultSet rs ) {
        try {
            return ResultSetAdapters.namespace( rs );
        } catch ( SQLException e ) {
            logger.debug( "Unable to map schema names.", e );
            return null;
        }
    }

    @Override public String generateTestKey() {
        return RandomStringUtils.randomAlphanumeric( 5 );
    }

    @Override public DelegatedStringSet generateTestValue() {
        return DelegatedStringSet.wrap( ImmutableSet.of( RandomStringUtils.randomAlphanumeric( 5 ),
                RandomStringUtils.randomAlphanumeric( 5 ),
                RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }
}
