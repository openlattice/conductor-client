package com.openlattice.postgres.mapstores;

import com.auth0.jwt.internal.org.apache.commons.lang3.RandomStringUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.EDM_VERSION;
import static com.openlattice.postgres.PostgresColumn.EDM_VERSION_NAME;
import static com.openlattice.postgres.PostgresTable.EDM_VERSIONS;

public class EdmVersionsMapstore extends AbstractBasePostgresMapstore<String, UUID> {
    private final HikariDataSource hds;
    private final String           loadQuerySql;
    private final String           insertQuerySql;

    public EdmVersionsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.EDM_VERSIONS.name(), EDM_VERSIONS, hds );
        this.hds = hds;
        this.loadQuerySql = "SELECT * FROM ".concat( EDM_VERSIONS.getName() ).concat( " WHERE " )
                .concat( EDM_VERSION_NAME.getName() ).concat( " = ? ORDER BY " ).concat( EDM_VERSION.getName() )
                .concat( " DESC LIMIT 1;" );
        this.insertQuerySql = EDM_VERSIONS.insertQuery( Optional.empty(), ImmutableList.of() );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( EDM_VERSION_NAME );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( EDM_VERSION );
    }

    @Override protected void bind( PreparedStatement ps, String key, UUID value ) throws SQLException {
        ps.setString( 1, key );
        ps.setObject( 2, value );
    }

    @Override protected void bind( PreparedStatement ps, String key ) throws SQLException {
        ps.setString( 1, key );
    }

    @Override protected UUID mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.edmVersion( rs );
    }

    @Override protected String mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.edmVersionName( rs );
    }

    @Override
    public UUID load( String key ) {
        UUID val = null;
        try ( Connection connection = hds.getConnection();
                PreparedStatement selectRow = connection.prepareStatement( loadQuerySql ); ) {
            bind( selectRow, key );
            ResultSet rs = selectRow.executeQuery();
            if ( rs.next() ) {
                val = mapToValue( rs );
            }
            rs.close();
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during select for key {}.", key, e );
        }
        return val;
    }

    @Override
    public void store( String key, UUID value ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement insertRow = connection.prepareStatement( insertQuerySql ) ) {
            bind( insertRow, key, value );
            logger.info( insertRow.toString() );
            insertRow.execute();
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during store for key {}.", key, e );
        }
    }

    @Override public String generateTestKey() {
        return RandomStringUtils.random( 10 );
    }

    @Override public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}
