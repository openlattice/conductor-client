package com.openlattice.postgres.mapstores;

import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.NAME;
import static com.openlattice.postgres.PostgresColumn.SECURABLE_OBJECTID;

public class NamesMapstore extends AbstractBasePostgresMapstore<UUID, String> {

    public NamesMapstore(
            String mapName,
            PostgresTableDefinition table,
            HikariDataSource hds ) {
        super( mapName, table, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( PostgresColumn.SECURABLE_OBJECTID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( PostgresColumn.NAME );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, String value ) throws SQLException {
        ps.setObject( 1, key );
        ps.setString( 2, value );

        // UPDATE
        ps.setString( 3, value );
    }

    @Override protected void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }

    @Override protected String mapToValue( ResultSet rs ) throws SQLException {
        return rs.getString( NAME.getName() );
    }

    @Override protected UUID mapToKey( ResultSet rs ) {
        try {
            return rs.getObject( SECURABLE_OBJECTID.getName(), UUID.class );
        } catch ( SQLException ex ) {
            logger.error( "Unable to map SECURABLE_OBJECTID column", ex );
            return null;
        }
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public String generateTestValue() {
        return "testValue";
    }
}
