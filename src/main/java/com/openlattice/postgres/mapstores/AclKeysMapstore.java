package com.openlattice.postgres.mapstores;

import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.NAME;
import static com.openlattice.postgres.PostgresColumn.SECURABLE_OBJECTID;
import static com.openlattice.postgres.PostgresTable.ACL_KEYS;

public class AclKeysMapstore extends AbstractBasePostgresMapstore<String, UUID> {

    public AclKeysMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ACL_KEYS.name(), ACL_KEYS, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( PostgresColumn.NAME );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( PostgresColumn.SECURABLE_OBJECTID );
    }

    @Override protected void bind( PreparedStatement ps, String key, UUID value ) throws SQLException {
        ps.setString( 1, key );
        ps.setObject( 2, value );

        // UPDATE
        ps.setObject( 3, value );
    }

    @Override protected void bind( PreparedStatement ps, String key ) throws SQLException {
        ps.setString( 1, key );
    }

    @Override protected UUID mapToValue( ResultSet rs ) throws SQLException {
        return rs.getObject( SECURABLE_OBJECTID.getName(), UUID.class );
    }

    @Override protected String mapToKey( ResultSet rs ) throws SQLException {
        return rs.getString( NAME.getName() );
    }

    @Override public String generateTestKey() {
        return "testKey";
    }

    @Override public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}
