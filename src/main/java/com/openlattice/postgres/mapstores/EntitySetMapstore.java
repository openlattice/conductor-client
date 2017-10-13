package com.openlattice.postgres.mapstores;

import com.dataloom.edm.EntitySet;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.*;

public class EntitySetMapstore extends AbstractBasePostgresMapstore<UUID, EntitySet> {

    public EntitySetMapstore(
            String mapName,
            PostgresTableDefinition table,
            HikariDataSource hds ) {
        super( mapName, table, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( NAME, ENTITY_TYPE_ID, TITLE, DESCRIPTION, CONTACTS );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, EntitySet value ) throws SQLException {
        ps.setObject( 1, key );
        ps.setString( 2, value.getName() );
        ps.setObject( 3, value.getEntityTypeId() );
        ps.setString( 4, value.getTitle() );
        ps.setString( 5, value.getDescription() );

        Array contacts = PostgresArrays.createTextArray( ps.getConnection(), value.getContacts().stream() );
        ps.setArray( 6, contacts );

        // UPDATE
        ps.setString( 7, value.getName() );
        ps.setObject( 8, value.getEntityTypeId() );
        ps.setString( 9, value.getTitle() );
        ps.setString( 10, value.getDescription() );
        ps.setArray( 11, contacts );
    }

    @Override protected void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }

    @Override protected EntitySet mapToValue( ResultSet rs ) throws SQLException {
        UUID id = mapToKey( rs );
        String name = rs.getString( NAME.getName() );
        UUID entityTypeId = rs.getObject( ENTITY_TYPE_ID.getName(), UUID.class );
        String title = rs.getString( TITLE.getName() );
        String description = rs.getString( DESCRIPTION.getName() );
        String[] contacts = (String[]) rs.getArray( CONTACTS.getName() ).getArray();
        return new EntitySet( id, entityTypeId, name, title, Optional.of( description ), Sets.newHashSet( contacts ) );
    }

    @Override protected UUID mapToKey( ResultSet rs ) {
        try {
            return rs.getObject( ID.getName(), UUID.class );
        } catch ( SQLException ex ) {
            logger.error( "Unable to map ID to UUID class", ex );
            return null;
        }
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public EntitySet generateTestValue() {
        return TestDataFactory.entitySet();
    }
}
