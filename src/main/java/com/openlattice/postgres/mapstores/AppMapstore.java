package com.openlattice.postgres.mapstores;

import com.dataloom.apps.App;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.openlattice.postgres.PostgresColumn.*;

public class AppMapstore extends AbstractBasePostgresMapstore<UUID, App> {
    public AppMapstore( HikariDataSource hds ) {
        super( HazelcastMap.APPS.name(), PostgresTable.APPS, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( NAME, TITLE, DESCRIPTION, CONFIG_TYPE_IDS );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, App value ) throws SQLException {
        ps.setObject( 1, key );
        ps.setString( 2, value.getName() );
        ps.setString( 3, value.getTitle() );
        ps.setString( 4, value.getDescription() );
        Array configTypesArray = PostgresArrays
                .createUuidArray( ps.getConnection(), value.getAppTypeIds().stream() );
        ps.setArray( 5, configTypesArray );

        // UPDATE
        ps.setString( 6, value.getName() );
        ps.setString( 7, value.getTitle() );
        ps.setString( 8, value.getDescription() );
        ps.setArray( 9, configTypesArray );
    }

    @Override protected void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }

    @Override protected App mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.app( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public App generateTestValue() {
        LinkedHashSet<UUID> configIds = new LinkedHashSet<>();
        configIds.add( UUID.randomUUID() );
        return new App( UUID.randomUUID(), "name", "title", Optional.of( "description" ), configIds );
    }
}
