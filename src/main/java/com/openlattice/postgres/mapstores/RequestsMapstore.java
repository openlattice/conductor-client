package com.openlattice.postgres.mapstores;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.requests.Request;
import com.dataloom.requests.RequestStatus;
import com.dataloom.requests.Status;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import static com.openlattice.postgres.PostgresArrays.createTextArray;
import static com.openlattice.postgres.PostgresArrays.createUuidArray;
import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresTable.REQUESTS;

public class RequestsMapstore extends AbstractBasePostgresMapstore<AceKey, Status> {

    public RequestsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.REQUESTS.name(), REQUESTS, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( PostgresColumn.PERMISSIONS, REASON, STATUS );
    }

    @Override protected void bind( PreparedStatement ps, AceKey key, Status value ) throws SQLException {
        bind( ps, key );

        Array permissions = createTextArray( ps.getConnection(),
                value.getRequest().getPermissions().stream().map( Permission::name ) );
        ps.setArray( 4, permissions );
        ps.setString( 5, value.getRequest().getReason() );
        ps.setString( 6, value.getStatus().name() );

        // UPDATE
        ps.setArray( 7, permissions );
        ps.setString( 8, value.getRequest().getReason() );
        ps.setString( 9, value.getStatus().name() );
    }

    @Override protected void bind( PreparedStatement ps, AceKey key ) throws SQLException {
        Principal p = key.getPrincipal();
        ps.setArray( 1, createUuidArray( ps.getConnection(), key.getKey().stream() ) );
        ps.setString( 2, p.getType().name() );
        ps.setString( 3, p.getId() );
    }

    @Override protected Status mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.status( rs );
    }

    @Override protected AceKey mapToKey( ResultSet rs ) {
        try {
            return ResultSetAdapters.aceKey( rs );
        } catch ( SQLException e ) {
            logger.debug( "Unable to map row to AceKey", e );
            return null;
        }
    }

    @Override public AceKey generateTestKey() {
        return new AceKey( ImmutableList.of( UUID.randomUUID(), UUID.randomUUID() ), TestDataFactory.userPrincipal() );
    }

    @Override public Status generateTestValue() {
        return TestDataFactory.status();
    }
}
