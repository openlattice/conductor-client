package com.openlattice.postgres.mapstores;

import com.dataloom.edm.type.AssociationType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresTable.ASSOCIATION_TYPES;

public class AssociationTypeMapstore extends AbstractBasePostgresMapstore<UUID, AssociationType> {

    public AssociationTypeMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ASSOCIATION_TYPES.name(), ASSOCIATION_TYPES, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( SRC, DST, BIDIRECTIONAL );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, AssociationType value ) throws SQLException {
        ps.setObject( 1, key );

        Array src = PostgresArrays.createUuidArray( ps.getConnection(), value.getSrc().stream() );
        Array dst = PostgresArrays.createUuidArray( ps.getConnection(), value.getDst().stream() );

        ps.setArray( 2, src );
        ps.setArray( 3, dst );
        ps.setBoolean( 4, value.isBidirectional() );

        // UPDATE

        ps.setArray( 5, src );
        ps.setArray( 6, dst );
        ps.setBoolean( 7, value.isBidirectional() );

    }

    @Override protected void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }

    @Override protected AssociationType mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.associationType( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public AssociationType generateTestValue() {
        return TestDataFactory.associationType();
    }
}
