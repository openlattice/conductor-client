package com.openlattice.postgres.mapstores;

import com.dataloom.edm.type.ComplexType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresTable.COMPLEX_TYPES;

public class ComplexTypeMapstore extends AbstractBasePostgresMapstore<UUID, ComplexType> {

    public ComplexTypeMapstore( HikariDataSource hds ) {
        super( HazelcastMap.COMPLEX_TYPES.name(), COMPLEX_TYPES, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( NAMESPACE, NAME, TITLE, DESCRIPTION, PROPERTIES, BASE_TYPE, SCHEMAS, CATEGORY );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, ComplexType value ) throws SQLException {
        ps.setObject( 1, key );

        FullQualifiedName fqn = value.getType();
        ps.setString( 2, fqn.getNamespace() );
        ps.setString( 3, fqn.getName() );

        ps.setString( 4, value.getTitle() );
        ps.setString( 5, value.getDescription() );

        Array properties = PostgresArrays.createUuidArray( ps.getConnection(), value.getProperties().stream() );
        ps.setArray( 6, properties );

        ps.setObject( 7, value.getBaseType().orNull() );

        Array schemas = PostgresArrays.createTextArray(
                ps.getConnection(),
                value.getSchemas().stream().map( FullQualifiedName::getFullQualifiedNameAsString ) );

        ps.setArray( 8, schemas );
        ps.setString( 9, value.getCategory().name() );

        // UPDATE
        ps.setString( 10, fqn.getNamespace() );
        ps.setString( 11, fqn.getName() );
        ps.setString( 12, value.getTitle() );
        ps.setString( 13, value.getDescription() );
        ps.setArray( 14, properties );
        ps.setObject( 15, value.getBaseType().orNull() );
        ps.setArray( 16, schemas );
        ps.setString( 17, value.getCategory().name() );
    }

    @Override protected void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }

    @Override protected ComplexType mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.complexType( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) {
        try {
            return ResultSetAdapters.id( rs );
        } catch ( SQLException e ) {
            logger.debug( "Unable to map ID to UUID", e );
            return null;
        }
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public ComplexType generateTestValue() {
        return TestDataFactory.complexType();
    }
}
