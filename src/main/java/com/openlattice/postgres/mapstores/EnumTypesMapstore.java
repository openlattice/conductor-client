package com.openlattice.postgres.mapstores;

import com.dataloom.edm.type.Analyzer;
import com.dataloom.edm.type.EnumType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresTable.ENUM_TYPES;

public class EnumTypesMapstore extends AbstractBasePostgresMapstore<UUID, EnumType> {

    public EnumTypesMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ENUM_TYPES.name(), ENUM_TYPES, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of(
                NAMESPACE,
                NAME,
                TITLE,
                DESCRIPTION,
                MEMBERS,
                SCHEMAS,
                DATATYPE,
                FLAGS,
                PII,
                ANALYZER );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, EnumType value ) throws SQLException {
        ps.setObject( 1, key );

        FullQualifiedName fqn = value.getType();
        ps.setString( 2, fqn.getNamespace() );
        ps.setString( 3, fqn.getName() );

        ps.setString( 4, value.getTitle() );
        ps.setString( 5, value.getDescription() );

        Array members = PostgresArrays.createTextArray( ps.getConnection(), value.getMembers().stream() );
        ps.setArray( 6, members );

        Array schemas = PostgresArrays.createTextArray(
                ps.getConnection(),
                value.getSchemas().stream().map( FullQualifiedName::getFullQualifiedNameAsString ) );
        ps.setArray( 7, schemas );

        ps.setString( 8, value.getDatatype().name() );
        ps.setBoolean( 9, value.isFlags() );
        ps.setBoolean( 10, value.isPIIfield() );
        ps.setString( 11, value.getAnalyzer().name() );

        // UPDATE
        ps.setString( 12, fqn.getNamespace() );
        ps.setString( 13, fqn.getName() );
        ps.setString( 14, value.getTitle() );
        ps.setString( 15, value.getDescription() );
        ps.setArray( 16, members );
        ps.setArray( 17, schemas );
        ps.setString( 18, value.getDatatype().name() );
        ps.setBoolean( 19, value.isFlags() );
        ps.setBoolean( 20, value.isPIIfield() );
        ps.setString( 21, value.getAnalyzer().name() );
    }

    @Override protected void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }

    @Override protected EnumType mapToValue( ResultSet rs ) throws SQLException {
        Optional<UUID> id = Optional.of( mapToKey( rs ) );

        String namespace = rs.getString( NAMESPACE.getName() );
        String name = rs.getString( NAME.getName() );
        FullQualifiedName fqn = new FullQualifiedName( namespace, name );

        String title = rs.getString( TITLE.getName() );
        Optional<String> description = Optional.fromNullable( rs.getString( DESCRIPTION.getName() ) );
        LinkedHashSet<String> members = Arrays.stream( (String[]) rs.getArray( MEMBERS.getName() ).getArray() )
                .collect( Collectors
                        .toCollection( LinkedHashSet::new ) );
        Set<FullQualifiedName> schemasFqns = Arrays.stream( (String[]) rs.getArray( SCHEMAS.getName() ).getArray() )
                .map( FullQualifiedName::new ).collect( Collectors.toSet() );

        String datatypeStr = rs.getString( DATATYPE.getName() );
        Optional<EdmPrimitiveTypeKind> datatype = ( datatypeStr == null ) ?
                Optional.absent() :
                Optional.fromNullable( EdmPrimitiveTypeKind.valueOf( datatypeStr ) );
        boolean flags = rs.getBoolean( FLAGS.getName() );
        Optional<Boolean> pii = Optional.fromNullable( rs.getBoolean( PII.getName() ) );
        Optional<Analyzer> analyzer = Optional.fromNullable( Analyzer.valueOf( rs.getString( ANALYZER.getName() ) ) );

        return new EnumType( id, fqn, title, description, members, schemasFqns, datatype, flags, pii, analyzer );
    }

    @Override protected UUID mapToKey( ResultSet rs ) {
        try {
            return rs.getObject( ID.getName(), UUID.class );
        } catch ( SQLException e ) {
            logger.debug( "Unable to map ID to UUID", e );
            return null;
        }
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public EnumType generateTestValue() {
        return TestDataFactory.enumType();
    }
}
