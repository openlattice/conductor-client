/*
 * Copyright (C) 2017. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresColumn.ANALYZER;
import static com.openlattice.postgres.PostgresColumn.DATATYPE;
import static com.openlattice.postgres.PostgresColumn.DESCRIPTION;
import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.NAME;
import static com.openlattice.postgres.PostgresColumn.NAMESPACE;
import static com.openlattice.postgres.PostgresColumn.PII;
import static com.openlattice.postgres.PostgresColumn.SCHEMAS;
import static com.openlattice.postgres.PostgresColumn.TITLE;

import com.dataloom.edm.type.Analyzer;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class PropertyTypeMapstore extends AbstractBasePostgresMapstore<UUID, PropertyType> {

    public PropertyTypeMapstore(
            String mapName,
            PostgresTableDefinition table,
            HikariDataSource hds ) {
        super( mapName, table, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( PostgresColumn.ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( NAMESPACE, NAME, DATATYPE, TITLE, DESCRIPTION, SCHEMAS, PII, ANALYZER );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, PropertyType value ) throws SQLException {
        ps.setObject( 1, key );

        FullQualifiedName fqn = value.getType();
        ps.setString( 2, fqn.getNamespace() );
        ps.setString( 3, fqn.getName() );

        ps.setString( 4, value.getDatatype().name() );
        ps.setString( 5, value.getTitle() );
        ps.setString( 6, value.getDescription() );

        Array schemas = PostgresArrays.createTextArray(
                ps.getConnection(),
                value.getSchemas().stream().map( FullQualifiedName::getFullQualifiedNameAsString ) );

        ps.setArray( 7, schemas );
        ps.setBoolean( 8, value.isPIIfield() );
        ps.setString( 9, value.getAnalyzer().name() );

        //UPDATE
        ps.setString( 10, fqn.getNamespace() );
        ps.setString( 11, fqn.getName() );

        ps.setString( 12, value.getDatatype().name() );
        ps.setString( 13, value.getTitle() );
        ps.setString( 14, value.getDescription() );

        ps.setArray( 15, schemas );
        ps.setBoolean( 16, value.isPIIfield() );
        ps.setString( 17, value.getAnalyzer().name() );
    }

    @Override protected void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }

    @Override protected PropertyType mapToValue( java.sql.ResultSet rs ) throws SQLException {
        UUID id = mapToKey( rs );
        String namespace = rs.getString( NAMESPACE.getName() );
        String name = rs.getString( NAME.getName() );
        EdmPrimitiveTypeKind datatype = EdmPrimitiveTypeKind.valueOf( rs.getString( DATATYPE.getName() ) );
        FullQualifiedName fqn = new FullQualifiedName( namespace, name );
        String title = rs.getString( TITLE.getName() );
        Optional<String> description = Optional.fromNullable( rs.getString( DESCRIPTION.getName() ) );
        String[] schemasFqns = (String[]) rs.getArray( SCHEMAS.getName() ).getArray();
        boolean pii = rs.getBoolean( PII.getName() );
        Analyzer analyzer = Analyzer.valueOf( rs.getString( ANALYZER.getName() ) );

        return new PropertyType( id,
                fqn,
                title,
                description,
                Arrays.asList( schemasFqns ).stream().map( FullQualifiedName::new ).collect( Collectors.toSet() ),
                datatype,
                Optional.of( pii ),
                Optional.of( analyzer )
        );
    }

    @Override protected UUID mapToKey( java.sql.ResultSet rs ) {
        try {
            return rs.getObject( ID.getName(), UUID.class );
        } catch ( SQLException ex ) {
            logger.error( "Unable to map ID to UUID class", ex );
            return null;
        }
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public PropertyType generateTestValue() {
        return TestDataFactory.propertyType();
    }
}
