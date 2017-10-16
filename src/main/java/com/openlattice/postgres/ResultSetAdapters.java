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

package com.openlattice.postgres;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.Analyzer;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.openlattice.postgres.PostgresArrays.getTextArray;
import static com.openlattice.postgres.PostgresColumn.*;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class ResultSetAdapters {
    private static final Logger logger = LoggerFactory.getLogger( ResultSetAdapters.class );

    public static EnumSet<Permission> permissions( ResultSet rs ) throws SQLException {
        String[] pStrArray = getTextArray( rs, PERMISSIONS_FIELD );

        if ( pStrArray.length == 0 ) {
            return EnumSet.noneOf( Permission.class );
        }

        EnumSet<Permission> permissions = EnumSet.noneOf( Permission.class );

        for ( String s : pStrArray ) {
            permissions.add( Permission.valueOf( s ) );
        }

        return permissions;
    }

    public static UUID id( ResultSet rs ) throws SQLException {
        return rs.getObject( ID.getName(), UUID.class );
    }

    public static String namespace( ResultSet rs ) throws SQLException {
        return rs.getString( NAMESPACE.getName() );
    }

    public static String name( ResultSet rs ) throws SQLException {
        return rs.getString( NAME.getName() );
    }

    public static String title( ResultSet rs ) throws SQLException {
        return rs.getString( TITLE.getName() );
    }

    public static String description( ResultSet rs ) throws SQLException {
        return rs.getString( DESCRIPTION.getName() );
    }

    public static Set<FullQualifiedName> schemas( ResultSet rs ) throws SQLException {
        return Arrays.stream( (String[]) rs.getArray( SCHEMAS.getName() ).getArray() ).map( FullQualifiedName::new )
                .collect( Collectors.toSet() );
    }

    public static EdmPrimitiveTypeKind datatype( ResultSet rs ) throws SQLException {
        return EdmPrimitiveTypeKind.valueOf( rs.getString( DATATYPE.getName() ) );
    }

    public static boolean pii( ResultSet rs ) throws SQLException {
        return rs.getBoolean( PII.getName() );
    }

    public static Analyzer analyzer( ResultSet rs ) throws SQLException {
        return Analyzer.valueOf( rs.getString( ANALYZER.getName() ) );
    }

    public static AceKey aceKey( ResultSet rs ) throws SQLException {
        List<UUID> aclKey = Arrays.asList( PostgresArrays.getUuidArray( rs, ACL_KEY_FIELD ) );
        PrincipalType principalType = PrincipalType.valueOf( rs.getString( PRINCIPAL_TYPE_FIELD ) );
        String principalId = rs.getString( PRINCIPAL_ID_FIELD );
        return new AceKey( aclKey, new Principal( principalType, principalId ) );
    }

    public static LinkedHashSet<UUID> key( ResultSet rs ) throws SQLException {
        return Arrays.stream( (UUID[]) rs.getArray( KEY.getName() ).getArray() )
                .collect( Collectors.toCollection( LinkedHashSet::new ) );
    }

    public static LinkedHashSet<UUID> properties( ResultSet rs ) throws SQLException {
        return Arrays.stream( (UUID[]) rs.getArray( PROPERTIES.getName() ).getArray() )
                .collect( Collectors.toCollection( LinkedHashSet::new ) );
    }

    public static UUID baseType( ResultSet rs ) throws SQLException {
        return rs.getObject( BASE_TYPE.getName(), UUID.class );
    }

    public static SecurableObjectType category( ResultSet rs ) throws SQLException {
        return SecurableObjectType.valueOf( rs.getString( CATEGORY.getName() ) );
    }

    public static UUID entityTypeId( ResultSet rs ) throws SQLException {
        return rs.getObject( ENTITY_TYPE_ID.getName(), UUID.class );
    }

    public static Set<String> contacts( ResultSet rs ) throws SQLException {
        return Sets.newHashSet( (String[]) rs.getArray( CONTACTS.getName() ).getArray() );
    }

    public static PropertyType propertyType( ResultSet rs ) throws SQLException {
        UUID id = id( rs );
        String namespace = namespace( rs );
        String name = name( rs );
        FullQualifiedName fqn = new FullQualifiedName( namespace, name );
        EdmPrimitiveTypeKind datatype = datatype( rs );
        String title = title( rs );
        Optional<String> description = Optional.fromNullable( description( rs ) );
        Set<FullQualifiedName> schemas = schemas( rs );
        Optional<Boolean> pii = Optional.fromNullable( pii( rs ) );
        Optional<Analyzer> analyzer = Optional.fromNullable( analyzer( rs ) );

        return new PropertyType( id, fqn, title, description, schemas, datatype, pii, analyzer );
    }

    public static EntityType entityType( ResultSet rs ) throws SQLException {
        UUID id = id( rs );
        String namespace = namespace( rs );
        String name = name( rs );
        FullQualifiedName fqn = new FullQualifiedName( namespace, name );
        String title = title( rs );
        Optional<String> description = Optional.fromNullable( description( rs ) );
        Set<FullQualifiedName> schemas = schemas( rs );
        LinkedHashSet<UUID> key = key( rs );
        LinkedHashSet<UUID> properties = properties( rs );
        Optional<UUID> baseType = Optional.fromNullable( baseType( rs ) );
        Optional<SecurableObjectType> category = Optional.of( category( rs ) );

        return new EntityType( id, fqn, title, description, schemas, key, properties, baseType, category );
    }

    public static EntitySet entitySet( ResultSet rs ) throws SQLException {
        UUID id = id( rs );
        String name = name( rs );
        UUID entityTypeId = entityTypeId( rs );
        String title = title( rs );
        Optional<String> description = Optional.fromNullable( description( rs ) );
        Set<String> contacts = contacts( rs );
        return new EntitySet( id, entityTypeId, name, title, description, contacts );
    }
}
