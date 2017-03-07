/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
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
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.kryptnostic.datastore.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.data.EntityKey;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.requests.RequestStatus;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.utils.Bytes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.reflect.TypeToken;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;

public final class RowAdapters {
    private static final Logger logger = LoggerFactory.getLogger( RowAdapters.class );

    private RowAdapters() {}

    private static final ProtocolVersion protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;

    public static SetMultimap<FullQualifiedName, Object> entity(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            ObjectMapper mapper ) {
        final SetMultimap<FullQualifiedName, Object> m = HashMultimap.create();
        for ( Row row : rs ) {
            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
            String entityId = row.getString( CommonColumns.ENTITYID.cql() );
            if ( propertyTypeId != null ) {
                PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
                m.put( pt.getType(),
                        deserializeValue( mapper,
                                row.getBytes( CommonColumns.PROPERTY_VALUE.cql() ),
                                pt.getDatatype(),
                                entityId ) );
            }
        }
        return m;
    }

    public static SetMultimap<UUID, Object> entityIndexedById(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            ObjectMapper mapper ) {
        final SetMultimap<UUID, Object> m = HashMultimap.create();
        for ( Row row : rs ) {
            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
            String entityId = row.getString( CommonColumns.ENTITYID.cql() );
            if ( propertyTypeId != null ) {
                PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
                m.put( propertyTypeId,
                        deserializeValue( mapper,
                                row.getBytes( CommonColumns.PROPERTY_VALUE.cql() ),
                                pt.getDatatype(),
                                entityId ) );
            }
        }
        return m;
    }

    public static Pair<SetMultimap<UUID, Object>, SetMultimap<FullQualifiedName, Object>> entityIdFQNPair(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            ObjectMapper mapper ) {
        final SetMultimap<UUID, Object> mByUUID = HashMultimap.create();
        final SetMultimap<FullQualifiedName, Object> mByKey = HashMultimap.create();

        for ( Row row : rs ) {
            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
            String entityId = row.getString( CommonColumns.ENTITYID.cql() );
            if ( propertyTypeId != null ) {
                PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
                Object value = deserializeValue( mapper,
                        row.getBytes( CommonColumns.PROPERTY_VALUE.cql() ),
                        pt.getDatatype(),
                        entityId );
                mByUUID.put( propertyTypeId,
                        value );
                mByKey.put( authorizedPropertyTypes.get( propertyTypeId ).getType(), value );
            }
        }
        return Pair.of( mByUUID, mByKey );
    }

    public static String entityId( Row row ) {
        return row.getString( CommonColumns.ENTITYID.cql() );
    }

    public static String name( Row row ) {
        return row.getString( CommonColumns.NAME.cql() );
    }

    public static String namespace( Row row ) {
        return row.getString( CommonColumns.NAMESPACE.cql() );
    }

    public static String title( Row row ) {
        return row.getString( CommonColumns.TITLE.cql() );
    }

    public static Optional<String> description( Row row ) {
        return Optional.fromNullable( row.getString( CommonColumns.DESCRIPTION.cql() ) );
    }

    public static Set<String> contacts( Row row ) {
        return row.getSet( CommonColumns.CONTACTS.cql(), String.class );
    }

    public static UUID id( Row row ) {
        return row.getUUID( CommonColumns.ID.cql() );
    }

    public static UUID entityTypeId( Row row ) {
        return row.getUUID( CommonColumns.ENTITY_TYPE_ID.cql() );
    }

    public static EntitySet entitySet( Row row ) {
        // TODO: Validate data read from Cassandra and log errors for invalid entries.
        UUID id = id( row );
        UUID entityTypeId = entityTypeId( row );
        String name = name( row );
        String title = title( row );
        Optional<String> description = description( row );
        Set<String> contacts = contacts( row );
        return new EntitySet( id, entityTypeId, name, title, description, contacts );
    }

    public static PropertyType propertyType( Row row ) {
        UUID id = id( row );
        FullQualifiedName type = new FullQualifiedName( namespace( row ), name( row ) );
        String title = title( row );
        Optional<String> description = description( row );
        Set<FullQualifiedName> schemas = row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
        EdmPrimitiveTypeKind dataType = row.get( CommonColumns.DATATYPE.cql(), EdmPrimitiveTypeKind.class );
        Optional<Boolean> piiField = Optional.of( row.getBool( CommonColumns.PII_FIELD.cql() ) );
        return new PropertyType( id, type, title, description, schemas, dataType, piiField );
    }

    public static EntityType entityType( Row row ) {
        UUID id = id( row );
        FullQualifiedName type = new FullQualifiedName( namespace( row ), name( row ) );
        String title = title( row );
        Optional<String> description = description( row );
        Set<FullQualifiedName> schemas = row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
        LinkedHashSet<UUID> key = (LinkedHashSet<UUID>) row.getSet( CommonColumns.KEY.cql(), UUID.class );
        LinkedHashSet<UUID> properties = (LinkedHashSet<UUID>) row.getSet( CommonColumns.PROPERTIES.cql(), UUID.class );
        Optional<UUID> baseType = Optional.fromNullable( row.getUUID( CommonColumns.BASE_TYPE.cql() ) );
        return new EntityType( id, type, title, description, schemas, key, properties, baseType );
    }

    public static FullQualifiedName splitFqn( Row row ) {
        String namespace = row.getString( CommonColumns.NAMESPACE.cql() );
        String name = row.getString( CommonColumns.NAME.cql() );
        return new FullQualifiedName( namespace, name );
    }

    public static FullQualifiedName fqn( Row row ) {
        return row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
    }

    public static FullQualifiedName type( Row row ) {
        return row.get( CommonColumns.TYPE.cql(), FullQualifiedName.class );
    }

    public static SecurableObjectType securableObjectType( Row row ) {
        return row.get( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), SecurableObjectType.class );
    }

    public static List<UUID> aclRoot( Row row ) {
        return row.getList( CommonColumns.ACL_ROOT.cql(), UUID.class );
    }

    public static Map<UUID, EnumSet<Permission>> aclChildrenPermissions( Row row ) {
        return row.getMap( CommonColumns.ACL_CHILDREN_PERMISSIONS.cql(),
                TypeToken.of( UUID.class ),
                EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
    }

    public static RequestStatus reqStatus( Row row ) {
        return row.get( CommonColumns.STATUS.cql(), RequestStatus.class );
    }

    public static String principalId( Row row ) {
        return row.getString( CommonColumns.PRINCIPAL_ID.cql() );
    }

    public static UUID requestId( Row row ) {
        return row.getUUID( CommonColumns.REQUESTID.cql() );
    }

    public static Set<EntityKey> entityKeys( Row row ) {
        return row.getSet( CommonColumns.ENTITY_KEYS.cql(), EntityKey.class );
    }

    public static Pair<UUID, Set<EntityKey>> linkedEntity( Row row ) {
        return Pair.of( row.getUUID( CommonColumns.VERTEX_ID.cql() ), entityKeys( row ) );
    }

    public static UUID syncId( Row row ) {
        return row.getUUID( CommonColumns.SYNCID.cql() );
    }

    public static UUID entitySetId( Row row ) {
        return row.getUUID( CommonColumns.ENTITY_SET_ID.cql() );
    }

    /**
     * This directly depends on Jackson's raw data binding. See http://wiki.fasterxml.com/JacksonInFiveMinutes
     * 
     * @param type
     * @return
     */
    public static ByteBuffer serializeValue(
            ObjectMapper mapper,
            Object value,
            EdmPrimitiveTypeKind type,
            String entityId ) {
        switch ( type ) {
            // To come back to: binary, byte
            /**
             * Jackson binds to Boolean
             */
            case Boolean:
                return TypeCodec.cboolean().serialize( (Boolean) value, protocolVersion );
            /**
             * Jackson binds to String
             */
            case Binary:
            case Date:
            case DateTimeOffset:
            case Duration:
            case Guid:
            case String:
            case TimeOfDay:
                return TypeCodec.varchar().serialize( (String) value, protocolVersion );
            /**
             * Jackson binds to Double
             */
            case Decimal:
            case Double:
            case Single:
                return TypeCodec.cdouble().serialize( Double.parseDouble( value.toString() ), protocolVersion );
            /**
             * Jackson binds to Integer, Long, or BigInteger
             */
            case Byte:
            case SByte:
                return TypeCodec.tinyInt().serialize( Byte.parseByte( value.toString() ), protocolVersion );
            case Int16:
                return TypeCodec.smallInt().serialize( Short.parseShort( value.toString() ), protocolVersion );
            case Int32:
                return TypeCodec.cint().serialize( Integer.parseInt( value.toString() ), protocolVersion );
            case Int64:
                return TypeCodec.bigint().serialize( Long.parseLong( value.toString() ), protocolVersion );
            default:
                try {
                    return ByteBuffer.wrap( mapper.writeValueAsBytes( value ) );
                } catch ( JsonProcessingException e ) {
                    logger.error( "Serialization error when writing entity " + entityId );
                    return null;
                }
        }
    }

    /**
     * This directly depends on Jackson's raw data binding. See http://wiki.fasterxml.com/JacksonInFiveMinutes
     * 
     * @param type
     * @return
     */
    public static Object deserializeValue(
            ObjectMapper mapper,
            ByteBuffer bytes,
            EdmPrimitiveTypeKind type,
            String entityId ) {
        switch ( type ) {
            /**
             * Jackson binds to Boolean
             */
            case Boolean:
                return TypeCodec.cboolean().deserialize( bytes, protocolVersion );
            /**
             * Jackson binds to String
             */
            case Binary:
            case Date:
            case DateTimeOffset:
            case Duration:
            case Guid:
            case String:
            case TimeOfDay:
                return TypeCodec.varchar().deserialize( bytes, protocolVersion );
            /**
             * Jackson binds to Double
             */
            case Decimal:
            case Double:
            case Single:
                return TypeCodec.cdouble().deserialize( bytes, protocolVersion );
            /**
             * Jackson binds to Integer, Long, or BigInteger
             */
            case Byte:
            case SByte:
                return TypeCodec.tinyInt().deserialize( bytes, protocolVersion );
            case Int16:
                return TypeCodec.smallInt().deserialize( bytes, protocolVersion );
            case Int32:
                return TypeCodec.cint().deserialize( bytes, protocolVersion );
            case Int64:
                return TypeCodec.bigint().deserialize( bytes, protocolVersion );
            default:
                try {
                    return mapper.readValue( Bytes.getArray( bytes ), Object.class );
                } catch ( IOException e ) {
                    logger.error( "Deserialization error when reading entity " + entityId );
                    return null;
                }
        }
    }
}
