package com.kryptnostic.datastore.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
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

public final class RowAdapters {
    private static final Logger logger            = LoggerFactory.getLogger( RowAdapters.class );
    // Need to match CassandraDataManager
    public static final String  VALUE_COLUMN_NAME = "value";

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
                        deserializeValue( mapper, row.getBytes( VALUE_COLUMN_NAME ), pt.getDatatype(), entityId ) );
            }
        }
        return m;
    }

    public static String entityId( Row row ) {
        return row.getString( CommonColumns.ENTITYID.cql() );
    }

    public static String name( Row row ) {
        return row.getString( CommonColumns.NAME.cql() );
    }

    public static String title( Row row ) {
        return row.getString( CommonColumns.TITLE.cql() );
    }

    public static Optional<String> description( Row row ) {
        return Optional.fromNullable( row.getString( CommonColumns.DESCRIPTION.cql() ) );
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
        FullQualifiedName type = fqn( row );
        UUID entityTypeId = entityTypeId( row );
        String name = name( row );
        String title = title( row );
        Optional<String> description = description( row );
        return new EntitySet( id, type, entityTypeId, name, title, description );
    }

    public static PropertyType propertyType( Row row ) {
        UUID id = row.getUUID( CommonColumns.ID.cql() );
        FullQualifiedName type = row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
        String title = title( row );
        Optional<String> description = description( row );
        Set<FullQualifiedName> schemas = row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
        EdmPrimitiveTypeKind dataType = row.get( CommonColumns.DATATYPE.cql(), EdmPrimitiveTypeKind.class );
        return new PropertyType( id, type, title, description, schemas, dataType );
    }

    public static EntityType entityType( Row row ) {
        UUID id = row.getUUID( CommonColumns.ID.cql() );
        FullQualifiedName type = row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
        String title = title( row );
        Optional<String> description = description( row );
        Set<FullQualifiedName> schemas = row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
        Set<UUID> key = row.getSet( CommonColumns.KEY.cql(), UUID.class );
        Set<UUID> properties = row.getSet( CommonColumns.PROPERTIES.cql(), UUID.class );
        return new EntityType( id, type, title, description, schemas, key, properties );
    }

    public static FullQualifiedName splitFqn( Row row ) {
        String namespace = row.getString( CommonColumns.NAMESPACE.cql() );
        String name = row.getString( CommonColumns.NAME.cql() );
        return new FullQualifiedName( namespace, name );
    }

    public static FullQualifiedName fqn( Row row ) {
        return row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
    }

    public static SecurableObjectType securableObjectType( Row row ) {
        return row.get( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), SecurableObjectType.class );
    }

    public static AclKeyPathFragment akpf( Row row ) {
        return new AclKeyPathFragment( securableObjectType( row ), id( row ) );
    }

    public static List<AclKeyPathFragment> aclKey( Row row ) {
        return row.getList( CommonColumns.ACL_KEYS.cql(), AclKeyPathFragment.class );
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
