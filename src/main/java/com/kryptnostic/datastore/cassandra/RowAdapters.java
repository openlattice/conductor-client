package com.kryptnostic.datastore.cassandra;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

public final class RowAdapters {
    private RowAdapters() {}

    public static SetMultimap<FullQualifiedName, Object> entity(
            ResultSet rs,
            Map<UUID, CassandraPropertyReader> propertyReaders ) {
        final SetMultimap<FullQualifiedName, Object> m = HashMultimap.create();
        for ( Row row : rs ) {
            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
            if ( propertyTypeId != null ) {
                CassandraPropertyReader propertyReader = propertyReaders.get( propertyTypeId );
                m.put( propertyReader.getType(), propertyReader.apply( row ) );
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
}
