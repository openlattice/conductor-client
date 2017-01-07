package com.kryptnostic.datastore.cassandra;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
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
    
    public static UUID id( Row row ) {
        return row.getUUID( CommonColumns.ID.cql() );
    }

    public static EntitySet entitySet( Row row ) {
        // TODO: Validate data read from Cassandra and log errors for invalid entries.
        UUID id = id( row );
        FullQualifiedName type = row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
        String name = row.getString( CommonColumns.NAME.cql() );
        String title = row.getString( CommonColumns.TITLE.cql() );
        String description = row.getString( CommonColumns.DESCRIPTION.cql() );
        return new EntitySet( id, type, name, title , description );
    }

    public static PropertyType propertyType( Row row ) {
        UUID id = row.getUUID( CommonColumns.ID.cql() );
        FullQualifiedName type = row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
        Set<FullQualifiedName> schemas = row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
        EdmPrimitiveTypeKind dataType = row.get( CommonColumns.DATATYPE.cql(), EdmPrimitiveTypeKind.class );
        return new PropertyType( id, type, schemas, dataType );
    }

    public static EntityType entityType( Row row ) {
        UUID id = row.getUUID( CommonColumns.ID.cql() );
        FullQualifiedName type = row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
        Set<FullQualifiedName> schemas = row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
        Set<UUID> key = row.getSet( CommonColumns.KEY.cql(), UUID.class );
        Set<UUID> properties = row.getSet( CommonColumns.PROPERTIES.cql(), UUID.class );
        return new EntityType( id, type, schemas, key, properties );
    }

    public static FullQualifiedName splitFqn( Row row ) {
        String namespace = row.getString( CommonColumns.NAMESPACE.cql() );
        String name = row.getString( CommonColumns.NAME.cql() );
        return new FullQualifiedName( namespace, name );
    }

    public static FullQualifiedName fqn( Row row ) {
        return row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
    }
}
