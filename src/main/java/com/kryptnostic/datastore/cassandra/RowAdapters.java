package com.kryptnostic.datastore.cassandra;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.datastax.driver.core.Row;

public final class RowAdapters {
    private RowAdapters() {}

    public static EntitySet entitySet( Row row ) {
        // TODO: Validate data read from Cassandra and log errors for invalid entries.
        UUID id = row.getUUID( CommonColumns.ID.cql() );
        FullQualifiedName type = row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
        String name = row.getString( CommonColumns.NAME.cql() );
        String title = row.getString( CommonColumns.TITLE.cql() );
        return new EntitySet( id, type, name, title );
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
        Set<FullQualifiedName> key = row.getSet( CommonColumns.KEY.cql(), FullQualifiedName.class );
        Set<FullQualifiedName> properties = row.getSet( CommonColumns.PROPERTIES.cql(), FullQualifiedName.class );
        return new EntityType( id, type, schemas, key, properties );
    }
}
