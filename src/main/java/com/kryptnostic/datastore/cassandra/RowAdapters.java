package com.kryptnostic.datastore.cassandra;

import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.EntitySet;
import com.datastax.driver.core.Row;

public final class RowAdapters {
    private RowAdapters() {}

    public static EntitySet entitySet( Row row ) {
        //TODO: Validate data read from Cassandra and log errors for invalid entries.
        UUID id = row.getUUID( CommonColumns.ID.cql() );
        FullQualifiedName type = row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
        String name = row.getString( CommonColumns.NAME.cql() );
        String title = row.getString( CommonColumns.TITLE.cql() );
        return new EntitySet( id, type, name, title );
    }
    
    

}
