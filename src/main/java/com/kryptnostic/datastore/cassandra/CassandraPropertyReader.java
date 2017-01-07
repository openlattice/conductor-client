package com.kryptnostic.datastore.cassandra;

import java.util.UUID;

import com.datastax.driver.core.Row;
import com.google.common.base.Function;

public class CassandraPropertyReader implements Function<Row, Object> {
    private final UUID                  typeId;
    private final Function<Row, Object> reader;

    public CassandraPropertyReader( UUID typeId, Function<Row, Object> reader ) {
        this.typeId = typeId;
        this.reader = reader;
    }

    public UUID getTypeId() {
        return typeId;
    }

    @Override
    public Object apply( Row input ) {
        return reader.apply( input );
    }

}
