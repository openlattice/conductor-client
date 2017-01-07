package com.kryptnostic.datastore.cassandra;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.Row;
import com.google.common.base.Function;

public class CassandraPropertyReader implements Function<Row, Object> {
    private final FullQualifiedName     type;
    private final Function<Row, Object> reader;

    private CassandraPropertyReader(  FullQualifiedName type, Function<Row, Object> reader ) {
        this.type = type;
        this.reader = reader;
    }

    public FullQualifiedName getType() {
        return type;
    }

    @Override
    public Object apply( Row input ) {
        return reader.apply( input );
    }
    
}
