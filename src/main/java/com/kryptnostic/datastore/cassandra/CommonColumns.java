package com.kryptnostic.datastore.cassandra;

import java.util.function.Function;

import com.datastax.driver.core.DataType;
import com.google.common.base.Preconditions;

public enum CommonColumns {
    ACLID( DataType.uuid() ),
    CLOCK( DataType.timestamp() ),
    ENTITYSETS( DataType.set( DataType.text() ) ),
    DATATYPE( DataType.text() ),
    MULTIPLICITY( DataType.bigint() ),
    NAME( DataType.text() ),
    OBJECTID( DataType.uuid() ),
    SYNCIDS( DataType.list( DataType.uuid() ) ),
    TITLE( DataType.text() ),
    TYPENAME( DataType.text() ),
    TYPE( DataType.text() ),
    VALUE( null );

    private final DataType type;

    private CommonColumns( DataType type ) {
        this.type = type;
    }

    public DataType getType( Function<CommonColumns, DataType> typeResolver ) {
        return type == null ? typeResolver.apply( this ) : getType();
    }

    public DataType getType() {
        return Preconditions.checkNotNull( type, "This column requires a type resolver." );
    }

    @Override
    public String toString() {
        return super.name().toLowerCase();
    }
}
