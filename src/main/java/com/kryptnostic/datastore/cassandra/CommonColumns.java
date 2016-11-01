package com.kryptnostic.datastore.cassandra;

import java.util.function.Function;

import org.apache.commons.lang3.RandomStringUtils;

import com.datastax.driver.core.DataType;
import com.google.common.base.Preconditions;

public enum CommonColumns {
    ACLID( DataType.uuid() ),
    ROLE( DataType.text() ),
    CLOCK( DataType.timestamp() ),
    DATATYPE( DataType.text() ),
    ENTITYSETS( DataType.set( DataType.text() ) ),
    ENTITY_TYPES( DataType.set( DataType.text() ) ),
    FQN( DataType.text() ),
    KEY( DataType.set( DataType.text() ) ),
    MULTIPLICITY( DataType.bigint() ),
    NAME( DataType.text() ),
    NAMESPACE( DataType.text() ),
    ENTITYID( DataType.uuid() ),
    PROPERTIES( DataType.set( DataType.text() ) ),
    SCHEMAS( DataType.set( DataType.text() ) ),
    SYNCIDS( DataType.list( DataType.uuid() ) ),
    TITLE( DataType.text() ),
    TYPENAME( DataType.text() ),
    TYPE( DataType.text() ),
    VALUE( null ),
    PERMISSIONS( DataType.cint() ),
    PARTITION_INDEX( DataType.tinyint() ); // partition index within a table for distribution purpose

    private final DataType type;
    private final String   bindMarker;

    private CommonColumns( DataType type ) {
        this.type = type;
        String maybeNewMarker = RandomStringUtils.randomAlphabetic( 8 );
        while ( !CommonColumnsHelper.usedBindMarkers.add( maybeNewMarker ) ) {
            maybeNewMarker = RandomStringUtils.randomAlphabetic( 8 );
        }
        this.bindMarker = maybeNewMarker;
    }

    public DataType getType( Function<CommonColumns, DataType> typeResolver ) {
        return type == null ? typeResolver.apply( this ) : getType();
    }

    public DataType getType() {
        return Preconditions.checkNotNull( type, "This column requires a type resolver." );
    }

    public String bindMarker() {
        return bindMarker;
    }

    public String cql() {
        return super.name().toLowerCase();
    }

    @Override
    @Deprecated
    public String toString() {
        return cql();
    }
}
