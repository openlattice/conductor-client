package com.kryptnostic.datastore.cassandra;

import java.util.function.Function;

import org.apache.commons.lang3.RandomStringUtils;

import com.datastax.driver.core.DataType;
import com.google.common.base.Preconditions;
import com.kryptnostic.rhizome.cassandra.ColumnDef;

public enum CommonColumns implements ColumnDef {
    ACLID( DataType.uuid() ),
    ROLE( DataType.text() ),
    USER( DataType.text() ),
    CLOCK( DataType.timestamp() ),
    DATATYPE( DataType.text() ),
    ENTITY_SET( DataType.text() ),
    ENTITY_SETS( DataType.set( DataType.text() ) ),
    ENTITY_TYPE( DataType.text() ),
    ENTITY_TYPES( DataType.set( DataType.text() ) ),
    FQN( DataType.text() ),
    KEY( DataType.set( DataType.text() ) ),
    MULTIPLICITY( DataType.bigint() ),
    NAME( DataType.text() ),
    NAMESPACE( DataType.text() ),
    REQUESTID( DataType.uuid() ),
    ENTITYID( DataType.uuid() ),
    PROPERTY_TYPE( DataType.text() ),
    PROPERTIES( DataType.set( DataType.text() ) ),
    SCHEMAS( DataType.set( DataType.text() ) ),
    SYNCIDS( DataType.list( DataType.uuid() ) ),
    TITLE( DataType.text() ),
    TYPENAME( DataType.text() ),
    TYPE( DataType.text() ),
    VALUE( null ),
    SECURABLE_OBJECT_TYPE( DataType.text() ),
    SECURABLE_OBJECTID( DataType.uuid() ),
    PERMISSIONS( DataType.set( DataType.text() ) ),
    PARTITION_INDEX( DataType.tinyint() ),
    PRINCIPAL_TYPE( DataType.text() ),
    PRINCIPAL_ID( DataType.text() ),
    ACL_KEYS( DataType.frozenList( DataType.text() ) ); // partition index within a table for distribution purpose

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

    public DataType getType( Function<ColumnDef, DataType> typeResolver ) {
        return type == null ? typeResolver.apply( this ) : getType();
    }

    public DataType getType() {
        return Preconditions.checkNotNull( type, "This column requires a type resolver." );
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
