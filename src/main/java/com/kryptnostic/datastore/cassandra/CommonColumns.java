package com.kryptnostic.datastore.cassandra;

import java.util.function.Function;

import org.apache.commons.lang3.RandomStringUtils;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.BindMarker;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Preconditions;
import com.kryptnostic.rhizome.cassandra.ColumnDef;

public enum CommonColumns implements ColumnDef {
    ACLID( DataType.uuid() ),
    ACL_KEYS( DataType.frozenList( DataType.blob() ) ), // partition index within a table for distribution purpose
    ROLE( DataType.text() ),
    USER( DataType.text() ),
    USERID( DataType.text() ),
    CLOCK( DataType.timestamp() ),
    DATATYPE( DataType.text() ),
    ENTITY_SET( DataType.text() ),
    ENTITY_SET_ID( DataType.uuid() ),
    ENTITY_SETS( DataType.set( DataType.text() ) ),
    ENTITY_TYPE( DataType.text() ),
    ENTITY_TYPES( DataType.set( DataType.text() ) ),
    FQN( DataType.text() ),
    KEY( DataType.set( DataType.uuid() ) ),
    MULTIPLICITY( DataType.bigint() ),
    NAME( DataType.text() ),
    NAMESPACE( DataType.text() ),
    REQUESTID( DataType.uuid() ),
    ENTITYID( DataType.text() ),
    PROPERTY_TYPE( DataType.text() ),
    PROPERTY_TYPE_ID( DataType.uuid() ),
    PROPERTY_VALUE( DataType.blob() ),
    PROPERTIES( DataType.set( DataType.uuid() ) ),
    SCHEMAS( DataType.set( DataType.text() ) ),
    SYNCID( DataType.uuid() ),
    SYNCIDS( DataType.list( DataType.uuid() ) ),
    TITLE( DataType.text() ),
    TYPE( DataType.text() ),
    VALUE( null ),
    SECURABLE_OBJECT_TYPE( DataType.text() ),
    SECURABLE_OBJECTID( DataType.uuid() ),
    PERMISSIONS( DataType.set( DataType.text() ) ),
    PARTITION_INDEX( DataType.tinyint() ),
    PRINCIPAL_TYPE( DataType.text() ),
    PRINCIPAL_ID( DataType.text() ),
    ID( DataType.uuid() ),
    TYPE_ID( DataType.uuid() ),
    DESCRIPTION( DataType.text() ),
    ENTITY_TYPE_ID( DataType.uuid() ),
    PRINCIPAL( DataType.text() );

    private final DataType type;

    private CommonColumns( DataType type ) {
        this.type = type;
        String maybeNewMarker = RandomStringUtils.randomAlphabetic( 8 );
        while ( !CommonColumnsHelper.usedBindMarkers.add( maybeNewMarker ) ) {
            maybeNewMarker = RandomStringUtils.randomAlphabetic( 8 );
        }
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

    @Override
    public BindMarker bindMarker() {
        return QueryBuilder.bindMarker( cql() );
    }
}
