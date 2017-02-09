package com.kryptnostic.datastore.cassandra;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.BindMarker;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Preconditions;
import com.kryptnostic.rhizome.cassandra.ColumnDef;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.function.Function;

public enum CommonColumns implements ColumnDef {
    ACLID( DataType.uuid() ),
    FROZEN_PERMISSIONS( DataType.frozenSet( DataType.text() ) ),
    ACL_CHILDREN_PERMISSIONS( DataType.map( DataType.uuid(), FROZEN_PERMISSIONS.getType() ) ),
    ACL_KEYS( DataType.frozenList( DataType.uuid() ) ), // partition index within a table for distribution purpose,
    ACL_ROOT( DataType.frozenList( DataType.uuid() ) ),
    ALLOWED_EMAIL_DOMAINS( DataType.set( DataType.text() ) ),
    AUDIT_EVENT_DETAILS( DataType.text() ),
    AUDIT_EVENT_TYPE( DataType.text() ),
    CLOCK( DataType.timestamp() ),
    DATATYPE( DataType.text() ),
    DESCRIPTION( DataType.text() ),
    ENTITYID( DataType.text() ),
    ENTITY_SET( DataType.text() ),
    ENTITY_SET_ID( DataType.uuid() ),
    ENTITY_SETS( DataType.set( DataType.text() ) ),
    ENTITY_TYPE( DataType.text() ),
    ENTITY_TYPE_ID( DataType.uuid() ),
    ENTITY_TYPES( DataType.set( DataType.text() ) ),
    FQN( DataType.text() ),
    ID( DataType.uuid() ),
    KEY( DataType.set( DataType.uuid() ) ),
    MEMBERS( DataType.set( DataType.text() ) ),
    MULTIPLICITY( DataType.bigint() ),
    NAME( DataType.text() ),
    NAME_SET( DataType.set( DataType.text() ) ),
    NAMESPACE( DataType.text() ),
    PARTITION_INDEX( DataType.tinyint() ),
    PERMISSIONS( DataType.set( DataType.text() ) ),
    PRINCIPAL( DataType.text() ),
    PRINCIPAL_TYPE( DataType.text() ),
    PRINCIPAL_ID( DataType.text() ),
    PROPERTY_TYPE( DataType.text() ),
    PROPERTY_TYPE_ID( DataType.uuid() ),
    PROPERTY_VALUE( DataType.blob() ),
    PROPERTIES( DataType.set( DataType.uuid() ) ),
    SCHEMAS( DataType.set( DataType.text() ) ),
    SYNCID( DataType.uuid() ),
    SYNCIDS( DataType.list( DataType.uuid() ) ),
    TITLE( DataType.text() ),
    TYPE( DataType.text() ),
    ROLE( DataType.text() ),
    ROLES( DataType.set( DataType.text() ) ),
    REQUESTID( DataType.uuid() ),
    SECURABLE_OBJECT_TYPE( DataType.text() ),
    SECURABLE_OBJECTID( DataType.uuid() ),
    STATUS( DataType.text() ),
    TIME_ID( DataType.uuid() ),
    TYPE_ID( DataType.uuid() ),
    TRUSTED_ORGANIZATIONS( DataType.set( DataType.uuid() ) ),
    USER( DataType.text() ),
    USERID( DataType.text() ),
    BLOCK( DataType.blob() ),
    COUNT( DataType.bigint() ),
    ACL_KEY_VALUE( DataType.frozenList( DataType.uuid() ) ),
    PII_FIELD( DataType.cboolean() );

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
