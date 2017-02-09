package com.dataloom.authorization.mapstores;

import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

import java.util.List;
import java.util.UUID;

public class SecurableObjectTypeMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<List<UUID>, SecurableObjectType> {

    public SecurableObjectTypeMapstore( Session session ) {
        super( HazelcastMap.SECURABLE_OBJECT_TYPES.name(), session, Tables.PERMISSIONS.getBuilder() );
    }

    @Override
    protected BoundStatement bind( List<UUID> key, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key, UUID.class );
    }

    @Override
    protected BoundStatement bind( List<UUID> key, SecurableObjectType value, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key, UUID.class )
                .set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), value, SecurableObjectType.class );
    }

    @Override
    protected List<UUID> mapKey( Row row ) {
        return AuthorizationUtils.aclKey( row );
    }

    @Override
    protected RegularStatement storeQuery() {
        return QueryBuilder
                .update( Tables.PERMISSIONS.getKeyspace(), Tables.PERMISSIONS.getName() )
                .with( QueryBuilder.set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(),
                        CommonColumns.SECURABLE_OBJECT_TYPE.bindMarker() ) )
                .where( QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(), CommonColumns.ACL_KEYS.bindMarker() ) );
    }

    @Override
    protected SecurableObjectType mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : AuthorizationUtils.securableObjectType( row );
    }

    @Override
    public List<UUID> generateTestKey() {
        return ImmutableList.of( UUID.randomUUID() );
    }

    @Override
    public SecurableObjectType generateTestValue() {
        return SecurableObjectType.EntitySet;
    }
}
