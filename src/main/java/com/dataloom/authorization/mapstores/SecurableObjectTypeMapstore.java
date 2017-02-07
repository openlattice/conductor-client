package com.dataloom.authorization.mapstores;

import java.util.List;
import java.util.UUID;

import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class SecurableObjectTypeMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<List<UUID>, SecurableObjectType>{

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
    
    @Override
    protected RegularStatement loadQuery(){
        return tableBuilder.buildLoadByPartitionKeyQueryWithStaticColumns();
    }

}
