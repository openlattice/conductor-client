package com.dataloom.linking.mapstores;

import java.util.UUID;

import com.dataloom.data.DelegatedEntityKeySet;
import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

/**
 * Long term should store Set of Entity Key instead - perhaps need to refactor data table as well. Probably not a good idea to write cql codec at 2am though..
 * @author Ho Chung Siu
 *
 */
public class LinkedEntitiesMapstore extends AbstractStructuredCassandraMapstore<EntityKey, DelegatedEntityKeySet> {

    public LinkedEntitiesMapstore( Session session ) {
        super( HazelcastMap.LINKED_ENTITIES.name(), session, Table.LINKED_ENTITIES.getBuilder() );
    }

    @Override
    public EntityKey generateTestKey() {
        return TestDataFactory.entityKey();
    }

    @Override
    public DelegatedEntityKeySet generateTestValue() {
        return DelegatedEntityKeySet.wrap( ImmutableSet.of( TestDataFactory.entityKey(), TestDataFactory.entityKey() ) );
    }

    @Override
    protected BoundStatement bind( EntityKey key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key.getEntitySetId() )
                .setString( CommonColumns.ENTITYID.cql(), key.getEntityId() );
    }

    @Override
    protected BoundStatement bind( EntityKey key, DelegatedEntityKeySet value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key.getEntitySetId() )
                .setString( CommonColumns.ENTITYID.cql(), key.getEntityId() )
                .setSet( CommonColumns.ENTITY_KEYS.cql(), value, EntityKey.class );        
    }

    @Override
    protected EntityKey mapKey( Row row ) {
        if( row == null ){
            return null;
        }
        UUID entitySetId = row.getUUID( CommonColumns.ENTITY_SET_ID.cql() );
        String entityId = row.getString( CommonColumns.ENTITYID.cql() );
        return new EntityKey( entitySetId, entityId );
    }

    @Override
    protected DelegatedEntityKeySet mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : DelegatedEntityKeySet.wrap( row.getSet( CommonColumns.ENTITY_KEYS.cql(), EntityKey.class ) );
    }

}
