package com.dataloom.data.mapstores;

import com.dataloom.data.EntityKey;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class EntityKeyIdsMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<EntityKey, UUID> {
    public EntityKeyIdsMapstore( String mapName, Session session, CassandraTableBuilder tableBuilder ) {
        super( mapName, session, tableBuilder );
    }

    @Override
    public EntityKey generateTestKey() {
        return TestDataFactory.entityKey();
    }

    @Override
    public UUID generateTestValue() {
        return UUID.randomUUID();
    }

    @Override
    protected BoundStatement bind( EntityKey key, BoundStatement bs ) {
        return bs.set( CommonColumns.ENTITY_KEY.cql(), key, EntityKey.class );
    }

    @Override
    protected BoundStatement bind( EntityKey key, UUID value, BoundStatement bs ) {
        return bs.set( CommonColumns.ENTITY_KEY.cql(), key, EntityKey.class ).setUUID( CommonColumns.ID.cql(), value );
    }

    @Override
    protected EntityKey mapKey( Row rs ) {
        return rs.get( CommonColumns.ENTITY_KEY.cql(), EntityKey.class );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        Row r = rs.one();
        return r.getUUID( CommonColumns.ID.cql() );
    }

    @Override
    public UUID load( EntityKey key ) {
        final UUID id = UUID.randomUUID();
        final ResultSet rs = asyncStore( key, id ).getUninterruptibly();

        if ( Util.wasLightweightTransactionApplied( rs ) ) {
            return id;
        } else {
            return mapValue( asyncLoad( key ).getUninterruptibly() );
        }
    }

    @Override
    public Map<EntityKey, UUID> loadAll( Collection<EntityKey> keys ) {
        return keys.stream()
                .distinct()
                .map( k -> new AsyncResolver( k, UUID.randomUUID(), this ) )
                .collect( Collectors.toMap( AsyncResolver::getKey, AsyncResolver::getId ) );

    }

    private static class AsyncResolver {
        private final EntityKey            key;
        private final UUID                 id;
        private final EntityKeyIdsMapstore ms;
        private final ResultSetFuture      rsf;

        public AsyncResolver(
                EntityKey key,
                UUID id,
                EntityKeyIdsMapstore ms ) {
            this.key = key;
            this.id = id;
            this.ms = ms;
            this.rsf = ms.asyncStore( key, id );
        }

        public EntityKey getKey() {
            return key;
        }

        public UUID getId() {
            if ( Util.wasLightweightTransactionApplied( rsf.getUninterruptibly() ) ) {
                return id;
            }
            return ms.mapValue( ms.asyncLoad( key ).getUninterruptibly() );
        }

    }

    @Override
    protected Insert storeQuery() {
        return super.storeQuery().ifNotExists();
    }
}
