package com.dataloom.data.mapstores;

import com.dataloom.data.EntityKey;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.map.eviction.LRUEvictionPolicy;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class EntityKeyIdsMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<EntityKey, UUID> {
    private static final Logger logger = LoggerFactory.getLogger( EntityKeyIdsMapstore.class );
    private final PreparedStatement updateQuery;
    private final PreparedStatement insertLookupIfNotExists;

    public EntityKeyIdsMapstore( String mapName, Session session, CassandraTableBuilder tableBuilder ) {
        super( mapName, session, tableBuilder );
        updateQuery = session.prepare( super.storeQuery() );
        insertLookupIfNotExists = session.prepare( Table.KEYS.getBuilder().buildStoreQuery().ifNotExists() );
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
        return startLoading( key, 1, true );
    }

    private UUID startLoading( EntityKey key, int backOffMillis, boolean create ) {
        final UUID id = UUID.randomUUID();
        final ResultSetFuture assignment = startLoadingAsync( key, id, create );
        return resolveLoad( assignment, key, id, backOffMillis );
    }

    private ResultSetFuture startLoadingAsync( EntityKey key, UUID id, boolean create ) {
        final ResultSetFuture assignment;
        if ( create ) {
            assignment = asyncStore( key, id );
        } else {
            assignment = asyncUpdate( key, id );
        }
        return assignment;
    }

    private UUID resolveLoad( ResultSetFuture assignment, EntityKey key, UUID id, int backoffMillis ) {
        final ResultSet rs = assignment.getUninterruptibly();

        if ( Util.wasLightweightTransactionApplied( rs ) ) {
            if ( Util.wasLightweightTransactionApplied( insertLookupIfNotExists( id, key ) ) ) {
                return id;
            } else {
                //Successfully wrote new assignment
                try {
                    Thread.sleep( backoffMillis );
                    return startLoading( key, backoffMillis << 1, false );
                } catch ( InterruptedException e ) {
                    logger.error( "Error while sleeping during backoff.", e );
                }
            }
        } else {
            //Return existing assignment
            return mapValue( asyncLoad( key ).getUninterruptibly() );
        }
        return null;
    }

    @Override
    public Map<EntityKey, UUID> loadAll( Collection<EntityKey> keys ) {
        return keys.stream()
                .distinct()
                .map( k -> new AsyncResolver( k, UUID.randomUUID(), this ) )
                .collect( Collectors.toMap( AsyncResolver::getKey, AsyncResolver::getId ) );

    }

    private ResultSet insertLookupIfNotExists( UUID id, EntityKey entityKey ) {
        return session.execute( bind( entityKey, id, insertLookupIfNotExists.bind() ) );
    }

    private ResultSetFuture asyncUpdate( EntityKey key, UUID id ) {
        return session.executeAsync( bind( key, id, updateQuery.bind() ) );
    }

    @Override
    protected Insert storeQuery() {
        return super.storeQuery().ifNotExists();
    }

    @Override public MapConfig getMapConfig() {
        //Don't let this map use more than 10% of heap
        return super.getMapConfig()
                .setMaxSizeConfig(
                        new MaxSizeConfig()
                                .setMaxSizePolicy( MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE )
                                .setSize( 10 ) )
                .setMapEvictionPolicy( LRUEvictionPolicy.INSTANCE );
    }

    private static class AsyncResolver {
        private final EntityKey            key;
        private final UUID                 id;
        private final EntityKeyIdsMapstore ms;
        private final ResultSetFuture      assignment;

        public AsyncResolver(
                EntityKey key,
                UUID id,
                EntityKeyIdsMapstore ms ) {
            this.key = key;
            this.id = id;
            this.ms = ms;
            this.assignment = ms.startLoadingAsync( key, id, true );
        }

        public EntityKey getKey() {
            return key;
        }

        public UUID getId() {
            return ms.resolveLoad( assignment, key, id, 1 );
        }
    }
}
