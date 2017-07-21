package com.dataloom.data.mapstores;

import com.dataloom.data.EntityKey;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.map.eviction.LRUEvictionPolicy;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityKeyIdsMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<EntityKey, UUID> {
    private static final Logger logger = LoggerFactory.getLogger( EntityKeyIdsMapstore.class );

    public EntityKeyIdsMapstore( String mapName, Session session, CassandraTableBuilder tableBuilder ) {
        super( mapName, session, tableBuilder );
    }

    @Override public Iterable<EntityKey> loadAllKeys() {
        return null;
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
    protected RegularStatement loadAllKeysQuery() {
        return tableBuilder.buildLoadAllPartitionKeysQuery();
    }

    @Override
    protected EntityKey mapKey( Row rs ) {
        return rs.get( CommonColumns.ENTITY_KEY.cql(), EntityKey.class );
    }

    private Stream<UUID> mapValues( ResultSet rs ) {
        return StreamUtil.stream( rs ).map( RowAdapters::id );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        return mapValues( rs ).iterator().next();
    }

    @Override
    public UUID load( EntityKey key ) {
        Set<UUID> ids;
        do {
            UUID id = UUID.randomUUID();
            asyncStore( key, id )
                    .map( ResultSetFuture::getUninterruptibly )
                    .allMatch( Util::wasLightweightTransactionApplied );

            ids = mapValues( asyncLoad( key ).getUninterruptibly() ).collect( Collectors.toSet() );
        } while ( ids.size() > 1 );

        return ids.iterator().next();
    }

    @Override
    public Map<EntityKey, UUID> loadAll( Collection<EntityKey> keys ) {
        return keys.stream()
                .parallel()
                .unordered()
                .distinct()
                .map( this::asyncLoad )
                .map( ResultSetFuture::getUninterruptibly )
                .flatMap( StreamUtil::stream )
                .collect( Collectors.toMap( RowAdapters::entityKey, RowAdapters::id ) );
    }

    @Override
    protected Insert storeQuery() {
        return tableBuilder.buildStoreQuery().ifNotExists();
    }

    @Override
    public MapConfig getMapConfig() {
        // Don't let this map use more than 10% of heap
        return super.getMapConfig()
                .setMaxSizeConfig(
                        new MaxSizeConfig()
                                .setMaxSizePolicy( MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE )
                                .setSize( 10 ) )
                .setMapEvictionPolicy( LRUEvictionPolicy.INSTANCE );
    }
}