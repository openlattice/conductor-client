package com.dataloom.data.mapstores;

import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.blocking.LinkingEntity;
import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class LinkingEntityMapstore
        extends AbstractStructuredCassandraMapstore<GraphEntityPair, LinkingEntity> {
    private static final CassandraTableBuilder ctb = Table.LINKING_ENTITIES.getBuilder();
    public static final String GRAPH_ID = "__key#graphId";
    public static final String ENTITY_KEY_ID = "__key#entityKeyId";
    // private static final Class<Set<String>> stringSetClass = (Class<Set<String>>) Sets.newHashSet().getClass();

    public LinkingEntityMapstore( Session session ) {
        super( HazelcastMap.LINKING_ENTITIES.name(), session, ctb );
    }

    @Override protected BoundStatement bind( GraphEntityPair key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
                .setUUID( CommonColumns.ENTITY_KEY_ID.cql(), key.getEntityKeyId() );
    }

    @Override protected BoundStatement bind( GraphEntityPair key, LinkingEntity value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
                .setUUID( CommonColumns.ENTITY_KEY_ID.cql(), key.getEntityKeyId() )
                .setMap( CommonColumns.ENTITY.cql(), value.getEntity() );
    }

    @Override protected GraphEntityPair mapKey( Row rs ) {
        UUID graphId = rs.getUUID( CommonColumns.GRAPH_ID.cql() );
        UUID entityKeyId = rs.getUUID( CommonColumns.ENTITY_KEY_ID.cql() );
        return new GraphEntityPair( graphId, entityKeyId );
    }

    @Override protected LinkingEntity mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return new LinkingEntity( row.getMap( CommonColumns.ENTITY.cql(),
                TypeCodec.uuid().getJavaType(),
                TypeCodec.set( TypeCodec.varchar() ).getJavaType() )
                .entrySet().stream().collect(
                        Collectors.toMap( entry -> entry.getKey(),
                                entry -> DelegatedStringSet.wrap( (Set<String>) entry.getValue() ) ) ) );
    }

    @Override public GraphEntityPair generateTestKey() {
        return new GraphEntityPair( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public LinkingEntity generateTestValue() {
        Map<UUID, DelegatedStringSet> result = Maps.newHashMap();
        result.put( UUID.randomUUID(), DelegatedStringSet.wrap( ImmutableSet.of( "test" ) ) );
        return new LinkingEntity( result );
    }

//    @Override public MapStoreConfig getMapStoreConfig() {
//        return super.getMapStoreConfig().setWriteDelaySeconds( 5 );
//    }

    @Override
    public MapConfig getMapConfig() {
        return super.getMapConfig()
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .addMapIndexConfig( new MapIndexConfig( GRAPH_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( ENTITY_KEY_ID, false ) );
    }
}
