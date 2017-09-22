package com.dataloom.data.mapstores;

import com.dataloom.data.EntityKey;
import com.dataloom.data.hazelcast.DelegatedEntityKeySet;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

import java.util.UUID;

public class LinkingEntityKeyIdPairMapstore
        extends AbstractStructuredCassandraMapstore<DelegatedEntityKeySet, UUID> {
    private static final CassandraTableBuilder ctb = Table.LINKING_ENTITY_KEY_ID_PAIRS.getBuilder();

    public LinkingEntityKeyIdPairMapstore( Session session ) {
        super( HazelcastMap.LINKING_ENTITY_KEY_ID_PAIRS.name(), session, ctb );
    }

    @Override protected BoundStatement bind( DelegatedEntityKeySet key, BoundStatement bs ) {
        return bs.setSet( CommonColumns.ENTITY_KEYS.cql(), key, EntityKey.class );
    }

    @Override protected BoundStatement bind( DelegatedEntityKeySet key, UUID value, BoundStatement bs ) {
        return bs.setSet( CommonColumns.ENTITY_KEYS.cql(), key, EntityKey.class )
                .setUUID( CommonColumns.GRAPH_ID.cql(), value );
    }

    @Override protected DelegatedEntityKeySet mapKey( Row row ) {
        return row == null ?
                null :
                DelegatedEntityKeySet.wrap( row.getSet( CommonColumns.ENTITY_KEYS.cql(), EntityKey.class ) );
    }

    @Override protected UUID mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : row.getUUID( CommonColumns.GRAPH_ID.cql() );
    }

    @Override public DelegatedEntityKeySet generateTestKey() {
        return DelegatedEntityKeySet
                .wrap( ImmutableSet.of( TestDataFactory.entityKey(), TestDataFactory.entityKey() ) );
    }

    @Override public UUID generateTestValue() {
        return UUID.randomUUID();
    }

    @Override
    public MapConfig getMapConfig() {
        return super.getMapConfig()
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .addMapIndexConfig( new MapIndexConfig( "this", false ) );
    }
}
