package com.dataloom.graph.mapstores;

import com.dataloom.graph.core.Neighborhood;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class EdgesMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, Neighborhood> {
    private static final Logger logger = LoggerFactory.getLogger( EdgesMapstore.class );
    public EdgesMapstore( Session session ) {
        super( HazelcastMap.EDGES.name(), session, Table.EDGES.getBuilder() );
    }

    private static void addToNeighborhood( LoomEdge e, Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> neighborhood ) {
        UUID dstTypeId = e.getKey().getDstTypeId();

        Map<UUID, SetMultimap<UUID, UUID>> m = neighborhood.get( dstTypeId );

        if ( m == null ) {
            m = new HashMap<>();
            neighborhood.put( dstTypeId, m );
        }

        UUID edgeTypeId = e.getKey().getEdgeTypeId();
        SetMultimap<UUID, UUID> sm = m.get( edgeTypeId );

        if ( sm == null ) {
            sm = HashMultimap.create();
            m.put( edgeTypeId, sm );
        }

        sm.put( e.getKey().getDstEntityKeyId(), e.getKey().getEdgeEntityKeyId() );

    }

    @Override public void store( UUID key, Neighborhood value ) {
        logger.error( "Shouldn't ever be calling store for edges mapstore." );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public Neighborhood generateTestValue() {
        return Neighborhood.randomNeighborhood();
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, Neighborhood value, BoundStatement bs ) {
        return null;
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs.getUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql() );
    }

    @Override
    public MapStoreConfig getMapStoreConfig() {
        super.getMapStoreConfig().setInitialLoadMode( InitialLoadMode.EAGER );
        return super.getMapStoreConfig();
    }

    @Override
    protected Neighborhood mapValue( ResultSet rs ) {
        Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> neighborhood = new HashMap<>();
        StreamUtil.stream( rs )
                .map( RowAdapters::loomEdge )
                .forEach( edge -> addToNeighborhood( edge, neighborhood ) );
        return new Neighborhood( neighborhood );
    }
}
