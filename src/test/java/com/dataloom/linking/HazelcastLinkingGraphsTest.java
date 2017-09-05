/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.linking;

import com.dataloom.authorization.HzAuthzTest;
import com.dataloom.clustering.DistributedClusterer;
import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.mapstores.LinkingVerticesMapstore;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.streams.StreamUtil;
import com.hazelcast.core.IMap;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class HazelcastLinkingGraphsTest extends HzAuthzTest {
    protected static final HazelcastLinkingGraphs    graphs;
    protected static final DistributedClusterer      partitioner;
    protected static final LinkingVerticesMapstore   lvm;
    protected static final IMap<LinkingEdge, Double> edges;
    protected static final UUID   graphId = UUID.randomUUID();
    private static final   Random r       = new Random();
    private static final   Logger logger  = LoggerFactory
            .getLogger( HazelcastLinkingGraphsTest.class );

    static {
        graphs = new HazelcastLinkingGraphs( hazelcastInstance );
        edges = hazelcastInstance.getMap( HazelcastMap.LINKING_EDGES.name() );
        partitioner = new DistributedClusterer( hazelcastInstance );
        lvm = new LinkingVerticesMapstore( session );
    }

    @Test
    public void testClustering() {
        final int entityCount = 1000;
        Set<EntityKey> entityKeys = new HashSet<>( entityCount );
        for ( int i = 0; i < entityCount; i++ ) {
            entityKeys.add( TestDataFactory.entityKey() );
        }

        Set<LinkingVertexKey> vertices = entityKeys
                .parallelStream()
                .map( entityKey -> graphs.getOrCreateVertex( graphId, entityKey ) )
                .collect( Collectors.toSet() );

        vertices.parallelStream()
                .flatMap( u -> vertices
                        .parallelStream()
                        .filter( v -> !u.equals( v ) && r.nextBoolean() && r.nextBoolean() && r.nextBoolean()
                                && r.nextBoolean() )
                        .map( v -> new LinkingEdge( u, v ) ) )
                .forEach( edge -> edges.set( edge, 2*r.nextDouble() ) );
        List<Entry<LinkingEdge, Double>> sortedEdges = edges.entrySet()
                .stream()
                .sorted( Comparator.comparing( Entry::getValue ) )
                .collect( Collectors.toList() );

        partitioner.cluster( graphId, sortedEdges.get( 0 ).getValue() );
//                new WeightedLinkingEdge( sortedEdges.get( 1 ).getValue(), sortedEdges.get( 1 ).getKey() )

        StreamUtil.stream( lvm.loadAllKeys() ).map( graphs::getVertex )
                .sorted( Comparator.comparing( LinkingVertex::getDiameter ) )
                .peek( v -> Assert.assertTrue( v.getEntityKeys().size() > 0 ) )
                .forEach( v -> logger.info( "Cluster: {}", v ) );
    }

}
