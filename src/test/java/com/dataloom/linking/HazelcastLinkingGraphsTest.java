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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.HzAuthzTest;
import com.dataloom.clustering.ClusteringPartitioner;
import com.dataloom.data.EntityKey;
import com.dataloom.linking.mapstores.LinkingVerticesMapstore;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.streams.StreamUtil;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class HazelcastLinkingGraphsTest extends HzAuthzTest {
    private static final Random                               r       = new Random();
    protected static final HazelcastLinkingGraphs             graphs;
    protected static final CassandraLinkingGraphsQueryService cgqs;
    protected static final ClusteringPartitioner              partitioner;
    protected static final LinkingVerticesMapstore            lvm;
    protected static final SortedCassandraLinkingEdgeBuffer   buffer;
    protected static final UUID                               graphId = UUID.randomUUID();
    private static final Logger                               logger  = LoggerFactory
            .getLogger( HazelcastLinkingGraphsTest.class );

    static {
        graphs = new HazelcastLinkingGraphs( hazelcastInstance );
        cgqs = new CassandraLinkingGraphsQueryService( cc.getKeyspace(), session );
        partitioner = new ClusteringPartitioner( cc.getKeyspace(), session, cgqs, graphs );
        lvm = new LinkingVerticesMapstore( session );
        buffer = new SortedCassandraLinkingEdgeBuffer(
                cc.getKeyspace(),
                session,
                graphId,
                0.0,
                0,
                0 );
    }

    @Test
    public void testClustering() {
        final int entityCount = 100;
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
                .map( edge -> new WeightedLinkingEdge( r.nextDouble(), edge ) )
                .forEach( buffer::addEdgeIfNotExists );
        
        buffer.waitForPendingOperations();
        partitioner.cluster( graphId, 2 );

        StreamUtil.stream( lvm.loadAllKeys() ).map( graphs::getVertex )
                .peek( v -> Assert.assertTrue( v.getEntityKeys().size() > 0 ) )
                .forEach( v -> logger.info( "Cluster: {}", v ) );
    }

}
