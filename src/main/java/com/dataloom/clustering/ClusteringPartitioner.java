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

package com.dataloom.clustering;

import com.dataloom.linking.CassandraLinkingGraphsQueryService;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.SortedCassandraLinkingEdgeBuffer;
import com.dataloom.linking.WeightedLinkingEdge;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class ClusteringPartitioner {
    private static final Logger logger = LoggerFactory
            .getLogger( ClusteringPartitioner.class );
    private final CassandraLinkingGraphsQueryService cgqs;
    private final HazelcastLinkingGraphs             graphs;
    private final Session                            session;
    private final String                             keyspace;
    private final double defaultThreshold = 0.1D;

    public ClusteringPartitioner(
            String keyspace,
            Session session,
            CassandraLinkingGraphsQueryService cgqs,
            HazelcastLinkingGraphs graphs ) {
        this.cgqs = cgqs;
        this.graphs = graphs;
        this.session = session;
        this.keyspace = keyspace;
    }

    public void cluster( UUID graphId ) {
        cluster( graphId, defaultThreshold );
    }

    @SuppressWarnings( "null" )
    public void cluster( UUID graphId, double threshold ) {
        Stopwatch cw = Stopwatch.createStarted();
        final SortedCassandraLinkingEdgeBuffer cbuf = new SortedCassandraLinkingEdgeBuffer(
                keyspace,
                session,
                graphId,
                threshold );
        /*
         * Start with clustering threshold t 1. Identify the two closest vertices/clusters v1, v2 and choose a new free
         * vertex UUID i at random 2. Compute longest shortest path to all clusters in the neighborhood of {v1,v2} 3.
         * Merge vertices into new cluster 4. Create new edges with weights computing in (2) 5. Loop until there are no
         * edges below clustering threshold t
         */

        WeightedLinkingEdge lightestEdgeAndWeight = cbuf.getLightestEdge();

        if ( lightestEdgeAndWeight == null ) {
            return;
        }

        LinkingEdge lightestEdge = lightestEdgeAndWeight.getEdge();
        double lightestWeight = lightestEdgeAndWeight.getWeight();
        Stopwatch w = Stopwatch.createUnstarted();
        while ( lightestWeight < threshold ) {
            if ( graphs.verticesExists( lightestEdge ) ) {
                w.start();
                Map<UUID, Double> srcNeighborWeights = cgqs.getSrcNeighbors( lightestEdge );
                Map<UUID, Double> dstNeighborWeights = cgqs.getDstNeighbors( lightestEdge );

                Set<UUID> neighbors = Sets
                        .newHashSetWithExpectedSize( srcNeighborWeights.size() + dstNeighborWeights.size() );
                neighbors.addAll( srcNeighborWeights.keySet() );
                neighbors.addAll( dstNeighborWeights.keySet() );

                final LinkingVertexKey vertexKey = graphs.merge( lightestEdgeAndWeight );

                logger.info( "Step 1 of one round of clustering took {} ms.", w.elapsed( TimeUnit.MILLISECONDS ) );
                List<ResultSetFuture> srsf = new ArrayList<>( 2 * neighbors.size() );
                for ( UUID neighbor : neighbors ) {
                    Double srcNeighborWeight = srcNeighborWeights.get( neighbor );
                    Double dstNeighborWeight = dstNeighborWeights.get( neighbor );
                    double minSrc;
                    if ( srcNeighborWeight == null ) {
                        minSrc = dstNeighborWeight.doubleValue() + lightestWeight;
                    } else if ( dstNeighborWeight == null ) {
                        minSrc = srcNeighborWeight.doubleValue();
                    } else {
                        minSrc = Math
                                .min( srcNeighborWeight.doubleValue(),
                                        dstNeighborWeight.doubleValue() + lightestWeight );
                    }

                    double minDst;
                    if ( srcNeighborWeight == null ) {
                        minDst = dstNeighborWeight.doubleValue();
                    } else if ( dstNeighborWeight == null ) {
                        minDst = srcNeighborWeight.doubleValue() + lightestWeight;
                    } else {
                        minDst = Math
                                .min( srcNeighborWeight.doubleValue() + lightestWeight,
                                        dstNeighborWeight.doubleValue() );
                    }

                    LinkingVertexKey neighborKey = new LinkingVertexKey( graphId, neighbor );

                    LinkingEdge replacementLinkingEdge = new LinkingEdge( vertexKey, neighborKey );

                    WeightedLinkingEdge weightedRLE = new WeightedLinkingEdge(
                            Math.max( minSrc, minDst ),
                            replacementLinkingEdge );

                    srsf.addAll( cbuf.asyncUnsafeAddEdge( weightedRLE ) );

                    if ( srcNeighborWeight != null ) {
                        srsf.addAll( cbuf.removeEdge(
                                new WeightedLinkingEdge(
                                        srcNeighborWeight.doubleValue(),
                                        new LinkingEdge( lightestEdge.getSrc(), neighborKey ) ) ) );
                    }

                    if ( dstNeighborWeight != null ) {
                        srsf.addAll( cbuf.removeEdge(
                                new WeightedLinkingEdge(
                                        dstNeighborWeight.doubleValue(),
                                        new LinkingEdge( lightestEdge.getDst(), neighborKey ) ) ) );
                    }
                }

                srsf.addAll( cbuf.removeEdge( lightestEdgeAndWeight ) );
                logger.info( "Step 2 of one round of clustering took {} ms.", w.elapsed( TimeUnit.MILLISECONDS ) );
                srsf.stream().filter( rsf -> rsf != null ).forEach( ResultSetFuture::getUninterruptibly );

                logger.info( "Step 3 of one round of clustering took {} ms.", w.elapsed( TimeUnit.MILLISECONDS ) );
                graphs.deleteVertex( lightestEdge.getSrc() );
                graphs.deleteVertex( lightestEdge.getDst() );

                logger.info( "Step 4 of one round of clustering took {} ms.", w.elapsed( TimeUnit.MILLISECONDS ) );

                logger.info( "One round of clustering took {} ms.", w.elapsed( TimeUnit.MILLISECONDS ) );
                w.reset();
            } else {
                logger.info( "Encountered removed edge: {}", lightestEdgeAndWeight );
                cbuf.removeEdge( lightestEdgeAndWeight ).stream().forEach( ResultSetFuture::getUninterruptibly );
            }

            // Setup next loop
            lightestEdgeAndWeight = cbuf.getLightestEdge();
            if ( lightestEdgeAndWeight == null ) {
                break;
            }

            lightestEdge = lightestEdgeAndWeight.getEdge();
            lightestWeight = lightestEdgeAndWeight.getWeight();
        }
        logger.info( "Total clustering time was {} ms", cw.elapsed( TimeUnit.MILLISECONDS ) );
    }

}
