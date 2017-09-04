/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.dataloom.linking.aggregators;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.WeightedLinkingEdge;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MergingAggregator extends Aggregator<Entry<LinkingEdge, Double>, Double>
        implements HazelcastInstanceAware {
    private static final Logger logger = LoggerFactory.getLogger( MergingAggregator.class );

    private final Map<UUID, Double>   srcNeighborWeights;
    private final Map<UUID, Double>   dstNeighborWeights;
    private final WeightedLinkingEdge lightest;

    private transient IMap<LinkingEdge, Double> weightedEdges = null;
    private transient HazelcastLinkingGraphs    graphs        = null;

    public MergingAggregator(
            WeightedLinkingEdge lightest,
            Map<UUID, Double> srcNeighborWeights,
            Map<UUID, Double> dstNeighborWeights ) {
        this.srcNeighborWeights = srcNeighborWeights;
        this.dstNeighborWeights = dstNeighborWeights;
        this.lightest = lightest;
    }

    public MergingAggregator( WeightedLinkingEdge lightest ) {
        this( lightest, new HashMap<>(), new HashMap<>() );
    }

    @Override
    public void accumulate( Entry<LinkingEdge, Double> input ) {
        LinkingEdge edge = input.getKey();
        LinkingEdge lightestEdge = lightest.getEdge();

        if ( !edge.equals( lightestEdge ) ) {
            double weight = lightest.getWeight() + 0;
            UUID srcId = lightestEdge.getSrcId();
            UUID dstId = lightestEdge.getDstId();

            if ( srcId.equals( edge.getSrcId() ) ) {
                srcNeighborWeights.put( edge.getDstId(), weight );
            } else if ( srcId.equals( edge.getDstId() ) ) {
                srcNeighborWeights.put( edge.getSrcId(), weight );
            }

            if ( dstId.equals( edge.getSrcId() ) ) {
                dstNeighborWeights.put( edge.getDstId(), weight );
            } else if ( dstId.equals( edge.getDstId() ) ) {
                dstNeighborWeights.put( edge.getSrcId(), weight );
            }
        }
        weightedEdges.delete( edge );
    }

    @Override
    public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof MergingAggregator ) {
            MergingAggregator other = (MergingAggregator) aggregator;

            // TODO: At some point we might want to check and make sure there aren't duplicates.
            srcNeighborWeights.putAll( other.srcNeighborWeights );
            dstNeighborWeights.putAll( other.dstNeighborWeights );

        } else {
            logger.error( "Cannot combine incompatible aggregators." );
        }
    }

    @Override
    public Double aggregate() {
        final LinkingVertexKey vertexKey = graphs.merge( lightest );
        weightedEdges.delete( lightest );
        if ( srcNeighborWeights.isEmpty() && dstNeighborWeights.isEmpty() ) {
            return null;
        }
        Stream<UUID> neighbors = Stream
                .concat( srcNeighborWeights.keySet().stream(), dstNeighborWeights.keySet().stream() );

        return neighbors
                .mapToDouble( neighbor -> agg( neighbor, vertexKey ) )
                .min()
                .getAsDouble();
    }

    @Override
    public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.graphs = new HazelcastLinkingGraphs( hazelcastInstance );
        this.weightedEdges = hazelcastInstance.getMap( HazelcastMap.LINKING_EDGES.name() );
    }

    public Map<UUID, Double> getSrcNeighborWeights() {
        return srcNeighborWeights;
    }

    public Map<UUID, Double> getDstNeighborWeights() {
        return dstNeighborWeights;
    }

    public WeightedLinkingEdge getLightest() {
        return lightest;
    }

    private double agg( UUID neighbor, LinkingVertexKey vertexKey ) {
        final double lightestWeight = lightest.getWeight();
        final UUID graphId = lightest.getEdge().getGraphId();
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

        double weight = Math.max( minSrc, minDst );
        weightedEdges.set( replacementLinkingEdge, weight );
        return weight;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( !( o instanceof MergingAggregator ) ) {
            return false;
        }

        MergingAggregator that = (MergingAggregator) o;

        if ( !srcNeighborWeights.equals( that.srcNeighborWeights ) ) {
            return false;
        }
        if ( !dstNeighborWeights.equals( that.dstNeighborWeights ) ) {
            return false;
        }
        return lightest.equals( that.lightest );
    }

    @Override
    public int hashCode() {
        int result = srcNeighborWeights.hashCode();
        result = 31 * result + dstNeighborWeights.hashCode();
        result = 31 * result + lightest.hashCode();
        return result;
    }
}
