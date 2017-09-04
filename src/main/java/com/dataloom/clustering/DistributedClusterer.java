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

package com.dataloom.clustering;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.WeightedLinkingEdge;
import com.dataloom.linking.aggregators.LightestEdgeAggregator;
import com.dataloom.linking.aggregators.MergingAggregator;
import com.dataloom.linking.components.Clusterer;
import com.dataloom.linking.predicates.LinkingPredicates;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.util.PriorityQueue;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DistributedClusterer implements Clusterer {

    private final IMap<LinkingEdge, Double> weightedEdges;

    public DistributedClusterer( HazelcastInstance hazelcastInstance ) {
        this.weightedEdges = hazelcastInstance.getMap( HazelcastMap.LINKING_EDGES.name() );

    }

    @Override
    public void cluster( UUID graphId, double minimax ) {
        PriorityQueue<Double> minimaxs = new PriorityQueue<>();
        WeightedLinkingEdge lightest = weightedEdges.aggregate( new LightestEdgeAggregator(),
                LinkingPredicates.minimax( graphId, minimax ) );

        while ( lightest.getWeight() < .05 && weightedEdges.size() > 0 ) {
            Double candidate = weightedEdges.aggregate( new MergingAggregator( lightest ),
                    LinkingPredicates.getAllEdges( lightest.getEdge() ) );

            if ( candidate != null ) {
                minimaxs.add( candidate );
            }

            while ( ( lightest = weightedEdges.aggregate( new LightestEdgeAggregator(),
                    LinkingPredicates.minimax( graphId, minimax ) ) ) == null ) {
                minimax = minimaxs.poll();
            }
        }

    }
}
