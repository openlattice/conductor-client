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

import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.WeightedLinkingEdge;
import com.hazelcast.aggregation.Aggregator;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LightestEdgeAggregator extends Aggregator<Entry<LinkingEdge, Double>, WeightedLinkingEdge[]> {
    private static final Logger logger = LoggerFactory.getLogger( LightestEdgeAggregator.class );

    private WeightedLinkingEdge[] lightest = new WeightedLinkingEdge[] { null, null };

    public LightestEdgeAggregator( WeightedLinkingEdge[] lightest ) {
        this.lightest = lightest;
    }

    public LightestEdgeAggregator() {
    }

    @Override
    public void accumulate( Entry<LinkingEdge, Double> input ) {
        double weight = input.getValue().doubleValue();
        if ( lightest[ 0 ] == null || weight < lightest[ 0 ].getWeight() ) {
            lightest[ 1 ] = lightest[ 0 ];
            lightest[ 0 ] = new WeightedLinkingEdge( weight, input.getKey() );
        } else if ( lightest[ 1 ] == null || weight < lightest[ 1 ].getWeight() ) {
            lightest[ 1 ] = new WeightedLinkingEdge( weight, input.getKey() );
        }
    }

    @Override
    public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof LightestEdgeAggregator ) {
            LightestEdgeAggregator other = (LightestEdgeAggregator) aggregator;
            merge( lightest[ 0 ] );
            merge( lightest[ 1 ] );
        } else {
            logger.error( "Incompatible aggregator for lightest edge" );
        }
    }

    public WeightedLinkingEdge[] getLightestEdges() {
        return lightest;
    }

    @Override
    public WeightedLinkingEdge[] aggregate() {
        return lightest;
    }

    private void merge( WeightedLinkingEdge edge ) {
        if ( edge == null ) { return; }
        double weight = edge.getWeight();
        if ( lightest[ 0 ] == null || weight < lightest[ 0 ].getWeight() ) {
            lightest[ 1 ] = lightest[ 0 ];
            lightest[ 0 ] = edge;
        } else if ( lightest[ 1 ] == null || weight < lightest[ 1 ].getWeight() ) {
            lightest[ 1 ] = edge;
        }
    }

}
