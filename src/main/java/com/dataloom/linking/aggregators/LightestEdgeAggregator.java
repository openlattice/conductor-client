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
public class LightestEdgeAggregator extends Aggregator<Entry<LinkingEdge, Double>, WeightedLinkingEdge> {
    private static final Logger logger = LoggerFactory.getLogger( LightestEdgeAggregator.class );

    private WeightedLinkingEdge lightest = null;

    @Override
    public void accumulate( Entry<LinkingEdge, Double> input ) {
        double weight = input.getValue().doubleValue();
        if ( lightest == null || weight > lightest.getWeight() ) {
            lightest = new WeightedLinkingEdge( weight, input.getKey() );
        }
    }

    @Override
    public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof LightestEdgeAggregator ) {
            LightestEdgeAggregator other = (LightestEdgeAggregator) aggregator;
            if ( lightest == null && other.lightest != null ) {
                lightest = other.lightest;
            } else if ( lightest != null && other.lightest != null && lightest.getWeight() > other.lightest
                    .getWeight() ) {
                lightest = other.lightest;
            }
        } else {
            logger.error( "Incompatible aggregator for lightest edge" );
        }
    }

    @Override public WeightedLinkingEdge aggregate() {
        return lightest;
    }

}
