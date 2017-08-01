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

package com.dataloom.data.aggregators;

import com.dataloom.data.hazelcast.DataKey;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.hazelcast.aggregation.Aggregator;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityAggregator extends Aggregator<Entry<DataKey, ByteBuffer>, SetMultimap<UUID, ByteBuffer>> {
    private final SetMultimap<UUID, ByteBuffer> byteBuffers = HashMultimap.create();

    @Override public void accumulate( Entry<DataKey, ByteBuffer> input ) {
        byteBuffers.put( input.getKey().getPropertyTypeId(), input.getValue() );
    }

    @Override public void combine( Aggregator aggregator ) {
        EntityAggregator other = (EntityAggregator) aggregator;
        this.byteBuffers.putAll( other.byteBuffers );
    }

    @Override public SetMultimap<UUID, ByteBuffer> aggregate() {
        return byteBuffers;
    }
}
