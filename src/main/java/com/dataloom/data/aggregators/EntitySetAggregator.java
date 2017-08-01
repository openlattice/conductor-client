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

import com.dataloom.data.storage.EntityBytes;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IQueue;
import java.util.Map.Entry;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntitySetAggregator extends Aggregator<Entry<UUID, EntityBytes>, Long> implements HazelcastInstanceAware {
    private static final Logger logger = LoggerFactory.getLogger( EntitySetAggregator.class );
    private final     UUID                streamId;
    private transient long                count;
    private transient IQueue<EntityBytes> stream;

    public EntitySetAggregator( UUID streamId ) {
        this( streamId, 0 );
    }

    public EntitySetAggregator( UUID streamId, long count ) {
        this.count = count;
        this.streamId = streamId;
    }

    @Override public void accumulate( Entry<UUID, EntityBytes> input ) {
        try {
            stream.put( input.getValue() );
            ++count;
        } catch ( InterruptedException e ) {
            logger.error( "Unable to stream entity: {}", input, e );
        }
    }

    @Override public void combine( Aggregator aggregator ) {
        this.count += ( (EntitySetAggregator) aggregator ).count;
    }

    @Override public Long aggregate() {
        return count;
    }

    public UUID getStreamId() {
        return streamId;
    }

    @Override
    public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.stream = hazelcastInstance.getQueue( streamId.toString() );
    }
}
