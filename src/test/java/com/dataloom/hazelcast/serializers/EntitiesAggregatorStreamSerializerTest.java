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

package com.dataloom.hazelcast.serializers;

import com.dataloom.data.aggregators.EntitiesAggregator;
import com.dataloom.data.hazelcast.PropertyKey;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntitiesAggregatorStreamSerializerTest
        extends AbstractStreamSerializerTest<EntitiesAggregatorStreamSerializer, EntitiesAggregator> {
    @Override protected EntitiesAggregatorStreamSerializer createSerializer() {
        return new EntitiesAggregatorStreamSerializer();
    }

    @Override protected EntitiesAggregator createInput() {
        EntitiesAggregator agg = new EntitiesAggregator();
        agg.getByteBuffers().put( new PropertyKey( UUID.randomUUID(), UUID.randomUUID() ),
                ByteBuffer.wrap( RandomUtils.nextBytes( 10 ) ) );
        return agg;
    }
}
