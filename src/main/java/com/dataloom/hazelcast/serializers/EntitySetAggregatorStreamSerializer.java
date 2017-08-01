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

import com.dataloom.data.aggregators.EntitySetAggregator;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import java.io.IOException;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class EntitySetAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<EntitySetAggregator> {
    @Override public Class<EntitySetAggregator> getClazz() {
        return EntitySetAggregator.class;
    }

    @Override public void write( ObjectDataOutput out, EntitySetAggregator object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getStreamId() );
        out.writeLong( object.aggregate() );
    }

    @Override public EntitySetAggregator read( ObjectDataInput in ) throws IOException {
        return new EntitySetAggregator( UUIDStreamSerializer.deserialize( in ), in.readLong() );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_SET_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }
}
