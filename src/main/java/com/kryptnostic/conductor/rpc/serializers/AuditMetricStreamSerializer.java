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

package com.kryptnostic.conductor.rpc.serializers;

import com.dataloom.auditing.AuditMetric;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@Component
public class AuditMetricStreamSerializer implements SelfRegisteringStreamSerializer<AuditMetric> {
    @Override public Class<? extends AuditMetric> getClazz() {
        return AuditMetric.class;
    }

    @Override public void write( ObjectDataOutput out, AuditMetric object ) throws IOException {
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getAclKey() );
        out.writeLong( object.getCounter() );
    }

    @Override public AuditMetric read( ObjectDataInput in ) throws IOException {
        List<UUID> aclKey = ListStreamSerializers.fastUUIDListDeserialize( in );
        long counter = in.readLong();
        return new AuditMetric( counter, aclKey );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.AUDIT_METRIC.ordinal();
    }

    @Override public void destroy() {

    }
}
