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
