package com.kryptnostic.conductor.rpc.serializers;

import com.dataloom.auditing.processors.AuditMetricIncrementer;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@Component
public class AuditMetricIncrementerStreamSerializer implements SelfRegisteringStreamSerializer<AuditMetricIncrementer> {
    private static final AuditMetricIncrementer INCREMENTER = new AuditMetricIncrementer();

    @Override
    public Class<? extends AuditMetricIncrementer> getClazz() {
        return AuditMetricIncrementer.class;
    }

    @Override
    public void write(
            ObjectDataOutput out, AuditMetricIncrementer object ) throws IOException {
    }

    @Override
    public AuditMetricIncrementer read( ObjectDataInput in ) throws IOException {
        return INCREMENTER;
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.AUDIT_METRIC_INCREMENTER.ordinal();
    }

    @Override
    public void destroy() {

    }
}
