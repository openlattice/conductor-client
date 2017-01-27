package com.kryptnostic.conductor.rpc;

import com.dataloom.auditing.processors.AuditMetricIncrementer;
import com.kryptnostic.conductor.rpc.serializers.AuditMetricIncrementerStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class AuditMetricIncrementerStreamSerializerTest
        extends AbstractStreamSerializerTest<AuditMetricIncrementerStreamSerializer, AuditMetricIncrementer> {

    @Override
    protected AuditMetricIncrementerStreamSerializer createSerializer() {
        return new AuditMetricIncrementerStreamSerializer();
    }

    @Override
    protected AuditMetricIncrementer createInput() {
        return new AuditMetricIncrementer();
    }
}
