package com.dataloom.auditing.processors;

import com.dataloom.auditing.AuditMetric;
import com.dataloom.authorization.AclKey;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import java.util.Map;

/**
 * This class is intended to increment the in memory count for audit events for an aclkey.
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class AuditMetricIncrementer extends AbstractRhizomeEntryProcessor<AclKey, AuditMetric, Void> {
    @Override
    public Void process( Map.Entry<AclKey, AuditMetric> entry ) {
        AuditMetric m = entry.getValue();
        m.increment();
        entry.setValue( m );
        return null;
    }

    @Override public boolean equals( Object obj ) {
        return obj instanceof AuditMetricIncrementer;
    }
}
