package com.dataloom.auditing;

import com.hazelcast.util.Preconditions;

import java.util.List;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class AuditMetric implements Comparable<AuditMetric> {
    private final List<UUID> aclKey;
    private       long       counter;

    public AuditMetric( long counter, List<UUID> aclKey ) {
        this.counter = counter;
        this.aclKey = Preconditions.checkNotNull( aclKey );
    }

    public void increment() {
        counter++;
    }

    public long getCounter() {
        return counter;
    }

    public List<UUID> getAclKey() {
        return aclKey;
    }

    @Override
    public int compareTo( AuditMetric o ) {
        return Long.compare( getCounter(), o.getCounter() );
    }
}
