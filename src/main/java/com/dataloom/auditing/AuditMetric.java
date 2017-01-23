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

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        AuditMetric that = (AuditMetric) o;

        if ( counter != that.counter )
            return false;
        return aclKey != null ? aclKey.equals( that.aclKey ) : that.aclKey == null;
    }

    @Override public int hashCode() {
        int result = aclKey != null ? aclKey.hashCode() : 0;
        result = 31 * result + (int) ( counter ^ ( counter >>> 32 ) );
        return result;
    }

    @Override public String toString() {
        return "AuditMetric{" +
                "aclKey=" + aclKey +
                ", counter=" + counter +
                '}';
    }
}
