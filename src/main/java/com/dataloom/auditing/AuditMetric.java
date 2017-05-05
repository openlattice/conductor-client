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

package com.dataloom.auditing;

import com.hazelcast.util.Preconditions;

import java.util.List;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */

@Deprecated
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
