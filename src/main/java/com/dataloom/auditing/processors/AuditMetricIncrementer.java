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

package com.dataloom.auditing.processors;

import com.dataloom.auditing.AuditMetric;
import com.dataloom.authorization.AclKey;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import java.util.Map;

/**
 * This class is intended to increment the in memory count for audit events for an aclkey.
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */

@Deprecated
public class AuditMetricIncrementer extends AbstractRhizomeEntryProcessor<AclKey, AuditMetric, Void> {
    private static final long serialVersionUID = 3920888304011438731L;

    @Override
    public Void process( Map.Entry<AclKey, AuditMetric> entry ) {
        AuditMetric m = entry.getValue();
        if( m == null ) {
            m = new AuditMetric(0L ,entry.getKey() );
        }
        m.increment();
        entry.setValue( m );
        return null;
    }

    @Override public boolean equals( Object obj ) {
        return obj instanceof AuditMetricIncrementer;
    }
}
