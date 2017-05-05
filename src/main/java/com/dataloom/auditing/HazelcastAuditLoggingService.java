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

import com.dataloom.auditing.processors.AuditMetricIncrementer;
import com.dataloom.authorization.AclKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */

@Deprecated
public class HazelcastAuditLoggingService {
    private static EntryProcessor<AclKey, AuditMetric> INCREMENTER = new AuditMetricIncrementer();
//    private final IMap<AuditableEventKey, AuditableEvent<?>> auditEvents;
    private final IMap<AclKey, AuditMetric> leaderboard;
    private final AuditQueryService         aqs;

    public HazelcastAuditLoggingService(
            HazelcastInstance hazelcastInstance,
            AuditQueryService aqs,
            EventBus eventBus ) {
//        this.auditEvents = hazelcastInstance.getMap( HazelcastMap.AUDIT_EVENTS.name() );
        this.leaderboard = hazelcastInstance.getMap( HazelcastMap.AUDIT_METRICS.name() );
        this.aqs = aqs;
        eventBus.register( this );
    }

    @Subscribe
    public void processAuditableEvent( AuditableEvent event ) {
        aqs.storeAuditableEvent( event );
        leaderboard.submitToKey( AclKey.wrap( event.getAclKey() ), INCREMENTER );
    }


}
