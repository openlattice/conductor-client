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
public class HazelcastAuditLoggingService {
    private static EntryProcessor<AclKey, AuditMetric> INCREMENTER = new AuditMetricIncrementer();
//    private final IMap<AuditableEventKey, AuditableEvent<?>> auditEvents;
    private final IMap<AclKey, AuditMetric>                  leaderboard;
    private final AuditQuerySerivce                          aqs;

    public HazelcastAuditLoggingService(
            HazelcastInstance hazelcastInstance,
            AuditQuerySerivce aqs,
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
