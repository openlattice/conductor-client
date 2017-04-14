package com.dataloom.auditing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastAuditLogService {

    private static final Logger logger = LoggerFactory.getLogger( HazelcastAuditLogService.class );

    private final IMap<AuditableEventKey, AuditableEvent> auditLog;
    private final AuditLogQueryService                    auditLogQueryService;

    public HazelcastAuditLogService( HazelcastInstance hazelcastInstance, AuditLogQueryService auditLogQueryService ) {

        this.auditLog = hazelcastInstance.getMap( HazelcastMap.AUDIT_LOG.name() );
        this.auditLogQueryService = auditLogQueryService;
    }
}
