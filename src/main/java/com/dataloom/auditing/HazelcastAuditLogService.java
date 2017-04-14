package com.dataloom.auditing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.neuron.AuditableSignal;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastAuditLogService {

    private static final Logger logger = LoggerFactory.getLogger( HazelcastAuditLogService.class );

    private final AuditLogQueryService                    auditLogQueryService;

    public HazelcastAuditLogService( HazelcastInstance hazelcastInstance, AuditLogQueryService auditLogQueryService ) {

        this.auditLogQueryService = auditLogQueryService;
    }

    public void log( AuditableSignal signal ) {

        this.auditLogQueryService.store( signal );
    }
}
