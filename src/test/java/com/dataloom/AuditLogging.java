package com.dataloom;

import com.dataloom.auditing.AuditQueryService;
import com.dataloom.auditing.AuditableEvent;
import com.dataloom.auditing.HazelcastAuditLoggingService;
import com.dataloom.authorization.HzAuthzTest;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.eventbus.EventBus;
import org.junit.Test;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class AuditLogging extends HzAuthzTest {
    protected static final HazelcastAuditLoggingService hals;
    protected static final AuditQueryService            auditQuerys;

    static {
        auditQuerys = new AuditQueryService( cc.getKeyspace(), session );
        hals = new HazelcastAuditLoggingService( hazelcastInstance,
                auditQuerys,
                testServer.getContext().getBean( EventBus.class ) );
    }

    @Test
    public void testAuditLogging() {
        AuditableEvent ae = new AuditableEvent( TestDataFactory.aclKey(),
                TestDataFactory.userPrincipal(),
                TestDataFactory.securableObjectType(),TestDataFactory.permissions(),"This is a test" );
        hals.processAuditableEvent(ae);
    }
}
