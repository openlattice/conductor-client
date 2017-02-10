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

package com.dataloom;

import com.dataloom.auditing.AuditQueryService;
import com.dataloom.auditing.AuditableEvent;
import com.dataloom.auditing.HazelcastAuditLoggingService;
import com.dataloom.authorization.HzAuthzTest;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Stopwatch;
import com.google.common.eventbus.EventBus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class AuditLogging extends HzAuthzTest {
    private static final Logger logger = LoggerFactory.getLogger(AuditLogging.class);
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

    @Test
    public void doMany() {
        Stopwatch w = Stopwatch.createStarted();
        for ( int i = 0; i < 1000; i++ ) {
            logger.info( "Attempt #{}", i );
            testListSecurableObjects();
        }
        logger.info( "Did 1000 check in {} ms", w.elapsed( TimeUnit.MILLISECONDS ) );
    }
}
