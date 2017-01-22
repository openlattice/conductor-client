package com.dataloom.requests;

import com.dataloom.authorization.AceKey;
import com.dataloom.hazelcast.pods.MapstoresPod;
import com.dataloom.hazelcast.pods.SharedStreamSerializersPod;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.requests.util.RequestUtil;
import com.datastax.driver.core.Session;
import com.geekbeast.rhizome.tests.bootstrap.CassandraBootstrap;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.codecs.pods.TypeCodecsPod;
import com.kryptnostic.datastore.cassandra.CassandraTablesPod;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.pods.CassandraPod;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RequestsTests extends CassandraBootstrap {
    protected static final RhizomeApplicationServer testServer;
    protected static final HazelcastInstance        hazelcastInstance;
    protected static final Session                  session;
    protected static final CassandraConfiguration   cc;
    protected static final RequestQueryService      aqs;
    protected static final HazelcastRequestsManager hzRequests;
    protected static final Lock lock = new ReentrantLock();

    protected static final Status      expected  = TestDataFactory.status();
    protected static final Status      expected2 = new Status(
            expected.getAclKey(),
            TestDataFactory.userPrincipal(),
            expected.getPermissions(),
            RequestStatus.SUBMITTED );
    protected static final Status      expected3 = new Status(
            expected.getAclKey(),
            TestDataFactory.userPrincipal(),
            expected.getPermissions(),
            RequestStatus.SUBMITTED );
    protected static final Status      expected4 = new Status(
            TestDataFactory.aclKey(),
            expected.getPrincipal(),
            expected.getPermissions(),
            RequestStatus.SUBMITTED );
    protected static final Set<Status> ss        = ImmutableSet.of( expected,
            expected2,
            expected3,
            TestDataFactory.status(),
            TestDataFactory.status(),
            TestDataFactory.status() );

    static {
        testServer = new RhizomeApplicationServer(
                MapstoresPod.class,
                CassandraPod.class,
                TypeCodecsPod.class,
                SharedStreamSerializersPod.class,
                CassandraTablesPod.class );
        testServer.sprout( "local", CassandraPod.CASSANDRA_PROFILE );
        hazelcastInstance = testServer.getContext().getBean( HazelcastInstance.class );
        session = testServer.getContext().getBean( Session.class );
        cc = testServer.getContext().getBean( CassandraConfiguration.class );
        aqs = new RequestQueryService( cc.getKeyspace(), session );
        hzRequests = new HazelcastRequestsManager( hazelcastInstance, aqs );

        Map<AceKey, Status> statusMap = RequestUtil.statusMap( ss );
        hzRequests.submitAll( statusMap );
    }

    @Test
    public void testSubmitAndGet() {
        Set<Status> expectedStatuses = ss.stream()
                .filter( status -> status.getPrincipal().equals( expected.getPrincipal() ) )
                .collect( Collectors.toSet() );

        long c = expectedStatuses.size();

        Set<Status> statuses = hzRequests
                .getStatuses( expected.getPrincipal(), expected.getStatus() )
                .collect( Collectors.toSet() );
        Assert.assertEquals( c, statuses.size() );
        Assert.assertEquals( expectedStatuses, statuses );
    }

    @Test
    public void testSubmitAndGetByPrincipalAndStatus() {
        Set<Status> expectedStatuses = ss.stream()
                .filter( status -> status.getPrincipal().equals( expected.getPrincipal() ) && status.getStatus()
                        .equals( expected.getStatus() ) )
                .collect( Collectors.toSet() );
        long c = expectedStatuses.size();
        Set<Status> statuses = hzRequests
                .getStatuses( expected.getPrincipal(), expected.getStatus() )
                .collect(
                Collectors.toSet());
        Assert.assertEquals( c, statuses.size() );
        Assert.assertEquals( expectedStatuses, statuses );
    }

    @Test
    public void testSubmitAndGetByAclKey() {
        long c = ss.stream()
                .map( Status::getAclKey )
                .filter( aclKey -> aclKey.equals( expected.getAclKey() ) )
                .count();
        Assert.assertEquals( c, hzRequests.getStatusesForAllUser( expected.getAclKey() ).count() );
    }

    @Test
    public void testSubmitAndGetByAclKeyAndStatus() {
        long c = ss.stream()
                .filter( status -> status.getAclKey().equals( expected.getAclKey() ) && status.getStatus()
                        .equals( RequestStatus.SUBMITTED ) )
                .count();
        Assert.assertEquals( c,
                hzRequests.getStatusesForAllUser( expected.getAclKey(), RequestStatus.SUBMITTED ).count() );
    }
}
