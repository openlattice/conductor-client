package com.dataloom.requests;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.Principal;
import com.dataloom.hazelcast.pods.MapstoresPod;
import com.dataloom.hazelcast.pods.SharedStreamSerializersPod;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.requests.util.RequestUtil;
import com.datastax.driver.core.Session;
import com.geekbeast.rhizome.tests.bootstrap.CassandraBootstrap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.codecs.pods.TypeCodecsPod;
import com.kryptnostic.datastore.cassandra.CassandraTablesPod;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.pods.CassandraPod;

public class RequestsTests extends CassandraBootstrap {
    protected static final RhizomeApplicationServer testServer;
    protected static final HazelcastInstance        hazelcastInstance;
    protected static final Session                  session;
    protected static final CassandraConfiguration   cc;
    protected static final RequestQueryService      aqs;
    protected static final HazelcastRequestsManager hzRequests;

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

    }

    @Test
    public void testSubmitAndGet() {
        Set<Status> ss = ImmutableSet.of( TestDataFactory.status(),
                TestDataFactory.status(),
                TestDataFactory.status() );
        Set<Principal> principals = ss.stream().map( Status::getPrincipal ).collect( Collectors.toSet() );
        Map<AceKey, Status> statusMap = RequestUtil.statusMap( ss );
        hzRequests.submitAll( statusMap );
        Stream<Status> statuses = hzRequests.getStatuses( principals.iterator().next() );
        Set<Status> retrieved = statuses.collect( Collectors.toSet() );
        Assert.assertTrue( retrieved.stream().anyMatch( ss::contains ) );
    }

    @Test
    public void testSubmitAndGetByPrincipalAndStatus() {
        Set<Status> ss = ImmutableSet.of( TestDataFactory.status(),
                TestDataFactory.status(),
                TestDataFactory.status() );
        Map<AceKey, Status> statusMap = RequestUtil.statusMap( ss );
        hzRequests.submitAll( statusMap );
        Multiset<RequestStatus> ms = HashMultiset.create();
        for ( Status s : ss ) {
            ms.add( s.getStatus() );
        }
        Status first = ss.iterator().next();
        Stream<Status> statuses = hzRequests.getStatuses( first.getPrincipal(), first.getStatus() );
        Set<Status> retrieved = statuses.collect( Collectors.toSet() );
        Assert.assertEquals( ms.count( first.getStatus() ), retrieved.size() );
    }

    @Test
    public void testSubmitAndGetByAclKey() {
        Status expected = TestDataFactory.status();
        Status expected2 = new Status(
                expected.getAclKey(),
                TestDataFactory.userPrincipal(),
                expected.getPermissions(),
                RequestStatus.SUBMITTED );
        Set<Status> ss = ImmutableSet.of( expected,
                expected2,
                TestDataFactory.status(),
                TestDataFactory.status(),
                TestDataFactory.status() );
        Map<AceKey, Status> statusMap = RequestUtil.statusMap( ss );
        long c = ss.stream().map( Status::getStatus ).filter( RequestStatus.SUBMITTED::equals ).count();
        hzRequests.submitAll( statusMap );
        Assert.assertEquals( c, hzRequests.getStatusesForAllUser( expected.getAclKey() ).count() );
    }

    @Test
    public void testSubmitAndGetByAclKeyAndStatus() {
        Status expected = TestDataFactory.status();
        Status expected2 = new Status(
                expected.getAclKey(),
                TestDataFactory.userPrincipal(),
                expected.getPermissions(),
                RequestStatus.SUBMITTED );
        Status expected3 = new Status(
                expected.getAclKey(),
                TestDataFactory.userPrincipal(),
                expected.getPermissions(),
                RequestStatus.SUBMITTED );
        Set<Status> ss = ImmutableSet.of( expected,
                expected2,
                expected3,
                TestDataFactory.status(),
                TestDataFactory.status(),
                TestDataFactory.status() );
        long c = ss.stream().map( Status::getStatus ).filter( RequestStatus.SUBMITTED::equals ).count();
        Map<AceKey, Status> statusMap = RequestUtil.statusMap( ss );
        hzRequests.submitAll( statusMap );
        Assert.assertEquals( c,
                hzRequests.getStatusesForAllUser( expected.getAclKey(), RequestStatus.SUBMITTED ).count() );
    }
    
    @Test
    public void testUpdateStatus() {
        
    }
}
