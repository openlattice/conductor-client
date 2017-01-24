package com.dataloom.requests;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.HzAuthzTest;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.requests.util.RequestUtil;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class RequestsTests extends HzAuthzTest {
    protected static final RequestQueryService      aqs;
    protected static final HazelcastRequestsManager hzRequests;
    protected static final Lock        lock      = new ReentrantLock();
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
            expected4,
            TestDataFactory.status(),
            TestDataFactory.status(),
            TestDataFactory.status() );
    protected static final Set<Status> submitted = ImmutableSet.of(
            expected2,
            expected3,
            expected4 );

    static {
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
                        Collectors.toSet() );
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

    @Test
    public void testApproval() {
        Assert.assertTrue( submitted.stream().allMatch( s -> !hzAuthz
                .checkIfHasPermissions( s.getAclKey(), ImmutableSet.of( s.getPrincipal() ), s.getPermissions() ) ) );
        ;
        hzRequests.submitAll( RequestUtil
                .statusMap( submitted.stream().map( RequestUtil::approve ).collect( Collectors.toSet() ) ) );

        Assert.assertTrue( submitted.stream().allMatch( s -> hzAuthz
                .checkIfHasPermissions( s.getAclKey(), ImmutableSet.of( s.getPrincipal() ), s.getPermissions() ) ) );

    }
}
