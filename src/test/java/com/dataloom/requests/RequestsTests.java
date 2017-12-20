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

package com.dataloom.requests;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.HzAuthzTest;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.requests.util.RequestUtil;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.AclKey;
import com.openlattice.postgres.mapstores.SecurableObjectTypeMapstore;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class RequestsTests extends HzAuthzTest {
    protected static final RequestQueryService      aqs;
    protected static final HazelcastRequestsManager hzRequests;
    protected static final Lock        lock      = new ReentrantLock();
    protected static final Status      expected  = TestDataFactory.status();
    protected static final Status      expected2 = new Status(
            expected.getRequest(),
            TestDataFactory.userPrincipal(),
            RequestStatus.SUBMITTED );
    protected static final Status      expected3 = new Status(
            expected.getRequest(),
            TestDataFactory.userPrincipal(),
            RequestStatus.SUBMITTED );
    protected static final Status      expected4 = new Status(
            TestDataFactory.aclKey(),
            expected.getRequest().getPermissions(),
            expected.getRequest().getReason(),
            expected.getPrincipal(),
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
        IMap<AclKey, SecurableObjectType> objectTypes = hazelcastInstance
                .getMap( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );
        ss.forEach( s -> objectTypes.set( s.getRequest().getAclKey(), SecurableObjectType.PropertyTypeInEntitySet ) );
        aqs = new RequestQueryService( hds );
        hzRequests = new HazelcastRequestsManager( hazelcastInstance, aqs, neuron );
        Map<AceKey, Status> statusMap = RequestUtil.statusMap( ss );
        SecurableObjectTypeMapstore objectTypes = new SecurableObjectTypeMapstore( hds );
        hzRequests.submitAll( statusMap );
    }

    @Test
    public void testSubmitAndGet() {
        Set<Status> expectedStatuses = ss.stream()
                .filter( status -> status.getPrincipal().equals( expected.getPrincipal() ) && status.getStatus()
                        .equals( expected.getStatus() ) )
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
                .map( Status::getRequest )
                .map( Request::getAclKey )
                .filter( aclKey -> aclKey.equals( expected.getRequest().getAclKey() ) )
                .count();
        Assert.assertEquals( c, hzRequests.getStatusesForAllUser( expected.getRequest().getAclKey() ).count() );
    }

    @Test
    public void testSubmitAndGetByAclKeyAndStatus() {
        long c = ss.stream()
                .filter( status -> status.getRequest().getAclKey().equals( expected.getRequest().getAclKey() )
                        && status.getStatus()
                        .equals( RequestStatus.SUBMITTED ) )
                .count();
        Assert.assertEquals( c,
                hzRequests.getStatusesForAllUser( expected.getRequest().getAclKey(), RequestStatus.SUBMITTED )
                        .count() );
    }

    @Test
    public void testApproval() {
        Assert.assertTrue( submitted.stream().allMatch( s -> !hzAuthz
                .checkIfHasPermissions( new AclKey( s.getRequest().getAclKey() ),
                        ImmutableSet.of( s.getPrincipal() ),
                        s.getRequest().getPermissions() ) ) );
        ;
        hzRequests.submitAll( RequestUtil
                .statusMap( submitted.stream().map( RequestUtil::approve ).collect( Collectors.toSet() ) ) );

        Assert.assertTrue( submitted.stream().allMatch( s -> hzAuthz
                .checkIfHasPermissions( new AclKey( s.getRequest().getAclKey() ),
                        ImmutableSet.of( s.getPrincipal() ),
                        s.getRequest().getPermissions() ) ) );

    }
}
