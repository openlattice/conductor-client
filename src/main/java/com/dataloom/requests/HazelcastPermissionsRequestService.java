package com.dataloom.requests;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.requests.mapstores.AclRootRequestDetailsPair;
import com.dataloom.requests.mapstores.AclRootUserIdPair;
import com.dataloom.requests.mapstores.UserIdRequestIdPair;
import com.dataloom.requests.processors.UpdateRequestStatusProcessor;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastPermissionsRequestService implements PermissionsRequestManager {
    private static final Logger                                        logger                              = LoggerFactory
            .getLogger( PermissionsRequestManager.class );
    private final PermissionsRequestQueryService                      prqs;

    public static final String                                         UNRESOLVED_PERMISSIONS_REQUESTS_MAP = "permissions_requests_unresolved";
    public static final String                                         RESOLVED_PERMISSIONS_REQUESTS_MAP   = "permissions_requests_resolved";

    private final IMap<AclRootUserIdPair, PermissionsRequestDetails>   unresolvedPRs;
    private final IMap<UserIdRequestIdPair, AclRootRequestDetailsPair> resolvedPRs;

    public HazelcastPermissionsRequestService(
            HazelcastInstance hazelcastInstance,
            PermissionsRequestQueryService prqs ) {
        unresolvedPRs = hazelcastInstance.getMap( UNRESOLVED_PERMISSIONS_REQUESTS_MAP );
        resolvedPRs = hazelcastInstance.getMap( RESOLVED_PERMISSIONS_REQUESTS_MAP );
        this.prqs = Preconditions.checkNotNull( prqs );
    }

    @Override
    public void upsertRequest(
            List<UUID> aclRoot,
            Principal principal,
            Map<UUID, EnumSet<Permission>> permissions ) {
        unresolvedPRs.put( new AclRootUserIdPair( aclRoot, principal.getId() ), new PermissionsRequestDetails(
                permissions,
                RequestStatus.SUBMITTED ) );

    }

    @Override
    public void updateRequestStatus( List<UUID> aclRoot, Principal principal, RequestStatus status ) {
        switch ( status ) {
            case APPROVED:
            case DECLINED:
                resolveRequest( aclRoot, principal, status );
                break;
            default:
                unresolvedPRs.executeOnKey( new AclRootUserIdPair( aclRoot, principal.getId() ),
                        new UpdateRequestStatusProcessor( status ) );
                break;
        }
    }

    private void resolveRequest( List<UUID> aclRoot, Principal principal, RequestStatus status ) {
        PermissionsRequestDetails details = unresolvedPRs.remove( new AclRootUserIdPair( aclRoot, principal.getId() ) );
        Preconditions.checkNotNull( details, "Permissions request does not exist." );
        details.setStatus( status );
        resolvedPRs.put( new UserIdRequestIdPair( principal.getId(), UUIDs.timeBased() ),
                new AclRootRequestDetailsPair( aclRoot, details ) );
    }

    @Override
    public PermissionsRequest getUnresolvedRequest(
            List<UUID> aclRoot,
            Principal principal ) {
        PermissionsRequestDetails details = unresolvedPRs.get( new AclRootUserIdPair( aclRoot, principal.getId() ) );
        Preconditions.checkNotNull( details, "No outstanding permission requests for this object." );
        return new PermissionsRequest( aclRoot, principal.getId(), details );
    }

    @Override
    public Iterable<PermissionsRequest> getAllUnresolvedRequests(
            List<UUID> aclRoot,
            EnumSet<RequestStatus> status ) {
        return prqs.getAllUnresolvedRequests( aclRoot, status );
    }

    @Override
    public Iterable<PermissionsRequest> getResolvedRequests(
            Principal principal,
            List<UUID> aclRoot) {
        return prqs.getResolvedRequests( principal, aclRoot );
    }

}
