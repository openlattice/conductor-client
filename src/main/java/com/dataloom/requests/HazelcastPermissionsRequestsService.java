package com.dataloom.requests;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.requests.mapstores.AclRootPrincipalPair;
import com.dataloom.requests.mapstores.PrincipalRequestIdPair;
import com.dataloom.requests.processors.UpdateRequestStatusProcessor;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastPermissionsRequestsService implements PermissionsRequestsManager {
    private static final Logger                                           logger = LoggerFactory
            .getLogger( PermissionsRequestsManager.class );
    private final PermissionsRequestsQueryService                         prqs;
    private final AuthorizationManager                                    authorizations;

    private final IMap<AclRootPrincipalPair, PermissionsRequestDetails>   unresolvedPRs;
    private final IMap<PrincipalRequestIdPair, AclRootRequestDetailsPair> resolvedPRs;

    public HazelcastPermissionsRequestsService(
            HazelcastInstance hazelcastInstance,
            PermissionsRequestsQueryService prqs,
            AuthorizationManager authorizations ) {
        unresolvedPRs = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS_REQUESTS_UNRESOLVED.name() );
        resolvedPRs = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS_REQUESTS_RESOLVED.name() );
        this.prqs = Preconditions.checkNotNull( prqs );
        this.authorizations = authorizations;
    }

    @Override
    public void upsertRequest(
            List<UUID> aclRoot,
            Principal principal,
            Map<UUID, EnumSet<Permission>> permissions ) {
        unresolvedPRs.put( new AclRootPrincipalPair( aclRoot, principal ), new PermissionsRequestDetails(
                permissions,
                RequestStatus.SUBMITTED ) );

    }

    @Override
    public void updateUnresolvedRequestStatus( List<UUID> aclRoot, Principal principal, RequestStatus status ) {
        switch ( status ) {
            case APPROVED:
                approveRequest( aclRoot, principal );
                archiveRequest( aclRoot, principal, status );
                break;
            case DECLINED:
                archiveRequest( aclRoot, principal, status );
                break;
            default:
                unresolvedPRs.executeOnKey( new AclRootPrincipalPair( aclRoot, principal ),
                        new UpdateRequestStatusProcessor( status ) );
                break;
        }
    }

    private void approveRequest( List<UUID> aclRoot, Principal principal ) {
        PermissionsRequestDetails details = unresolvedPRs.get( new AclRootPrincipalPair( aclRoot, principal ) );
        Preconditions.checkNotNull( details, "Permissions request does not exist." );

        UUID objectId = AuthorizationUtils.getLastAclKeySafely( aclRoot );
        Map<UUID, EnumSet<Permission>> permissions = details.getPermissions();
        // When permissions of aclRoot itself is requested
        if ( objectId != null && permissions.containsKey( objectId ) ) {
            authorizations.addPermission( aclRoot, principal, permissions.get( objectId ) );
            permissions.remove( objectId );
        }
        // When permissions of children of aclRoot is requested
        for ( Map.Entry<UUID, EnumSet<Permission>> entry : permissions.entrySet() ) {
            List<UUID> objectWithChild = new ArrayList<>( aclRoot );
            objectWithChild.add( entry.getKey() );
            authorizations.addPermission( objectWithChild, principal, entry.getValue() );
        }
    }

    private void archiveRequest( List<UUID> aclRoot, Principal principal, RequestStatus status ) {
        PermissionsRequestDetails details = unresolvedPRs.remove( new AclRootPrincipalPair( aclRoot, principal ) );
        Preconditions.checkNotNull( details, "Permissions request does not exist." );
        details.setStatus( status );
        resolvedPRs.put( new PrincipalRequestIdPair( principal, UUIDs.timeBased() ),
                new AclRootRequestDetailsPair( aclRoot, details ) );
    }

    @Override
    public PermissionsRequest getUnresolvedRequestOfUser(
            List<UUID> aclRoot,
            Principal principal ) {
        PermissionsRequestDetails details = unresolvedPRs.get( new AclRootPrincipalPair( aclRoot, principal ) );
        Preconditions.checkNotNull( details, "No outstanding permission requests for this object." );
        return new PermissionsRequest( aclRoot, principal, details );
    }

    @Override
    public Iterable<PermissionsRequest> getAllUnresolvedRequestsOfAdmin(
            List<UUID> aclRoot,
            EnumSet<RequestStatus> status ) {
        return prqs.getAllUnresolvedRequestsOfAdmin( aclRoot, status );
    }

    @Override
    public Iterable<PermissionsRequest> getAllUnresolvedRequestsOfAdmin( List<UUID> aclRoot ) {
        return prqs.getAllUnresolvedRequestsOfAdmin( aclRoot );
    }

    @Override
    public Iterable<PermissionsRequest> getResolvedRequestsOfUser(
            List<UUID> aclRoot,
            Principal principal ) {
        return prqs.getResolvedRequestsOfUser( aclRoot, principal );
    }

}
