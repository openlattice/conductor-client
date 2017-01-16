package com.dataloom.requests;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;

public interface PermissionsRequestsManager {
    
    // For users
    void upsertRequest( List<UUID> aclRoot, Principal principal, Map<UUID, EnumSet<Permission>> permissions);

    PermissionsRequest getUnresolvedRequestOfUser( List<UUID> aclRoot, Principal principal );

    Iterable<PermissionsRequest> getResolvedRequestsOfUser( List<UUID> aclRoot, Principal principal );
    
    // For admin
    void updateUnresolvedRequestStatus( List<UUID> aclRoot, Principal principal, RequestStatus status );

    // TODO Pagination
    Iterable<PermissionsRequest> getAllUnresolvedRequestsOfAdmin( List<UUID> aclRoot, EnumSet<RequestStatus> status );
    
    Iterable<PermissionsRequest> getAllUnresolvedRequestsOfAdmin( List<UUID> aclRoot );
    
}
