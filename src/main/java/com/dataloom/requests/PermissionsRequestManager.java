package com.dataloom.requests;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;

public interface PermissionsRequestManager {

    void upsertRequest( List<UUID> aclRoot, Principal principal, Map<UUID, EnumSet<Permission>> permissions);

    void updateRequestStatus( List<UUID> aclRoot, Principal principal, RequestStatus status );

    PermissionsRequest getUnresolvedRequest( List<UUID> aclRoot, Principal principal );

    // TODO Pagination
    Iterable<PermissionsRequest> getAllUnresolvedRequests( List<UUID> aclRoot, EnumSet<RequestStatus> status );
    
    Iterable<PermissionsRequest> getResolvedRequests( Principal principal, List<UUID> aclRoot );
    
}
