package com.dataloom.requests;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;

public interface PermissionsRequestManager {

    void upsertRequest( List<AclKeyPathFragment> aclRoot, Principal principal, Map<AclKeyPathFragment, EnumSet<Permission>> permissions);

    void updateRequestStatus( List<AclKeyPathFragment> aclRoot, Principal principal, RequestStatus status );

    PermissionsRequest getUnresolvedRequest( List<AclKeyPathFragment> aclRoot, Principal principal );

    // TODO Pagination
    Iterable<PermissionsRequest> getAllUnresolvedRequests( List<AclKeyPathFragment> aclRoot, EnumSet<RequestStatus> status );
    
    Iterable<PermissionsRequest> getResolvedRequests( Principal principal, List<AclKeyPathFragment> aclRoot );
    
}
