package com.dataloom.requests.util;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.requests.PermissionsRequest;
import com.dataloom.requests.PermissionsRequestDetails;
import com.dataloom.requests.RequestStatus;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class PermissionsRequestsUtils {
    private PermissionsRequestsUtils() {}

    public static PermissionsRequest getPRFromRow( Row row ) {
        final List<UUID> aclRoot = RowAdapters.aclRoot( row );
        final Principal user = new Principal( PrincipalType.USER, RowAdapters.principalId( row ) );
        Map<UUID, EnumSet<Permission>> permissions = RowAdapters.aclChildrenPermissions( row );
        RequestStatus status = RowAdapters.reqStatus( row );
        return new PermissionsRequest( aclRoot, user, new PermissionsRequestDetails( permissions, status ) );
    }

}
