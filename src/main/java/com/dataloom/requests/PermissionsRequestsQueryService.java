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

import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Principal;
import com.dataloom.requests.util.PermissionsRequestsUtils;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public class PermissionsRequestsQueryService {
    private static final Logger     logger = LoggerFactory
            .getLogger( PermissionsRequestsQueryService.class );

    private final Session           session;
    private final PreparedStatement unresolvedPRsQuery;
    private final PreparedStatement unresolvedPRsQueryByStatus;
    private final PreparedStatement resolvedPRsQuery;

    public PermissionsRequestsQueryService( Session session ) {
        this.session = session;
        unresolvedPRsQuery = session.prepare( Tables.PERMISSIONS_REQUESTS_UNRESOLVED.getBuilder().buildLoadAllQuery()
                .where( QueryBuilder.eq( CommonColumns.ACL_ROOT.cql(), CommonColumns.ACL_ROOT.bindMarker() ) ) );
        unresolvedPRsQueryByStatus = session
                .prepare( Tables.PERMISSIONS_REQUESTS_UNRESOLVED.getBuilder().buildLoadAllQuery()
                        .where( QueryBuilder.eq( CommonColumns.ACL_ROOT.cql(), CommonColumns.ACL_ROOT.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.STATUS.cql(), CommonColumns.STATUS.bindMarker() ) ) );
        resolvedPRsQuery = session.prepare( Tables.PERMISSIONS_REQUESTS_RESOLVED.getBuilder().buildLoadAllQuery()
                .where( QueryBuilder.eq( CommonColumns.PRINCIPAL_ID.cql(), CommonColumns.PRINCIPAL_ID.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.ACL_ROOT.cql(), CommonColumns.ACL_ROOT.bindMarker() ) ) );
    }

    public Iterable<PermissionsRequest> getAllUnresolvedRequestsOfAdmin(
            List<UUID> aclRoot ) {
        ResultSetFuture rsf = session.executeAsync( unresolvedPRsQuery.bind()
                .setList( CommonColumns.ACL_ROOT.cql(), aclRoot, UUID.class ) );
        return Iterables.transform( StreamUtil.makeLazy( rsf ),
                PermissionsRequestsUtils::getPRFromRow )::iterator;
    }

    public Iterable<PermissionsRequest> getAllUnresolvedRequestsOfAdmin(
            List<UUID> aclRoot,
            EnumSet<RequestStatus> status ) {
        Stream<ResultSetFuture> rsfs = status.stream()
                .map( st -> session.executeAsync( unresolvedPRsQueryByStatus.bind()
                        .setList( CommonColumns.ACL_ROOT.cql(), aclRoot, UUID.class )
                        .set( CommonColumns.STATUS.cql(), st, RequestStatus.class ) ) );
        return StreamUtil.getRowsAndFlatten( rsfs )
                .map( PermissionsRequestsUtils::getPRFromRow )::iterator;
    }

    public Iterable<PermissionsRequest> getResolvedRequestsOfUser(
            List<UUID> aclRoot,
            Principal principal ) {
        ResultSetFuture rsf = session.executeAsync( resolvedPRsQuery.bind()
                .setString( CommonColumns.PRINCIPAL_ID.cql(), principal.getId() )
                .setList( CommonColumns.ACL_ROOT.cql(), aclRoot, UUID.class ) );
        return Iterables.transform( StreamUtil.makeLazy( rsf ), PermissionsRequestsUtils::getPRFromRow )::iterator;
    }

}
