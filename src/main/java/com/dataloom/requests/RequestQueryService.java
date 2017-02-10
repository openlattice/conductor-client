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

import static com.kryptnostic.datastore.cassandra.CommonColumns.ACL_KEYS;
import static com.kryptnostic.datastore.cassandra.CommonColumns.PRINCIPAL_ID;
import static com.kryptnostic.datastore.cassandra.CommonColumns.PRINCIPAL_TYPE;
import static com.kryptnostic.datastore.cassandra.CommonColumns.STATUS;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.kryptnostic.conductor.rpc.odata.Tables;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class RequestQueryService {

    private final Session           session;
    private final PreparedStatement getRequestKeysForPrincipal;
    private final PreparedStatement getRequestKeysForPrincipalAndStatus;
    private final PreparedStatement getRequestKeysForAclKey;
    private final PreparedStatement getRequestKeysForAclKeyAndStatus;

    public RequestQueryService( String keyspace, Session session ) {
        this.session = session;
        getRequestKeysForPrincipal = session.prepare( keysForPrincipal( keyspace ) );
        getRequestKeysForPrincipalAndStatus = session.prepare( keysForPrincipalAndStatus( keyspace ) );
        getRequestKeysForAclKey = session.prepare( keysForAclKey( keyspace ) );
        getRequestKeysForAclKeyAndStatus = session.prepare( keysForAclKeyAndStatus( keyspace ) );
    }

    private static Select.Where keysForPrincipal( String keyspace ) {
        return QueryBuilder
                .select( ACL_KEYS.cql() )
                .from( keyspace, Tables.REQUESTS.getName() )
                .allowFiltering()
                .where( PRINCIPAL_TYPE.eq() )
                .and( PRINCIPAL_ID.eq() );
    }

    private static Select.Where keysForPrincipalAndStatus( String keyspace ) {
        return keysForPrincipal( keyspace ).and( STATUS.eq() );
    }

    private static Select.Where keysForAclKey( String keyspace ) {
        return QueryBuilder
                .select( ACL_KEYS.cql(), PRINCIPAL_TYPE.cql(), PRINCIPAL_ID.cql() )
                .from( keyspace, Tables.REQUESTS.getName() )
                .allowFiltering()
                .where( ACL_KEYS.eq() );
    }

    private static Select.Where keysForAclKeyAndStatus( String keyspace ) {
        return keysForAclKey( keyspace ).and( STATUS.eq() );
    }

    public Stream<AceKey> getRequestKeys( Principal principal ) {
        ResultSet rs = session
                .execute( getRequestKeysForPrincipal
                        .bind()
                        .set( PRINCIPAL_TYPE.cql(), principal.getType(), PrincipalType.class )
                        .setString( PRINCIPAL_ID.cql(), principal.getId() ) );
        return StreamUtil.stream( rs )
                .map( AuthorizationUtils::aclKey )
                .map( aclKey -> new AceKey( aclKey, principal ) );
    }

    public Stream<AceKey> getRequestKeys( Principal principal, RequestStatus requestStatus ) {
        ResultSet rs = session
                .execute( getRequestKeysForPrincipalAndStatus
                        .bind()
                        .set( PRINCIPAL_TYPE.cql(), principal.getType(), PrincipalType.class )
                        .setString( PRINCIPAL_ID.cql(), principal.getId() )
                        .set( STATUS.cql(), requestStatus, RequestStatus.class ) );
        return StreamUtil.stream( rs )
                .map( AuthorizationUtils::aclKey )
                .map( aclKey -> new AceKey( aclKey, principal ) );
    }

    public Stream<AceKey> getRequestKeys( List<UUID> aclKey ) {
        ResultSet rs = session.execute( getRequestKeysForAclKey
                .bind()
                .setList( ACL_KEYS.cql(), aclKey, UUID.class ) );
        return StreamUtil.stream( rs )
                .map( AuthorizationUtils::aceKey );
    }

    public Stream<AceKey> getRequestKeys( List<UUID> aclKey, RequestStatus requestStatus ) {
        ResultSet rs = session.execute( getRequestKeysForAclKeyAndStatus
                .bind()
                .setList( ACL_KEYS.cql(), aclKey, UUID.class )
                .set( STATUS.cql(), requestStatus, RequestStatus.class ) );
        return StreamUtil.stream( rs )
                .map( AuthorizationUtils::aceKey );
    }
}
