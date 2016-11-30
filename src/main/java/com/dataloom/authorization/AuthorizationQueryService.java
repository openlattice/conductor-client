package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.edm.internal.DatastoreConstants;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Iterables;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public class AuthorizationQueryService {
    private static final Logger                                logger = LoggerFactory
            .getLogger( AuthorizationQueryService.class );
    private final Session                                      session;
    private final PreparedStatement                            getAuthorizedAclKeysPrepStmt;
    private final PreparedStatement                            getAclsForSecurableObjectPrepStmt;
    private final IMap<AccessControlEntryKey, Set<Permission>> aces;

    public AuthorizationQueryService( Session session, HazelcastInstance hazelcastInstance ) {
        this.session = session;
        aces = hazelcastInstance.getMap( HazelcastAuthorizationService.ACES_MAP );

        getAuthorizedAclKeysPrepStmt = session.prepare( QueryBuilder
                .select( CommonColumns.SECURABLE_TYPE.cql(), CommonColumns.ID.cql() )
                .from( DatastoreConstants.KEYSPACE, "authz" )
                .where( QueryBuilder.eq( CommonColumns.PRINCIPAL_TYPE.cql(), QueryBuilder.bindMarker( "_type" ) ) )
                .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_ID.cql(), QueryBuilder.bindMarker( "_id" ) ) ) );

        getAclsForSecurableObjectPrepStmt = session.prepare( QueryBuilder
                .select( CommonColumns.PRINCIPAL_TYPE.cql(), CommonColumns.PRINCIPAL_ID.cql() )
                .from( DatastoreConstants.KEYSPACE, "authz" )
                .where( QueryBuilder.eq( CommonColumns.SECURABLE_TYPE.cql(), QueryBuilder.bindMarker( "_type" ) ) )
                .and( QueryBuilder.eq( CommonColumns.ID.cql(), QueryBuilder.bindMarker( "_id" ) ) ) );
    }

    public Iterable<AclKey> getAuthorizedAclKeys( Principal principal, Set<Permission> desiredPermissions ) {
        ResultSetFuture rsf = session.executeAsync(
                getAuthorizedAclKeysPrepStmt.bind( "_type", principal.getType(), "_id", principal.getName() ) );
        // Initial transform concat is to defer evaluation of getUninterruptibly()
        return Iterables.transform(
                Iterables.concat( Iterables.transform( Arrays.asList( rsf ), frs -> frs.getUninterruptibly() ) ),
                AuthorizationQueryService::getAclKeyFromRow );
    }

    public LazyAcl getAclsForSecurableObject( AclKey aclKey ) {
        ResultSetFuture rsf = session.executeAsync(
                getAclsForSecurableObjectPrepStmt.bind( "_type",
                        aclKey.getObjectType(),
                        "_id",
                        aclKey.getObjectId() ) );
        // Initial transform concat is to defer evaluation of getUninterruptibly()
        Iterable<Principal> principals = Iterables.transform(
                Iterables.concat( Iterables.transform( Arrays.asList( rsf ), frs -> frs.getUninterruptibly() ) ),
                AuthorizationQueryService::getPrincipalFromRow );
        Iterable<LazyAce> lazyAces = Iterables.transform( principals,
                principal -> new LazyAce( principal, aces.get(
                        new AccessControlEntryKey( aclKey.getObjectId(), aclKey.getObjectType(), principal ) ) ) );
        return new LazyAcl( aclKey, lazyAces );
    }

    private static AclKey getAclKeyFromRow( Row row ) {
        final String securableType = row.getString( CommonColumns.SECURABLE_TYPE.cql() );
        final UUID objectId = checkNotNull( row.getUUID( CommonColumns.ID.cql() ),
                "Securable object id cannot be null." );
        checkState( StringUtils.isNotBlank( securableType ), "Encountered blank securable type" );
        return new AclKey(
                SecurableObjectType.valueOf( securableType ),
                objectId );
    }

    private static Principal getPrincipalFromRow( Row row ) {
        final String principalType = row.getString( CommonColumns.PRINCIPAL_TYPE.cql() );
        final String principalId = row.getString( CommonColumns.PRINCIPAL_ID.cql() );
        checkState( StringUtils.isNotBlank( principalId ), "Securable object id cannot be null." );
        checkState( StringUtils.isNotBlank( principalType ), "Encountered blank securable type" );
        return new Principal(
                PrincipalType.valueOf( principalType ),
                principalId );
    }

}
