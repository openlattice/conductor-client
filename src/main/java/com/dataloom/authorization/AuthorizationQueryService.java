package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public class AuthorizationQueryService {
    private static final Logger                 logger = LoggerFactory
            .getLogger( AuthorizationQueryService.class );
    private final Session                       session;
    private final PreparedStatement             authorizedAclKeysQuery;
    private final PreparedStatement             aclsForSecurableObjectQuery;
    private final IMap<AceKey, Set<Permission>> aces;

    public AuthorizationQueryService( Session session, HazelcastInstance hazelcastInstance ) {
        this.session = session;
        aces = hazelcastInstance.getMap( HazelcastAuthorizationService.ACES_MAP );

        authorizedAclKeysQuery = session.prepare( QueryBuilder
                .select( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), CommonColumns.SECURABLE_OBJECTID.cql() )
                .from( DatastoreConstants.KEYSPACE, "authz" )
                .where( QueryBuilder.eq( CommonColumns.PRINCIPAL_TYPE.cql(),
                        QueryBuilder.bindMarker( CommonColumns.PRINCIPAL_TYPE.bindMarker() ) ) )
                .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_ID.cql(),
                        QueryBuilder.bindMarker( CommonColumns.PRINCIPAL_ID.bindMarker() ) ) ) );

        aclsForSecurableObjectQuery = session.prepare( QueryBuilder
                .select( CommonColumns.PRINCIPAL_TYPE.cql(), CommonColumns.PRINCIPAL_ID.cql() )
                .from( DatastoreConstants.KEYSPACE, "authz" )
                .where( QueryBuilder.eq( CommonColumns.SECURABLE_OBJECT_TYPE.cql(),
                        QueryBuilder.bindMarker( CommonColumns.SECURABLE_OBJECT_TYPE.bindMarker() ) ) )
                .and( QueryBuilder.eq( CommonColumns.SECURABLE_OBJECTID.cql(),
                        QueryBuilder.bindMarker( CommonColumns.SECURABLE_OBJECTID.bindMarker() ) ) ) );
    }

    public Iterable<AclKey> getAuthorizedAclKeys( Principal principal, Set<Permission> desiredPermissions ) {
        ResultSetFuture rsf = session.executeAsync(
                authorizedAclKeysQuery.bind( CommonColumns.PRINCIPAL_TYPE.bindMarker(),
                        principal.getType(),
                        CommonColumns.PRINCIPAL_ID.bindMarker(),
                        principal.getName() ) );
        return Iterables.transform( makeLazy( rsf ), AuthorizationQueryService::getAclKeyFromRow );
    }

    public LazyAcl getAclsForSecurableObject( AclKey aclKey ) {
        ResultSetFuture rsf = session.executeAsync(
                aclsForSecurableObjectQuery.bind( "_type",
                        aclKey.getObjectType(),
                        "_id",
                        aclKey.getObjectId() ) );

        Iterable<Principal> principals = Iterables.transform( makeLazy( rsf ),
                AuthorizationQueryService::getPrincipalFromRow );
                Iterables.transform( principals,
                principal -> new AceFuture( principal, aces.getAsync(
                        new AceKey( aclKey.getObjectId(), aclKey.getObjectType(), principal ) ) ) );
        return new LazyAcl( aclKey, lazyAces );
    }

    private static AclKey getAclKeyFromRow( Row row ) {
        final String securableType = row.getString( CommonColumns.SECURABLE_OBJECT_TYPE.cql() );
        checkState( StringUtils.isNotBlank( securableType ), "Encountered blank securable type" );
        final UUID objectId = checkNotNull( row.getUUID( CommonColumns.SECURABLE_OBJECTID.cql() ),
                "Securable object id cannot be null." );
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

    /**
     * Useful adapter for {@code Iterables#transform(Iterable, com.google.common.base.Function)} that allows lazy
     * evaluation of result set future.
     * 
     * @param rsf The result set future to make a lazy evaluated iterator
     * @return The lazy evaluatable iteratable
     */
    private static Iterable<Row> makeLazy( ResultSetFuture rsf ) {
        return rsf.getUninterruptibly()::iterator;
    }

}
