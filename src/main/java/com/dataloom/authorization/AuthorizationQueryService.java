package com.dataloom.authorization;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.util.CassandraMappingUtils;
import com.dataloom.edm.internal.DatastoreConstants;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Iterables;
import com.hazelcast.core.HazelcastInstance;
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
                        CommonColumns.PRINCIPAL_TYPE.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_ID.cql(),
                        CommonColumns.PRINCIPAL_ID.bindMarker() ) ) );

        aclsForSecurableObjectQuery = session.prepare( QueryBuilder
                .select( CommonColumns.PRINCIPAL_TYPE.cql(), CommonColumns.PRINCIPAL_ID.cql() )
                .from( DatastoreConstants.KEYSPACE, "authz" )
                .where( QueryBuilder.eq( CommonColumns.SECURABLE_OBJECT_TYPE.cql(),
                        CommonColumns.SECURABLE_OBJECT_TYPE.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.SECURABLE_OBJECTID.cql(),
                        CommonColumns.SECURABLE_OBJECTID.bindMarker() ) ) );
    }

    public Iterable<AclKey> getAuthorizedAclKeys( Principal principal, Set<Permission> desiredPermissions ) {
        ResultSetFuture rsf = session.executeAsync(
                authorizedAclKeysQuery.bind( CommonColumns.PRINCIPAL_TYPE.bindMarker(),
                        principal.getType(),
                        CommonColumns.PRINCIPAL_ID.bindMarker(),
                        principal.getName() ) );
        return Iterables.transform( CassandraMappingUtils.makeLazy( rsf ), CassandraMappingUtils::getAclKeyFromRow );
    }

    public Acl getAclsForSecurableObject( AclKey aclKey ) {
        ResultSetFuture rsf = session.executeAsync(
                aclsForSecurableObjectQuery.bind( CommonColumns.SECURABLE_OBJECT_TYPE.bindMarker(),
                        aclKey.getObjectType(),
                        CommonColumns.SECURABLE_OBJECTID.bindMarker(),
                        aclKey.getObjectId() ) );

        Iterable<Principal> principals = Iterables.transform( CassandraMappingUtils.makeLazy( rsf ),
                CassandraMappingUtils::getPrincipalFromRow );
        Iterable<AceFuture> futureAces = Iterables.transform( principals,
                principal -> new AceFuture( principal, aces.getAsync(
                        new AceKey( aclKey.getObjectId(), aclKey.getObjectType(), principal ) ) ) );
        return new Acl( aclKey, Iterables.transform( futureAces, futureAce -> futureAce.getUninterruptibly() ) );
    }

}
