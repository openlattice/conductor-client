package com.dataloom.authorization;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.mapstores.PermissionMapstore;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
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
    private final PreparedStatement             authorizedAclKeysForObjectTypeQuery;
    private final PreparedStatement             aclsForSecurableObjectQuery;
    private final IMap<AceKey, Set<Permission>> aces;

    public AuthorizationQueryService( Session session, HazelcastInstance hazelcastInstance ) {
        this.session = session;
        aces = hazelcastInstance.getMap( HazelcastAuthorizationService.ACES_MAP );
        authorizedAclKeysQuery = session.prepare( QueryBuilder
                .select( CommonColumns.ACL_KEYS.cql() )
                .from( DatastoreConstants.KEYSPACE, PermissionMapstore.MAP_NAME ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.PRINCIPAL_TYPE.cql(),
                        CommonColumns.PRINCIPAL_TYPE.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_ID.cql(),
                        CommonColumns.PRINCIPAL_ID.bindMarker() ) )
                .and( QueryBuilder.contains( CommonColumns.PERMISSIONS.cql(),
                        CommonColumns.PERMISSIONS.bindMarker() ) ) );

        authorizedAclKeysForObjectTypeQuery = session.prepare( QueryBuilder
                .select( CommonColumns.ACL_KEYS.cql() )
                .from( DatastoreConstants.KEYSPACE, PermissionMapstore.MAP_NAME ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.PRINCIPAL_TYPE.cql(),
                        CommonColumns.PRINCIPAL_TYPE.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_ID.cql(),
                        CommonColumns.PRINCIPAL_ID.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.SECURABLE_OBJECT_TYPE.cql(),
                        CommonColumns.SECURABLE_OBJECT_TYPE.bindMarker() ) )
                .and( QueryBuilder.contains( CommonColumns.PERMISSIONS.cql(),
                        CommonColumns.PERMISSIONS.bindMarker() ) ) );

        aclsForSecurableObjectQuery = session.prepare( QueryBuilder
                .select( CommonColumns.PRINCIPAL_TYPE.cql(), CommonColumns.PRINCIPAL_ID.cql() )
                .from( DatastoreConstants.KEYSPACE, PermissionMapstore.MAP_NAME ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(),
                        CommonColumns.SECURABLE_OBJECT_TYPE.bindMarker() ) ) );

    }

    public Iterable<List<AclKey>> getAuthorizedAclKeys(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> desiredPermissions ) {
        ResultSetFuture rsf = session.executeAsync(
                authorizedAclKeysForObjectTypeQuery.bind()
                        .set( CommonColumns.PRINCIPAL_TYPE.cql(), principal.getType(), PrincipalType.class )
                        .setString( CommonColumns.PRINCIPAL_ID.cql(), principal.getId() )
                        .set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), objectType, SecurableObjectType.class )
                        .setSet( CommonColumns.PERMISSIONS.cql(), desiredPermissions ) );
        return Iterables.transform( CassandraMappingUtils.makeLazy( rsf ), CassandraMappingUtils::getAclKeysFromRow );
    }

    public Iterable<List<AclKey>> getAuthorizedAclKeys( Principal principal, EnumSet<Permission> desiredPermissions ) {
        ResultSetFuture rsf = session.executeAsync(
                authorizedAclKeysQuery.bind()
                        .set( CommonColumns.PRINCIPAL_TYPE.cql(), principal.getType(), PrincipalType.class )
                        .setString( CommonColumns.PRINCIPAL_ID.cql(), principal.getId() )
                        .setSet( CommonColumns.PERMISSIONS.cql(), desiredPermissions ) );
        return Iterables.transform( CassandraMappingUtils.makeLazy( rsf ), CassandraMappingUtils::getAclKeysFromRow );
    }

    public Acl getAclsForSecurableObject( List<AclKey> aclKey ) {
        ResultSetFuture rsf = session.executeAsync(
                aclsForSecurableObjectQuery.bind().setList( CommonColumns.ACL_KEYS.cql(),
                        aclKey ) );

        Iterable<Principal> principals = Iterables.transform( CassandraMappingUtils.makeLazy( rsf ),
                CassandraMappingUtils::getPrincipalFromRow );
        Iterable<AceFuture> futureAces = Iterables.transform( principals,
                principal -> new AceFuture( principal, aces.getAsync(
                        new AceKey( aclKey, principal ) ) ) );
        return new Acl( aclKey, Iterables.transform( futureAces, AceFuture::getUninterruptibly ) );
    }

}
