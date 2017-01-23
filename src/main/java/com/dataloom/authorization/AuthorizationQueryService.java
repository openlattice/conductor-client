package com.dataloom.authorization;

import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AuthorizationQueryService {
    private static final Logger logger = LoggerFactory
            .getLogger( AuthorizationQueryService.class );
    private final Session                                  session;
    private final PreparedStatement                        authorizedAclKeysQuery;
    private final PreparedStatement                        authorizedAclKeysForObjectTypeQuery;
    private final PreparedStatement                        aclsForSecurableObjectQuery;
    private final PreparedStatement                        setObjectType;
    private final IMap<AceKey, DelegatedPermissionEnumSet> aces;

    public AuthorizationQueryService( String keyspace, Session session, HazelcastInstance hazelcastInstance ) {
        this.session = session;
        aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );
        authorizedAclKeysQuery = session.prepare( QueryBuilder
                .select( CommonColumns.ACL_KEYS.cql() )
                .from( keyspace, Tables.PERMISSIONS.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.PRINCIPAL_TYPE.cql(),
                        CommonColumns.PRINCIPAL_TYPE.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_ID.cql(),
                        CommonColumns.PRINCIPAL_ID.bindMarker() ) )
                .and( QueryBuilder.contains( CommonColumns.PERMISSIONS.cql(),
                        CommonColumns.PERMISSIONS.bindMarker() ) ) );

        authorizedAclKeysForObjectTypeQuery = session.prepare( QueryBuilder
                .select( CommonColumns.ACL_KEYS.cql() )
                .from( keyspace, Tables.PERMISSIONS.getName() ).allowFiltering()
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
                .from( keyspace, Tables.PERMISSIONS.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(),
                        CommonColumns.ACL_KEYS.bindMarker() ) ) );

        setObjectType = session.prepare( QueryBuilder
                .update( keyspace, Tables.PERMISSIONS.getName() )
                .with( QueryBuilder.set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(),
                        CommonColumns.SECURABLE_OBJECT_TYPE.bindMarker() ) )
                .where( QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(), CommonColumns.ACL_KEYS.bindMarker() ) ) );

    }

    public Stream<List<UUID>> getAuthorizedAclKeys(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> desiredPermissions ) {
        return desiredPermissions
                .stream()
                .map( desiredPermission ->
                        authorizedAclKeysForObjectTypeQuery.bind()
                                .set( CommonColumns.PRINCIPAL_TYPE.cql(), principal.getType(), PrincipalType.class )
                                .setString( CommonColumns.PRINCIPAL_ID.cql(), principal.getId() )
                                .set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), objectType, SecurableObjectType.class )
                                .set( CommonColumns.PERMISSIONS.cql(),
                                        desiredPermission,
                                        Permission.class ) )
                .peek( q -> logger.info( "Executing query: {}", q.toString() ) )
                .map( session::executeAsync )
                .map( ResultSetFuture::getUninterruptibly )
                .flatMap( StreamUtil::stream )
                .map( AuthorizationUtils::aclKey );
    }

    public Set<List<UUID>> getAuthorizedAclKeys(
            Set<Principal> principals,
            SecurableObjectType objectType,
            EnumSet<Permission> desiredPermissions ) {
        return principals
                .stream()
                .flatMap( principal -> getAuthorizedAclKeys(
                        principal,
                        objectType,
                        desiredPermissions ) ).collect( Collectors.toSet() );
    }

    public Stream<List<UUID>> getAuthorizedAclKeys(
            Principal principal,
            EnumSet<Permission> desiredPermissions ) {
        return desiredPermissions
                .stream()
                .map( desiredPermission -> session.executeAsync(
                        authorizedAclKeysQuery.bind()
                                .set( CommonColumns.PRINCIPAL_TYPE.cql(), principal.getType(), PrincipalType.class )
                                .setString( CommonColumns.PRINCIPAL_ID.cql(), principal.getId() )
                                .set( CommonColumns.PERMISSIONS.cql(), desiredPermission, Permission.class ) ) )
                .map( ResultSetFuture::getUninterruptibly )
                .flatMap( StreamUtil::stream )
                .map( AuthorizationUtils::aclKey );
    }

    public Set<List<UUID>> getAuthorizedAclKeys(
            Set<Principal> principals,
            EnumSet<Permission> desiredPermissions ) {
        return principals
                .stream()
                .flatMap( principal -> getAuthorizedAclKeys(
                        principal,
                        desiredPermissions ) )
                .collect( Collectors.toSet() );
    }

    public Stream<Principal> getPrincipalsForSecurableObject( List<UUID> aclKeys ) {
        return Stream.of( session.executeAsync(
                aclsForSecurableObjectQuery.bind().setList( CommonColumns.ACL_KEYS.cql(),
                        aclKeys, UUID.class ) ) )
                .map( ResultSetFuture::getUninterruptibly )
                .flatMap( StreamUtil::stream )
                .map( AuthorizationUtils::getPrincipalFromRow );
    }

    public Acl getAclsForSecurableObject( List<UUID> aclKeys ) {
        Stream<Ace> accessControlEntries = getPrincipalsForSecurableObject( aclKeys )
                .map( principal -> new AceKey( aclKeys, principal ) )
                .map( aceKey -> new AceFuture( aceKey.getPrincipal(), aces.getAsync( aceKey ) ) )
                .map( AceFuture::getUninterruptibly );
        return new Acl( aclKeys, accessControlEntries::iterator );

    }

    public void createEmptyAcl( List<UUID> aclKey, SecurableObjectType objectType ) {
        BoundStatement bs = setObjectType.bind()
                .setList( CommonColumns.ACL_KEYS.cql(), aclKey, UUID.class )
                .set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), objectType, SecurableObjectType.class );
        session.execute( bs );
        logger.info( "Created empty acl with key {} and type {}", aclKey, objectType );
    }
}
