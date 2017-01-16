package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Iterables;

import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.authorization.processors.PermissionRemover;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.datastore.util.Util;

public class HazelcastAuthorizationService implements AuthorizationManager {
    private static final Logger logger = LoggerFactory.getLogger( AuthorizationManager.class );

    private final IMap<AceKey, Set<Permission>> aces;
    private final AuthorizationQueryService     aqs;
    private final DurableExecutorService        executor;

    public HazelcastAuthorizationService( HazelcastInstance hazelcastInstance, AuthorizationQueryService aqs ) {
        aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );
        this.aqs = checkNotNull( aqs );
        this.executor = hazelcastInstance.getDurableExecutorService( "default" );
    }

    @Override
    public void addPermission(
            List<UUID> key,
            Principal principal,
            Set<Permission> permissions ) {
        aces.executeOnKey( new AceKey( key, principal ), new PermissionMerger( permissions ) );
    }

    @Override
    public void removePermission(
            List<UUID> key,
            Principal principal,
            Set<Permission> permissions ) {
        aces.executeOnKey( new AceKey( key, principal ), new PermissionRemover( permissions ) );
    }

    @Override
    public void setPermission(
            List<UUID> key,
            Principal principal,
            Set<Permission> permissions ) {
        aces.set( new AceKey( key, principal ), permissions );
    }

    @Override
    public void deletePermissions( List<UUID> aclKeys ) {
        Iterable<Principal> principals = aqs.getPrincipalsForSecurableObject( aclKeys );

        StreamSupport
                .stream( principals.spliterator(), false )
                .map( principal -> new AceKey( aclKeys, principal ) )
                .forEach( Util.safeDeleter( aces ) );
    }

    @Override
    public boolean checkIfHasPermissions(
            List<UUID> key,
            Set<Principal> principals,
            Set<Permission> requiredPermissions ) {
        Set<Permission> permissions = getSecurableObjectPermissions( key, principals );
        return permissions.containsAll( requiredPermissions );
    }

    @Override
    public boolean checkIfUserIsOwner( List<UUID> aclKey, Principal principal ) {
        checkArgument( principal.getType().equals( PrincipalType.USER ), "A role cannot be the owner of an object" );
        return checkIfHasPermissions( aclKey, ImmutableSet.of( principal ), EnumSet.of( Permission.OWNER ) );
    }

    @Override
    public Set<Permission> getSecurableObjectPermissions(
            List<UUID> key,
            Set<Principal> principals ) {
        return aces
                .getAll( principals.stream().map( principal -> new AceKey( key, principal ) )
                        .collect( Collectors.toSet() ) )
                .values().stream().flatMap( permissions -> permissions.stream() )
                .collect( Collectors.toCollection( () -> EnumSet.noneOf( Permission.class ) ) );
    }

    @Override
    public Iterable<UUID> getAuthorizedObjectsOfType(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> aces ) {
        return Iterables.transform( aqs.getAuthorizedAclKeys( principal, objectType, aces ),
                AuthorizationUtils::getLastAclKeySafely );
    }
    
    @Override
    public Iterable<UUID> getAuthorizedObjectsOfType(
            Set<Principal> principal,
            SecurableObjectType objectType,
            EnumSet<Permission> aces ) {
        return Iterables.transform( aqs.getAuthorizedAclKeys( principal, objectType, aces ),
                AuthorizationUtils::getLastAclKeySafely );
    }
    
    @Override
    public Acl getAllSecurableObjectPermissions( List<UUID> key ) {
        return aqs.getAclsForSecurableObject( key );
    }

    public Map<String, AclKeyInfo> getAuthorizedObjects( Map<Principal, EnumSet<Permission>> aces ) {
        // Map acl keys into nested format based on length
        //
        aces.entrySet().stream().map( e -> aqs.getAuthorizedAclKeys( e.getKey(), e.getValue() ) ).collect( HashSet::new,
                ( s, v ) -> v.forEach( aclKey -> s.add( aclKey ) ),
                ( lhs, rhs ) -> lhs.addAll( rhs ) );
        return null;
    }

    @Override
    public void addPermissionsRequestForPropertyTypeInEntitySet(
            String username,
            Principal principal,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            EnumSet<Permission> permissions ) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removePermissionsRequestForEntitySet( UUID id ) {
        // TODO Auto-generated method stub

    }

}
