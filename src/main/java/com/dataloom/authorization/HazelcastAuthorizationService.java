package com.dataloom.authorization;

import com.dataloom.auditing.AuditableEvent;
import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.authorization.processors.PermissionRemover;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Iterables;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HazelcastAuthorizationService implements AuthorizationManager {
    private static final Logger logger = LoggerFactory.getLogger( AuthorizationManager.class );

    private final IMap<AceKey, DelegatedPermissionEnumSet> aces;
    private final AuthorizationQueryService                aqs;
    private final EventBus                                 eventBus;

    public HazelcastAuthorizationService(
            HazelcastInstance hazelcastInstance,
            AuthorizationQueryService aqs,
            EventBus eventBus ) {
        aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );
        this.aqs = checkNotNull( aqs );
        this.eventBus = checkNotNull( eventBus );
    }

    @Override
    public void createEmptyAcl( List<UUID> aclKey, SecurableObjectType objectType ) {
        aqs.createEmptyAcl( aclKey, objectType );
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
        aces.set( new AceKey( key, principal ), DelegatedPermissionEnumSet.wrap( permissions ) );
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
            EnumSet<Permission> requiredPermissions ) {
        principals.forEach( p -> eventBus.post( new AuditableEvent( key, p, SecurableObjectType.Datasource,
                requiredPermissions, "This is a test" ) ) );
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
                .getAll( principals
                        .stream()
                        .map( principal -> new AceKey( key, principal ) )
                        .collect( Collectors.toSet() ) )
                .values()
                .stream()
                //                .peek( ps -> logger.info( "Implementing class: {}", ps.getClass().getCanonicalName() ) )
                .flatMap( permissions -> permissions.stream() )
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

    @Override
    public Iterable<List<UUID>> getAuthorizedObjects( Principal principal, EnumSet<Permission> permissions ) {
        return aqs.getAuthorizedAclKeys( principal, permissions );
    }

    @Override
    public Iterable<List<UUID>> getAuthorizedObjects( Set<Principal> principal, EnumSet<Permission> permissions ) {
        return aqs.getAuthorizedAclKeys( principal, permissions );
    }

}
