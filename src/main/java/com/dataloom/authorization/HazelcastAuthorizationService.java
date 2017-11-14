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

package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dataloom.authorization.events.AclUpdateEvent;
import com.dataloom.authorization.paging.AuthorizedObjectsPagingInfo;
import com.dataloom.authorization.paging.AuthorizedObjectsSearchResult;
import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.authorization.processors.PermissionRemover;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class HazelcastAuthorizationService implements AuthorizationManager {
    private static final Logger logger = LoggerFactory.getLogger( AuthorizationManager.class );

    private final IMap<AceKey, DelegatedPermissionEnumSet> aces;
    private final IMap<List<UUID>, SecurableObjectType>    securableObjectTypes;
    private final AuthorizationQueryService                aqs;
    private final EventBus                                 eventBus;

    public HazelcastAuthorizationService(
            HazelcastInstance hazelcastInstance,
            AuthorizationQueryService aqs,
            EventBus eventBus ) {
        aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );
        securableObjectTypes = hazelcastInstance.getMap( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );
        this.aqs = checkNotNull( aqs );
        this.eventBus = checkNotNull( eventBus );
    }

    private void updateAcl( List<UUID> aclKey, Principal principal ) {
        if ( aclKey.size() == 1 ) {
            eventBus.post( new AclUpdateEvent( aclKey, ImmutableSet.of( principal ) ) );
        }
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
        aces.executeOnKey( new AceKey( key, principal ),
                new PermissionMerger( DelegatedPermissionEnumSet.wrap( permissions ) ) );
        updateAcl( key, principal );
    }

    @Override
    public void removePermission(
            List<UUID> key,
            Principal principal,
            Set<Permission> permissions ) {
        aces.executeOnKey( new AceKey( key, principal ), new PermissionRemover( permissions ) );
        updateAcl( key, principal );
    }

    @Override
    public void setPermission(
            List<UUID> key,
            Principal principal,
            Set<Permission> permissions ) {
        aces.set( new AceKey( key, principal ), DelegatedPermissionEnumSet.wrap( permissions ) );
        updateAcl( key, principal );
    }

    @Override
    public void deletePermissions( List<UUID> aclKeys ) {
        aqs.deletePermissionsByAclKeys( aclKeys );
    }

    @Override
    public void deletePrincipalPermissions( Principal principal ) {
        aqs.deletePermissionsByPrincipal( principal );
    }

    @Override
    public boolean checkIfHasPermissions(
            List<UUID> key,
            Set<Principal> principals,
            EnumSet<Permission> requiredPermissions ) {
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
    public Stream<List<UUID>> getAuthorizedObjectsOfType(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> aces ) {
        return aqs.getAuthorizedAclKeysForPrincipals( ImmutableSet.of( principal ),
                aces,
                Optional.of( objectType ),
                Optional.empty(),
                Optional.empty() );
    }

    @Override
    public Stream<List<UUID>> getAuthorizedObjectsOfType(
            Set<Principal> principals,
            SecurableObjectType objectType,
            EnumSet<Permission> aces ) {
        return aqs.getAuthorizedAclKeysForPrincipals( principals,
                aces,
                Optional.of( objectType ),
                Optional.empty(),
                Optional.empty() );
    }

    @Override
    public AuthorizedObjectsSearchResult getAuthorizedObjectsOfType(
            NavigableSet<Principal> principals,
            SecurableObjectType objectType,
            Permission permission,
            String offset,
            int pageSize ) {
        return aqs.getAuthorizedAclKeys( principals, objectType, permission, offset, pageSize );
    }

    @Override
    public Acl getAllSecurableObjectPermissions( List<UUID> key ) {
        return aqs.getAclsForSecurableObject( key );
    }

    @Override
    public Stream<List<UUID>> getAuthorizedObjects( Principal principal, EnumSet<Permission> permissions ) {
        return aqs.getAuthorizedAclKeys( principal, permissions );
    }

    @Override
    public Stream<List<UUID>> getAuthorizedObjects( Set<Principal> principal, EnumSet<Permission> permissions ) {
        return aqs.getAuthorizedAclKeys( principal, permissions );
    }

    @Override
    public Iterable<Principal> getSecurableObjectOwners( List<UUID> key ) {
        return aqs.getOwnersForSecurableObject( key );
    }

}
