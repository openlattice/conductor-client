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

import com.dataloom.auditing.AuditableEvent;
import com.dataloom.authorization.paging.AuthorizedObjectsPagingInfo;
import com.dataloom.authorization.paging.AuthorizedObjectsSearchResult;
import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.authorization.processors.PermissionRemover;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.neuron.AuditableSignal;
import com.dataloom.neuron.Neuron;
import com.dataloom.neuron.Signal;
import com.dataloom.neuron.SignalType;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.kryptnostic.datastore.util.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        aces.executeOnKey( new AceKey( key, principal ), new PermissionMerger( DelegatedPermissionEnumSet.wrap( permissions ) ) );
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
        aqs.deletePermissionsByAclKeys( aclKeys );
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
    public Stream<List<UUID>> getAuthorizedObjectsOfType(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> aces ) {
        return aqs.getAuthorizedAclKeys( principal, objectType, aces );
    }

    @Override
    public Stream<List<UUID>> getAuthorizedObjectsOfType(
            Set<Principal> principals,
            SecurableObjectType objectType,
            EnumSet<Permission> aces ) {
        return aqs.getAuthorizedAclKeys( principals, objectType, aces )
                .stream();
    }

    @Override
    public AuthorizedObjectsSearchResult getAuthorizedObjectsOfType(
            NavigableSet<Principal> principals,
            SecurableObjectType objectType,
            Permission permission,
            AuthorizedObjectsPagingInfo pagingInfo,
            int pageSize ) {
        return aqs.getAuthorizedAclKeys( principals, objectType, permission, pagingInfo, pageSize );
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
        return aqs.getAuthorizedAclKeys( principal, permissions ).stream();
    }

    @Override
    public Iterable<Principal> getSecurableObjectOwners( List<UUID> key ) {
        return aqs.getOwnersForSecurableObject( key );
    }

}
