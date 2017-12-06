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

import com.codahale.metrics.annotation.Timed;
import com.dataloom.authorization.events.AclUpdateEvent;
import com.dataloom.authorization.paging.AuthorizedObjectsSearchResult;
import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.authorization.processors.PermissionRemover;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationAggregator;
import com.openlattice.authorization.mapstores.PermissionMapstore;
import com.openlattice.authorization.processors.SecurableObjectTypeUpdater;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastAuthorizationService implements AuthorizationManager {
    private static final Logger logger = LoggerFactory.getLogger( AuthorizationManager.class );

    private final IMap<AclKey, SecurableObjectType> securableObjectTypes;
    private final IMap<AceKey, AceValue>            aces;
    private final AuthorizationQueryService         aqs;
    private final EventBus                          eventBus;

    public HazelcastAuthorizationService(
            HazelcastInstance hazelcastInstance,
            AuthorizationQueryService aqs,
            EventBus eventBus ) {
        this.aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );
        this.securableObjectTypes = hazelcastInstance.getMap( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );
        this.aqs = checkNotNull( aqs );
        this.eventBus = checkNotNull( eventBus );
    }

    private void updateAcl( AclKey aclKey, Principal principal ) {
        if ( aclKey.size() == 1 ) {
            eventBus.post( new AclUpdateEvent( aclKey, ImmutableSet.of( principal ) ) );
        }
    }

    @Override
    public void setSecurableObjectType( AclKey aclKey, SecurableObjectType objectType ) {
        securableObjectTypes.set( aclKey, objectType );
        aces.executeOnEntries( new SecurableObjectTypeUpdater( objectType ), hasAclKey( aclKey ) );
    }

    @Override
    public void addPermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions ) {
        //TODO: We should do something better than reading the securable object type.
        SecurableObjectType securableObjectType = securableObjectTypes.getOrDefault( key, SecurableObjectType.Unknown );
        if ( securableObjectType == SecurableObjectType.Unknown ) {
            logger.warn( "Unrecognized object type for acl key {} key ", key );
        }
        aces.executeOnKey( new AceKey( key, principal ), new PermissionMerger( permissions, securableObjectType ) );
        updateAcl( key, principal );
    }

    @Override
    public void removePermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions ) {
        aces.executeOnKey( new AceKey( key, principal ), new PermissionRemover( permissions ) );
        updateAcl( key, principal );
    }

    @Override
    public void setPermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions ) {
        //This should be a rare call to overwrite all permissions, so it's okay to do a read before write.
        SecurableObjectType securableObjectType = securableObjectTypes.getOrDefault( key, SecurableObjectType.Unknown );
        aces.set( new AceKey( key, principal ), new AceValue( permissions, securableObjectType ) );
        updateAcl( key, principal );
    }

    @Override
    public void deletePermissions( AclKey aclKey ) {
        securableObjectTypes.delete( aclKey );
        aces.removeAll( hasAclKey( aclKey ) );
        aqs.deletePermissionsByAclKeys( aclKey );
    }

    @Timed
    @Override
    public Map<AclKey, EnumMap<Permission, Boolean>> maybeFastAccessChecksForPrincipals(
            Set<AccessCheck> accessChecks,
            Set<Principal> principals ) {
        final Map<AclKey, EnumMap<Permission, Boolean>> results = new HashMap<>( accessChecks.size() );
        final Set<AceKey> aceKeys = new HashSet<>( accessChecks.size() * principals.size() );
        final Map<AceKey, AceValue> aceMap;

        //Prepare the results data structure
        accessChecks
                .parallelStream()
                .forEach( accessCheck -> {
                    AclKey aclKey = accessCheck.getAclKey();
                    EnumMap<Permission, Boolean> granted = results.get( aclKey );

                    if ( granted == null ) {
                        granted = new EnumMap<>( Permission.class );
                        results.put( aclKey, granted );
                    }

                    for ( Permission permission : accessCheck.getPermissions() ) {
                        granted.putIfAbsent( permission, false );
                    }

                    principals.forEach( p -> aceKeys.add( new AceKey( aclKey, p ) ) );
                } );

        aceMap = aces.getAll( aceKeys );

        aceMap.forEach( ( ak, av ) -> {
            EnumMap<Permission, Boolean> granted = results.get( ak.getKey() );
            av.getPermissions().forEach( p -> {
                if ( granted.containsKey( p ) ) {
                    granted.put( p, true );
                }
            } );
        } );

        return results;
    }

    @Timed
    @Override
    public Map<AclKey, EnumMap<Permission, Boolean>> accessChecksForPrincipals(
            Set<AccessCheck> accessChecks,
            Set<Principal> principals ) {
        return accessChecks
                .parallelStream()
                .collect( Collectors.toConcurrentMap(
                        ac -> ac.getAclKey(),
                        ac ->
                                aces.aggregate( new AuthorizationAggregator( ac.getPermissions() ),
                                        matches( ac.getAclKey(), principals, ac.getPermissions() ) ).getPermissions()
                ) );

    }

    @Override
    public void deletePrincipalPermissions( Principal principal ) {
        aqs.deletePermissionsByPrincipal( principal );
    }

    public Predicate matches( AclKey aclKey, Collection<Principal> principals, EnumSet<Permission> permissions ) {
        return Predicates.and(
                hasAclKey( aclKey ),
                hasPrincipals( principals ),
                hasAnyPermissions( permissions ) );
    }

    @Override
    public boolean checkIfHasPermissions(
            AclKey key,
            Set<Principal> principals,
            EnumSet<Permission> requiredPermissions ) {
        Set<Permission> permissions = getSecurableObjectPermissions( key, principals );
        return permissions.containsAll( requiredPermissions );
    }

    @Override
    public boolean checkIfUserIsOwner( AclKey aclKey, Principal principal ) {
        checkArgument( principal.getType().equals( PrincipalType.USER ), "A role cannot be the owner of an object" );
        return checkIfHasPermissions( aclKey, ImmutableSet.of( principal ), EnumSet.of( Permission.OWNER ) );
    }

    @Override
    @Timed
    public Set<Permission> getSecurableObjectPermissions(
            AclKey key,
            Set<Principal> principals ) {
        final EnumSet<Permission> objectPermissions = EnumSet.noneOf( Permission.class );
        final Set<AceKey> aceKeys = principals
                .stream()
                .map( principal -> new AceKey( key, principal ) )
                .collect( Collectors.toSet() );
        aces
                .getAll( aceKeys )
                .values()
                .stream()
                //                .peek( ps -> logger.info( "Implementing class: {}", ps.getClass().getCanonicalName() ) )
                .map( AceValue::getPermissions )
                .filter( permissions -> permissions != null )
                .forEach( objectPermissions::addAll );
        return objectPermissions;
    }

    @Timed
    @Override
    public Stream<AclKey> getAuthorizedObjectsOfType(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions ) {
        Predicate p = Predicates
                .and( hasPrincipal( principal ), hasType( objectType ), hasExactPermissions( permissions ) );
        return this.aces.keySet( p )
                .stream()
                .map( AceKey::getKey );
    }

    @Timed
    @Override
    public Stream<AclKey> getAuthorizedObjectsOfType(
            Set<Principal> principals,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions ) {
        Predicate p = Predicates
                .and( hasPrincipals( principals ), hasType( objectType ), hasExactPermissions( permissions ) );
        return this.aces.keySet( p )
                .stream()
                .map( AceKey::getKey );
    }

    @Timed
    @Override
    public AuthorizedObjectsSearchResult getAuthorizedObjectsOfType(
            NavigableSet<Principal> principals,
            SecurableObjectType objectType,
            Permission permission,
            String offset,
            int pageSize ) {

        return aqs.getAuthorizedAclKeys( principals, objectType, permission, offset, pageSize );
    }

    @Timed
    @Override
    public Acl getAllSecurableObjectPermissions( AclKey key ) {

        return aqs.getAclsForSecurableObject( key );
    }

    @Timed
    @Override
    public Stream<AclKey> getAuthorizedObjects( Principal principal, EnumSet<Permission> permissions ) {
        return aqs.getAuthorizedAclKeys( principal, permissions );
    }

    @Timed
    @Override
    public Stream<AclKey> getAuthorizedObjects( Set<Principal> principal, EnumSet<Permission> permissions ) {
        return aqs.getAuthorizedAclKeys( principal, permissions );
    }

    @Timed
    @Override
    public Iterable<Principal> getSecurableObjectOwners( AclKey key ) {
        return aqs.getOwnersForSecurableObject( key );
    }

    @Timed
    @Override
    public Map<AceKey, AceValue> getPermissionMap( Set<AclKey> aclKeys, Set<Principal> principals ) {
        Set<AceKey> aceKeys = aclKeys
                .stream()
                .flatMap( aclKey -> principals.stream().map( p -> new AceKey( aclKey, p ) ) )
                .collect( Collectors.toSet() );

        return aces.getAll( aceKeys );
    }

    private static Predicate hasExactPermissions( EnumSet<Permission> permissions ) {
        Predicate[] subPredicates = new Predicate[ permissions.size() ];
        int i = 0;
        for ( Permission p : permissions ) {
            subPredicates[ i++ ] = Predicates.equal( PermissionMapstore.PERMISSIONS_INDEX, p );
        }
        return Predicates.and( subPredicates );
    }

    private static Predicate hasAnyPermissions( EnumSet<Permission> permissions ) {
        return Predicates.in( PermissionMapstore.PERMISSIONS_INDEX, permissions.toArray( new Permission[ 0 ] ) );
    }

    private static Predicate hasAclKey( AclKey aclKey ) {
        //TODO: Do something about ignored order of acl key pieces.
        Predicate[] subPredicates = new Predicate[ aclKey.size() + 1 ];
        int i = 0;

        for ( UUID p : aclKey ) {
            subPredicates[ i++ ] = Predicates.equal( PermissionMapstore.ACL_KEY_INDEX, p );
        }

        subPredicates[ i ] = Predicates.equal( PermissionMapstore.ACL_KEY_LENGTH_INDEX, aclKey.getSize() );

        return Predicates.and( subPredicates );
    }

    private static Predicate hasType( SecurableObjectType objectType ) {
        return Predicates.equal( PermissionMapstore.SECURABLE_OBJECT_TYPE_INDEX, objectType );
    }

    private static Predicate hasPrincipals( Collection<Principal> principals ) {
        return Predicates.in( PermissionMapstore.PRINCIPAL_INDEX, principals.toArray( new Principal[ 0 ] ) );
    }

    private static Predicate hasPrincipal( Principal principal ) {
        return Predicates.equal( PermissionMapstore.PRINCIPAL_INDEX, principal );
    }

}
