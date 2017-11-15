package com.dataloom.organizations.roles;

import com.dataloom.authorization.*;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.roles.Role;
import com.dataloom.organizations.processors.NestedPrincipalMerger;
import com.dataloom.organizations.processors.NestedPrincipalRemover;
import com.dataloom.organizations.roles.processors.PrincipalDescriptionUpdater;
import com.dataloom.organizations.roles.processors.PrincipalTitleUpdater;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.kryptnostic.datastore.util.Util;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AclKeySet;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.projections.PrincipalProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class HazelcastPrincipalService implements SecurePrincipalsManager, AuthorizingComponent {

    private static final Logger logger = LoggerFactory
            .getLogger( HazelcastPrincipalService.class );

    private final AuthorizationManager                  authorizations;
    private final HazelcastAclKeyReservationService     reservations;
    private final IMap<AclKey, SecurablePrincipal>      principals;
    private final IMap<AclKey, AclKeySet>               principalTrees; // RoleName -> Member RoleNames
    private final IMap<String, Auth0UserBasic>          users;
    private final IMap<List<UUID>, SecurableObjectType> securableObjectTypes;

    public HazelcastPrincipalService(
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService reservations,
            AuthorizationManager authorizations ) {

        this.authorizations = authorizations;
        this.reservations = reservations;
        this.principals = hazelcastInstance.getMap( HazelcastMap.PRINCIPALS.name() );
        this.principalTrees = hazelcastInstance.getMap( HazelcastMap.PRINCIPAL_TREES.name() );
        this.users = hazelcastInstance.getMap( HazelcastMap.USERS.name() );
        this.securableObjectTypes = hazelcastInstance.getMap( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );
    }

    @Override public void createSecurablePrincipalIfNotExists(
            Principal owner, SecurablePrincipal principal ) {
        createSecurablePrincipalIfNotExists( principal );
        final AclKey aclKey = principal.getAclKey();

        try {
            authorizations.createEmptyAcl( aclKey, principal.getCategory() );
            authorizations.addPermission( aclKey, owner, EnumSet.allOf( Permission.class ) );
        } catch ( Exception e ) {
            logger.error( "Unable to create principal {}", principal, e );
            Util.deleteSafely( principals, aclKey );
            reservations.release( principal.getId() );
            authorizations.deletePermissions( aclKey );
            throw new IllegalStateException( "Unable to create principal: " + principal.toString() );
        }
    }

    public void createSecurablePrincipalIfNotExists( SecurablePrincipal principal ) {
        reservations.reserveIdAndValidateType( principal, principal::getName );
        principals.set( principal.getAclKey(), principal );
        securableObjectTypes.putIfAbsent( principal.getAclKey(), principal.getCategory() );
    }

    @Override
    public void updateTitle( AclKey aclKey, String title ) {
        principals.executeOnKey( aclKey, new PrincipalTitleUpdater( title ) );
    }

    @Override
    public void updateDescription( AclKey aclKey, String description ) {
        principals.executeOnKey( aclKey, new PrincipalDescriptionUpdater( description ) );
    }

    @Override
    public SecurablePrincipal getSecurablePrincipal( AclKey aclKey ) {
        return principals.get( aclKey );
    }

    @Override public AclKey lookup( Principal p ) {
        return principals.values( findPrincipal( p ) ).stream().map( SecurablePrincipal::getAclKey ).findFirst().get();
    }

    @Override public Collection<SecurablePrincipal> getSecurablePrincipals( PrincipalType principalType ) {
        return principals.values( Predicates.equal( "principalType", principalType ) );
    }
    //    @Override public SecurablePrincipal getSecurablePrincipal( Principal principal ) {
    //
    //        return principals.get( principal );
    //    }

    @Override
    public SetMultimap<SecurablePrincipal, SecurablePrincipal> getRolesForUsersInOrganization( UUID organizationId ) {
        new PagingPredicate<>();
        return null;
    }

    @Override
    public Collection<SecurablePrincipal> getAllRolesInOrganization( UUID organizationId ) {
        Predicate rolesInOrganization = Predicates.and( Predicates.equal( "principalType", PrincipalType.ROLE ),
                Predicates.equal( "aclKey[0]", organizationId ) );
        return principals.values( rolesInOrganization );
    }

    @Override
    public void deletePrincipal( AclKey aclKey ) {
        SecurablePrincipal securablePrincipal = Util.getSafely( principals, aclKey );
        deletePrincipal( securablePrincipal );
    }

    private void deletePrincipal( SecurablePrincipal securablePrincipal ) {
        checkNotNull( securablePrincipal, "Principal not found." );
        Principal principal = securablePrincipal.getPrincipal();
        AclKey aclKey = securablePrincipal.getAclKey();

        //Remove the role from all users before deleting.
        for ( SecurablePrincipal user : getAllUsersWithPrincipal( aclKey ) ) {
            removePrincipalFromPrincipal( aclKey, user.getAclKey() );
        }

        authorizations.deletePrincipalPermissions( principal );
        reservations.release( securablePrincipal.getId() );
        securableObjectTypes.delete( securablePrincipal.getAclKey() );
        principals.delete( securablePrincipal.getAclKey() );
    }

    @Override
    public void deleteAllRolesInOrganization( UUID organizationId ) {
        Collection<SecurablePrincipal> allRolesInOrg = getAllRolesInOrganization( organizationId );
        for ( SecurablePrincipal securablePrincipal : allRolesInOrg ) {
            deletePrincipal( securablePrincipal );
        }
    }

    @Override
    public void addPrincipalToPrincipal( AclKey source, AclKey target ) {
        ensurePrincipalsExist( source, target );
        principalTrees
                .executeOnKey( target, new NestedPrincipalMerger( ImmutableSet.of( source ) ) );
    }

    public void removePrincipalFromPrincipal( AclKey source, AclKey target ) {
        ensurePrincipalsExist( source, target );
        principalTrees
                .executeOnKey( target, new NestedPrincipalRemover( ImmutableSet.of( source ) ) );

    }

    private void ensurePrincipalsExist( AclKey... aclKeys ) {
        ensurePrincipalsExist( "All principals must exist!", aclKeys );
    }

    private void ensurePrincipalsExist( String msg, AclKey... aclKeys ) {
        checkState( Stream.of( aclKeys )
                .filter( aclKey -> !principals.containsKey( aclKey ) )
                .peek( aclKey -> logger.error( "Principal with acl key {} does not exist!", aclKey ) )
                .count() == 0, msg );
    }

    @Override
    public Collection<SecurablePrincipal> getAllUsersWithPrincipal( AclKey aclKey ) {
        Predicate hasPrincipal = Predicates.and( Predicates.equal( "value[any]", aclKey ),
                Predicates.equal( "principalType", PrincipalType.USER ) );
        //It sucks to load all, but being lazy and not using an read only entry processor.
        return principals.getAll( principalTrees.keySet( hasPrincipal ) )
                .values()
                .stream()
                .collect( Collectors.toList() );
    }

    @Override
    public Collection<Auth0UserBasic> getAllUserProfilesWithPrincipal( AclKey principal ) {
        return users.getAll( getAllUsersWithPrincipal( principal )
                .stream()
                .map( SecurablePrincipal::getName )
                .collect( Collectors.toSet() ) ).values();
    }

    @Override
    public Map<AclKey, Object> executeOnPrincipal(
            EntryProcessor<AclKey, SecurablePrincipal> ep,
            Predicate p ) {
        return principals.executeOnEntries( ep, p );
    }

    @Override
    public Collection<SecurablePrincipal> getSecurablePrincipals( Predicate p ) {
        return principals.values( p );
    }

    @Override
    public Collection<Principal> getPrincipals( Predicate<AclKey, SecurablePrincipal> p ) {
        return principals.project( new PrincipalProjection(), p );
    }

    @Override public Collection<SecurablePrincipal> getSecurablePrincipals( Set<Principal> members ) {
        Predicate p = Predicates
                .in( "principal", members.toArray( new Principal[ 0 ] ) );
        return principals.values( p );
    }

    @Override
    public boolean principalExists( Principal p ) {
        return reservations.isReserved( p.getId() );
    }

    @Override public Auth0UserBasic getUser( String userId ) {
        return Util.getSafely( users, userId );
    }

    @Override public Role getRole( UUID organizationId, UUID roleId ) {
        AclKey aclKey = new AclKey( organizationId, roleId );
        return (Role) Util.getSafely( principals, aclKey );
    }

    @Override public Collection<SecurablePrincipal> getAllPrincipals( SecurablePrincipal sp ) {
        final AclKeySet roles = Util.getSafely( principalTrees, sp.getAclKey() );
        if ( roles == null ) {
            return ImmutableList.of();
        }
        Set<AclKey> nextLayer = roles;

        while ( !nextLayer.isEmpty() ) {
            Map<AclKey, AclKeySet> nextRoles = principalTrees.getAll( nextLayer );
            nextLayer = nextRoles.values().stream().flatMap( AclKeySet::stream ).collect( Collectors.toSet() );
            roles.addAll( nextLayer );
        }

        return principals.getAll( roles ).values();
    }

    @Override

    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    private static Predicate findPrincipal( Principal p ) {
        return Predicates.equal( "principal", p );
    }

}
