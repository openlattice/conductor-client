package com.dataloom.organizations.roles;

import static com.google.common.base.Preconditions.checkState;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.roles.Role;
import com.dataloom.organizations.processors.NestedPrincipalMerger;
import com.dataloom.organizations.processors.NestedPrincipalRemover;
import com.dataloom.organizations.roles.processors.PrincipalDescriptionUpdater;
import com.dataloom.organizations.roles.processors.PrincipalTitleUpdater;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.kryptnostic.datastore.util.Util;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.projections.PrincipalProjection;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastPrincipalService implements SecurePrincipalsManager, AuthorizingComponent {

    private static final Logger                 logger              = LoggerFactory
            .getLogger( HazelcastPrincipalService.class );
    private static       EnumSet<PrincipalType> NESTABLE_PRINCIPALS = EnumSet
            .of( PrincipalType.ROLE, PrincipalType.USER );
    private final AuthorizationManager              authorizations;
    private final HazelcastAclKeyReservationService reservations;
    private final UserDirectoryService              uds;
    private final IMap<AclKey, SecurablePrincipal>  principals;
    private final IMap<AclKey, Set<AclKey>>         nestedPrincipals; // RoleName -> Member RoleNames
    private final IMap<String, Auth0UserBasic>      users;

    public HazelcastPrincipalService(
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService reservations,
            UserDirectoryService uds,
            AuthorizationManager authorizations ) {

        this.authorizations = authorizations;
        this.reservations = reservations;
        this.uds = uds;
        this.principals = hazelcastInstance.getMap( HazelcastMap.PRINCIPALS.name() );
        this.nestedPrincipals = hazelcastInstance.getMap( HazelcastMap.NESTED_PRINCIPALS.name() );
        this.users = hazelcastInstance.getMap( HazelcastMap.USERS.name() );
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
    public Collection<SecurablePrincipal> getAllRolesInOrganization( UUID organizationId ) {
        Predicate rolesInOrganization = Predicates.and( Predicates.equal( "principalType", PrincipalType.ROLE ),
                Predicates.equal( "aclKey[0]", organizationId ) );
        return principals.values( rolesInOrganization );
    }

    @Override
    public void deletePrincipal( AclKey aclKey ) {
        //TODO: Implement delete

        //        Role role = checkNotNull( Util.getSafely( roles, roleName ), "Role not found." );
        //
        //        //Remove the role from all users before deleting.
        //        for ( Principal user : getAllUsersWithPrincipal( roleName ) ) {
        //            removePrincipalFromPrincipal( roleName, user );
        //        }
        //
        //        reservations.release( role.getId() );
        //        roles.delete( roleName );
        //        authorizations.deletePermissions( role.getAclKey() );
        //        securableObjectTypes.deleteSecurableObjectType( role.getAclKey() );
    }

    @Override
    public void deleteAllRolesInOrganization( UUID organizationId ) {
        Collection<SecurablePrincipal> allRolesInOrg = getAllRolesInOrganization( organizationId );
        //TODO: Implement deletion

        //        for ( Principal user : users ) {
        //            uds.removeAllRolesInOrganizationFromUser( user.getId(), allRolesInOrg );
        //        }
        //
        //        for ( Role role : allRolesInOrg ) {
        //            authorizations.deletePermissions( role.getAclKey() );
        //            reservations.release( role.getId() );
        //            securableObjectTypes.deleteSecurableObjectType( role.getAclKey() );
        //        }
        //
        //        rqs.deleteAllRolesInOrganization( organizationId, allRolesInOrg );
    }

    @Override
    public void addPrincipalToPrincipal( AclKey source, AclKey target ) {
        ensurePrincipalsExist( source, target );
        nestedPrincipals
                .executeOnKey( target, new NestedPrincipalMerger( ImmutableSet.of( source ) ) );
    }

    public void removePrincipalFromPrincipal( AclKey source, AclKey target ) {
        ensurePrincipalsExist( source, target );
        nestedPrincipals
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
    public Collection<Principal> getAllUsersWithPrincipal( AclKey aclKey ) {
        Predicate hasPrincipal = Predicates.and( Predicates.equal( "value[any]", aclKey ),
                Predicates.equal( "principalType", PrincipalType.USER ) );
        //It sucks to load all, but being lazy and not using an read only entry processor.
        return principals.getAll( nestedPrincipals.keySet( hasPrincipal ) )
                .values()
                .stream()
                .map( SecurablePrincipal::getPrincipal )
                .collect( Collectors.toList() );
    }

    @Override
    public Collection<Auth0UserBasic> getAllUserProfilesWithPrincipal( AclKey principal ) {
        return users.getAll( getAllUsersWithPrincipal( principal )
                .stream()
                .map( Principal::getId )
                .collect( Collectors.toSet() ) ).values();
    }

    @Override
    public Map<AclKey, Object> executeOnPrincipal(
            EntryProcessor<AclKey, SecurablePrincipal> ep,
            Predicate p ) {
        return principals.executeOnEntries( ep, p );
    }

    @Override public Collection<SecurablePrincipal> getSecurablePrincipals( Predicate p ) {
        return principals.values( p );
    }

    @Override
    public Collection<Principal> getPrincipals( Predicate<AclKey, SecurablePrincipal> p ) {
        return principals.project( new PrincipalProjection(), p );
    }

    @Override public Role getRole( UUID organizationId, UUID roleId ) {
        AclKey aclKey = new AclKey( organizationId, roleId );

        return (Role) Util.getSafely( principals, aclKey );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    private static Predicate findPrincipal( Principal p ) {
        return Predicates.equal( "principal", p );
    }

}
