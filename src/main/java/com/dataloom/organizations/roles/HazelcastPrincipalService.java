package com.dataloom.organizations.roles;

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
import com.google.common.collect.Iterables;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.kryptnostic.datastore.util.Util;
import com.openlattice.authorization.SecurablePrincipal;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastPrincipalService implements RolesManager, AuthorizingComponent {

    private static final Logger                 logger              = LoggerFactory
            .getLogger( HazelcastPrincipalService.class );
    private static       EnumSet<PrincipalType> NESTABLE_PRINCIPALS = EnumSet
            .of( PrincipalType.ROLE, PrincipalType.USER );
    private final AuthorizationManager                authorizations;
    private final HazelcastAclKeyReservationService   reservations;
    private final UserDirectoryService                uds;
    private final IMap<Principal, SecurablePrincipal> principals;
    private final IMap<Principal, Set<Principal>>     nestedPrincipals; // RoleName -> Member RoleNames
    private final IMap<String, Auth0UserBasic>        users;

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
        final List<UUID> aclKey = principal.getAclKey();

        try {
            authorizations.createEmptyAcl( aclKey, principal.getCategory() );
            authorizations.addPermission( aclKey, owner, EnumSet.allOf( Permission.class ) );
        } catch ( Exception e ) {
            logger.error( "Unable to create principal {}", principal, e );
            Util.deleteSafely( principals, principal.getPrincipal() );
            reservations.release( principal.getId() );
            authorizations.deletePermissions( aclKey );
            throw new IllegalStateException( "Unable to create principal: " + principal.toString() );
        }
    }

    public void createSecurablePrincipalIfNotExists( SecurablePrincipal principal ) {
        reservations.reserveIdAndValidateType( principal, principal::getName );
        principals.set( principal.getPrincipal(), principal );
    }

    @Override
    public void updateTitle( Principal principal, String title ) {
        principals.executeOnKey( principal, new PrincipalTitleUpdater( title ) );
    }

    @Override
    public void updateDescription( Principal principal, String description ) {
        principals.executeOnKey( principal, new PrincipalDescriptionUpdater( description ) );
    }

    @Override public SecurablePrincipal getPrincipal( Principal principal ) {
        return principals.get( principal );
    }

    @Override public Collection<SecurablePrincipal> getSecurablePrincipals( PrincipalType principalType ) {
        return principals.values( Predicates.equal( "principalType", principalType ) );
    }

    @Override
    public Collection<SecurablePrincipal> getAllRolesInOrganization( UUID organizationId ) {
        Predicate rolesInOrganization = Predicates.and( Predicates.equal( "principalType", PrincipalType.ROLE ),
                Predicates.equal( "aclKey[0]", organizationId ) );
        return principals.values( rolesInOrganization );
    }

    @Override
    public void deletePrincipal( Principal principal ) {
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
    public void deleteAllRolesInOrganization( UUID organizationId, Iterable<Principal> users ) {
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
    public void addPrincipalToPrincipal( Principal source, Principal target ) {
        //TODO: Make sure principal exists.

        nestedPrincipals
                .executeOnKey( target, new NestedPrincipalMerger( ImmutableSet.of( source ) ) );
    }

    public void removePrincipalFromPrincipal( Principal source, Principal target ) {
        //TODO: Ensure principal exists

        nestedPrincipals
                .executeOnKey( target, new NestedPrincipalRemover( ImmutableSet.of( source ) ) );

    }

    @Override
    public Collection<Principal> getAllUsersWithPrincipal( Principal principal ) {
        Predicate hasPrincipal = Predicates.and( Predicates.equal( "value[any]", principal ),
                Predicates.equal( "principalType", PrincipalType.USER ) );
        Collection<Principal> users = nestedPrincipals.keySet( hasPrincipal );
        return users;
    }

    @Override
    public Collection<Auth0UserBasic> getAllUserProfilesWithPrincipal( Principal principal ) {
        return users.getAll( getAllUsersWithPrincipal( principal )
                .stream()
                .map( Principal::getId )
                .collect( Collectors.toSet() ) ).values();
    }

    @Override
    public Map<Principal, Object> executeOnPrincipal(
            EntryProcessor<Principal, SecurablePrincipal> ep,
            Predicate p ) {
        return principals.executeOnEntries( ep, p );
    }

    @Override public Collection<SecurablePrincipal> getSecurablePrincipals( Predicate p ) {
        return principals.values( p );
    }

    @Override
    public Collection<Principal> getPrincipals( Predicate p ) {
        return principals.keySet( p );
    }

    @Override public Role getRole( UUID organizationId, UUID roleId ) {
        Predicate findRole = Predicates.and(
                Predicates.equal( "principalType", PrincipalType.ROLE ),
                Predicates.equal( "aclKey[0]", organizationId ),
                Predicates.equal( "aclKey[1]", roleId )
        );

        //There should only be one element returned from the query above.
        return Iterables.getOnlyElement(
                principals
                        .values( findRole )
                        .stream()
                        .map( principal -> (Role) principal )::iterator );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

}
