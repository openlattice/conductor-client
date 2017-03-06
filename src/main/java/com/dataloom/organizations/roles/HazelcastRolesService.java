package com.dataloom.organizations.roles;

import java.util.Set;
import java.util.UUID;

import org.spark_project.guava.collect.Iterables;

import com.clearspring.analytics.util.Preconditions;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organization.roles.RoleKey;
import com.dataloom.organizations.PrincipalSet;
import com.dataloom.organizations.processors.PrincipalMerger;
import com.dataloom.organizations.processors.PrincipalRemover;
import com.dataloom.organizations.roles.processors.RoleDescriptionUpdater;
import com.dataloom.organizations.roles.processors.RoleTitleUpdater;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastRolesService implements RolesManager, AuthorizingComponent {
    private final AuthorizationManager              authorizations;
    private final HazelcastAclKeyReservationService reservations;
    private final UserDirectoryService              uds;

    private final IMap<RoleKey, OrganizationRole>      roles;
    private final IMap<RoleKey, PrincipalSet>          usersWithRole;

    private final IMap<UUID, String>                orgsTitles;

    public HazelcastRolesService(
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService reservations,
            UserDirectoryService uds,
            AuthorizationManager authorizations ) {
        this.authorizations = authorizations;
        this.reservations = reservations;
        this.uds = uds;

        this.roles = hazelcastInstance.getMap( HazelcastMap.ROLES.name() );
        this.usersWithRole = hazelcastInstance.getMap( HazelcastMap.USERS_WITH_ROLE.name() );

        this.orgsTitles = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_TITLES.name() );
    }

    @Override
    public void createRoleIfNotExists( OrganizationRole role ) {
        ensureValidOrganizationRole( role );

        reservations.reserveIdAndValidateType( role );

        Preconditions.checkState(
                roles.putIfAbsent( new RoleKey( role.getOrganizationId(), role.getId() ), role ) == null,
                "Organization Role already exists." );
    }

    @Override
    public void updateTitle( RoleKey roleKey, String title ) {
        roles.executeOnKey( roleKey, new RoleTitleUpdater( title ) );
    }

    @Override
    public void updateDescription( RoleKey roleKey, String description ) {
        roles.executeOnKey( roleKey, new RoleDescriptionUpdater( description ) );
    }

    @Override
    public OrganizationRole getRole( RoleKey roleKey ) {
        return roles.get( roleKey );
    }

    @Override
    public Iterable<OrganizationRole> getAllRoles( UUID organizationId ) {
        return ;
    }

    @Override
    public void deleteRole( RoleKey roleKey ) {
        UUID id = roleKey.getRoleId();
        for( Principal user : getAllUsersOfRole( roleKey ) ){
            removeRoleFromUser( roleKey, user );
        }
        roles.delete( id );
        reservations.release( id );
    }

    private void ensureValidOrganizationRole( OrganizationRole role ) {
        // check organization exists
        Preconditions.checkArgument( orgsTitles.containsKey( role.getOrganizationId() ),
                "Organization associated to this role does not exist." );
    }

    @Override
    public void addRoleToUser( RoleKey roleKey, Principal user ) {
        Preconditions.checkArgument( user.getType() == PrincipalType.USER, "Cannot add roles to another ROLE object.");
        
        usersWithRole.executeOnKey( roleKey, new PrincipalMerger( ImmutableSet.of( user ) ) );
        
        uds.addRoleToUser( user.getId(), roleKey.toString() );
    }

    @Override
    public void removeRoleFromUser( RoleKey roleKey, Principal user ) {
        Preconditions.checkArgument( user.getType() == PrincipalType.USER, "Cannot remove roles to another ROLE object.");
        
        usersWithRole.executeOnKey( roleKey, new PrincipalRemover( ImmutableSet.of( user ) ) );
        
        uds.removeRoleFromUser( user.getId(), roleKey.toString() );
    }

    @Override
    public Iterable<Principal> getAllUsersOfRole( RoleKey roleKey ) {
        return usersWithRole.get( roleKey ).unwrap();
    }

    @Override
    public Iterable<Auth0UserBasic> getAllUserProfilesOfRole( RoleKey roleKey ) {
        return Iterables.transform( getAllUsersOfRole( roleKey ), principal -> uds.getUser( principal.getId() ) );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

}
