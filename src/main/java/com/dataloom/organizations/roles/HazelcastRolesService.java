package com.dataloom.organizations.roles;

import java.util.Set;
import java.util.UUID;

import com.clearspring.analytics.util.Preconditions;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.roles.OrganizationRole;
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

    private final IMap<UUID, OrganizationRole>      roles;
    private final IMap<UUID, PrincipalSet>          usersWithRole;

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
                roles.putIfAbsent( role.getId(), role ) == null,
                "Organization Role already exists." );
    }

    @Override
    public void updateTitle( String roleIdString, String title ) {
        roles.executeOnKey( OrganizationRole.getRoleIdFromString( roleIdString ), new RoleTitleUpdater( title ) );
    }

    @Override
    public void updateDescription( String roleIdString, String description ) {
        roles.executeOnKey( OrganizationRole.getRoleIdFromString( roleIdString ), new RoleDescriptionUpdater( description ) );
    }

    @Override
    public OrganizationRole getRole( String roleIdString ) {
        return roles.get( OrganizationRole.getRoleIdFromString( roleIdString ) );
    }

    @Override
    public Iterable<OrganizationRole> getAllRoles() {
        return roles.values();
    }

    @Override
    public void deleteRole( String roleIdString ) {
        UUID id = OrganizationRole.getRoleIdFromString( roleIdString );
        for( Principal user : getAllUsersOfRole( roleIdString ) ){
            removeRoleFromUser( user.getId(), roleIdString );
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
    public void addRoleToUser( String userId, String roleIdString ) {
        Principal principal = new Principal( PrincipalType.USER, userId );
        
        UUID id = OrganizationRole.getRoleIdFromString( roleIdString );
        usersWithRole.executeOnKey( id, new PrincipalMerger( ImmutableSet.of( principal ) ) );
        
        uds.addRoleToUser( userId, roleIdString );
    }

    @Override
    public void removeRoleFromUser( String userId, String roleIdString ) {
        Principal principal = new Principal( PrincipalType.USER, userId );
        
        UUID id = OrganizationRole.getRoleIdFromString( roleIdString );
        usersWithRole.executeOnKey( id, new PrincipalRemover( ImmutableSet.of( principal ) ) );
        
        uds.removeRoleFromUser( userId, roleIdString );
    }

    @Override
    public Set<Principal> getAllUsersOfRole( String roleIdString ) {
        return usersWithRole.get( OrganizationRole.getRoleIdFromString( roleIdString ) ).unwrap();
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

}
