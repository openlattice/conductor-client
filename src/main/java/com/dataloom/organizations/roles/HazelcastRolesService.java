package com.dataloom.organizations.roles;

import java.util.UUID;

import com.clearspring.analytics.util.Preconditions;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organizations.roles.processors.RoleDescriptionUpdater;
import com.dataloom.organizations.roles.processors.RoleTitleUpdater;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastRolesService implements RolesManager, AuthorizingComponent {
    private final AuthorizationManager              authorizations;

    private final HazelcastAclKeyReservationService reservations;

    private final IMap<RoleKey, OrganizationRole>   roles;

    private final IMap<UUID, String>                orgsTitles;

    public HazelcastRolesService(
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService reservations,
            AuthorizationManager authorizations ) {
        this.roles = hazelcastInstance.getMap( HazelcastMap.ROLES.name() );
        this.authorizations = authorizations;
        this.reservations = reservations;

        this.orgsTitles = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_TITLES.name() );
    }

    @Override
    public void createRoleIfNotExists( OrganizationRole role ) {
        ensureValidOrganizationRole( role );
        reservations.reserveIdAndValidateType( role );

        RoleKey key = new RoleKey( role.getOrganizationId(), role.getId() );
        Preconditions.checkState(
                roles.putIfAbsent( key, role ) == null,
                "Organization Role already exists." );
    }

    @Override
    public void updateTitle( UUID organizationId, UUID roleId, String title ) {
        roles.executeOnKey( new RoleKey( organizationId, roleId ), new RoleTitleUpdater( title ) );
    }

    @Override
    public void updateDescription( UUID organizationId, UUID roleId, String description ) {
        roles.executeOnKey( new RoleKey( organizationId, roleId ), new RoleDescriptionUpdater( description ) );
    }

    @Override
    public Iterable<OrganizationRole> getAllRoles() {
        return roles.values();
    }

    @Override
    public void deleteRole( UUID organizationId, UUID roleId ) {
        // TODO remove all users with that role
        roles.delete( new RoleKey( organizationId, roleId ) );
        reservations.release( roleId );
    }

    private void ensureValidOrganizationRole( OrganizationRole role ) {
        // check organization exists
        Preconditions.checkArgument( orgsTitles.containsKey( role.getOrganizationId() ),
                "Organization associated to this role does not exist." );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

}
