package com.dataloom.organizations.roles;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Iterables;

import com.clearspring.analytics.util.Preconditions;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.securable.SecurableObjectType;
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
import com.kryptnostic.datastore.util.Util;

public class HazelcastRolesService implements RolesManager, AuthorizingComponent {
    private static final Logger                     logger = LoggerFactory
            .getLogger( HazelcastRolesService.class );

    private final RolesQueryService                 rqs;

    private final AuthorizationManager              authorizations;
    private final HazelcastAclKeyReservationService reservations;
    private final ExpiringTokenTracker tokenTracker;
    private final UserDirectoryService              uds;

    private final IMap<RoleKey, OrganizationRole>   roles;
    private final IMap<RoleKey, PrincipalSet>       usersWithRole;

    private final IMap<UUID, String>                orgsTitles;

    public HazelcastRolesService(
            HazelcastInstance hazelcastInstance,
            RolesQueryService rqs,
            HazelcastAclKeyReservationService reservations,
            ExpiringTokenTracker tokenTracker,
            UserDirectoryService uds,
            AuthorizationManager authorizations ) {
        this.rqs = rqs;

        this.authorizations = authorizations;
        this.reservations = reservations;
        this.tokenTracker = tokenTracker;
        this.uds = uds;

        this.roles = hazelcastInstance.getMap( HazelcastMap.ROLES.name() );
        this.usersWithRole = hazelcastInstance.getMap( HazelcastMap.USERS_WITH_ROLE.name() );

        this.orgsTitles = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_TITLES.name() );
    }

    private void createRoleIfNotExists( OrganizationRole role ) {
        ensureValidOrganizationRole( role );

        reservations.reserveIdAndValidateType( role );

        Preconditions.checkState(
                roles.putIfAbsent( role.getRoleKey(), role ) == null,
                "Organization Role already exists." );
    }

    @Override
    public void createRoleIfNotExists( Principal principal, OrganizationRole role ) {
        Principals.ensureUser( principal );
        createRoleIfNotExists( role );

        try {
            authorizations.addPermission( role.getRoleKey().getAclKey(),
                    principal,
                    EnumSet.allOf( Permission.class ) );

            authorizations.createEmptyAcl( role.getRoleKey().getAclKey(), SecurableObjectType.OrganizationRole );
        } catch ( Exception e ) {
            logger.error( "Unable to create role {} in organization {}", role.getTitle(), role.getOrganizationId(), e );
            Util.deleteSafely( roles, role.getRoleKey() );
            reservations.release( role.getId() );
            throw new IllegalStateException(
                    "Unable to create role: " + role.getTitle() + " in organization " + role.getOrganizationId() );
        }
    }

    @Override
    public void updateTitle( RoleKey roleKey, String title ) {
        String newName = OrganizationRole.getStringRepresentation( roleKey.getOrganizationId(), title );
        reservations.renameReservation( roleKey.getRoleId(), newName );

        roles.executeOnKey( roleKey, new RoleTitleUpdater( title ) );
        //TODO update user roles in Auth0 too
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
    public Iterable<OrganizationRole> getAllRolesInOrganization( UUID organizationId ) {
        return rqs.getAllRolesInOrganization( organizationId );
    }

    @Override
    public void deleteRole( RoleKey roleKey ) {
        for ( Principal user : getAllUsersOfRole( roleKey ) ) {
            removeRoleFromUser( roleKey, user );
        }
        roles.delete( roleKey );
        reservations.release( roleKey.getRoleId() );
    }

    @Override
    public void deleteAllRolesInOrganization( UUID organizationId, Iterable<Principal> users ) {
        for ( Principal user : users ) {
            uds.removeAllRolesInOrganizationFromUser( user.getId(), organizationId );
            tokenTracker.trackUser( user.getId() );
        }
        for ( OrganizationRole role : getAllRolesInOrganization( organizationId ) ) {
            authorizations.deletePermissions( Arrays.asList( organizationId, role.getId() ) );
            reservations.release( role.getId() );
        }
        rqs.deleteAllRolesInOrganization( organizationId );
    }

    private void ensureValidOrganizationRole( OrganizationRole role ) {
        // check organization exists
        Preconditions.checkArgument( orgsTitles.containsKey( role.getOrganizationId() ),
                "Organization associated to this role does not exist." );
    }

    @Override
    public void addRoleToUser( RoleKey roleKey, Principal user ) {
        Preconditions.checkArgument( user.getType() == PrincipalType.USER, "Cannot add roles to another ROLE object." );
        
        usersWithRole.executeOnKey( roleKey, new PrincipalMerger( ImmutableSet.of( user ) ) );

        uds.addRoleToUser( user.getId(), getRole( roleKey ).toString() );
        tokenTracker.trackUser( user.getId() );
    }

    @Override
    public void removeRoleFromUser( RoleKey roleKey, Principal user ) {
        Preconditions.checkArgument( user.getType() == PrincipalType.USER,
                "Cannot remove roles from another ROLE object." );

        usersWithRole.executeOnKey( roleKey, new PrincipalRemover( ImmutableSet.of( user ) ) );

        uds.removeRoleFromUser( user.getId(), getRole( roleKey ).toString() );
        tokenTracker.trackUser( user.getId() );
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
