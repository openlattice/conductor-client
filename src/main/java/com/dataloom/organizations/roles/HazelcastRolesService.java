package com.dataloom.organizations.roles;

import com.dataloom.authorization.*;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.roles.Role;
import com.dataloom.organization.roles.RoleKey;
import com.dataloom.organizations.PrincipalSet;
import com.dataloom.organizations.processors.OrganizationMemberRoleMerger;
import com.dataloom.organizations.processors.OrganizationMemberRoleRemover;
import com.dataloom.organizations.roles.processors.RoleDescriptionUpdater;
import com.dataloom.organizations.roles.processors.RoleTitleUpdater;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.dataloom.streams.StreamUtil.stream;

public class HazelcastRolesService implements RolesManager, AuthorizingComponent {

    private static final Logger logger = LoggerFactory.getLogger( HazelcastRolesService.class );

    private final AuthorizationManager                      authorizations;
    private final HazelcastAclKeyReservationService         reservations;
    private final RolesQueryService                         rqs;
    private final AbstractSecurableObjectResolveTypeService securableObjectTypes;
    private final TokenExpirationTracker                    tokenTracker;
    private final UserDirectoryService                      uds;

    private final IMap<RoleKey, Role>         roles;
    private final IMap<RoleKey, PrincipalSet> usersWithRole;

    public HazelcastRolesService(
            HazelcastInstance hazelcastInstance,
            RolesQueryService rqs,
            HazelcastAclKeyReservationService reservations,
            TokenExpirationTracker tokenTracker,
            UserDirectoryService uds,
            AbstractSecurableObjectResolveTypeService securableObjectTypes,
            AuthorizationManager authorizations ) {

        this.rqs = rqs;
        this.authorizations = authorizations;
        this.reservations = reservations;
        this.tokenTracker = tokenTracker;
        this.uds = uds;
        this.securableObjectTypes = securableObjectTypes;

        this.roles = hazelcastInstance.getMap( HazelcastMap.ROLES.name() );
        this.usersWithRole = hazelcastInstance.getMap( HazelcastMap.USERS_WITH_ROLE.name() );
    }

    public void createRoleIfNotExists( Role role ) {

        /*
         * a Role is uniquely identified by both organizationId and roleId, but AclKey reservation only works with a
         * single UUID. since we can't use both organizationId and roleId for AclKey reservation, we need a unique
         * String to identify a Role for the UUID <-> String mapping. this should be thought of as a hack since we
         * really need a List<UUID> <-> String mapping.
         */
        reservations.reserveIdAndValidateType( role, role::getReservationName );

        Preconditions.checkState(
                roles.putIfAbsent( role.getRoleKey(), role ) == null,
                "Role already exists."
        );
    }

    @Override
    public void createRoleIfNotExists( Principal principal, Role role ) {

        Principals.ensureUser( principal );
        createRoleIfNotExists( role );

        try {
            List<UUID> roleAclKey = role.getRoleKey().getAclKey();
            authorizations.addPermission( roleAclKey, principal, EnumSet.allOf( Permission.class ) );
            authorizations.createEmptyAcl( roleAclKey, SecurableObjectType.Role );
        } catch ( Exception e ) {
            logger.error( "Unable to create role {} in organization {}", role.getTitle(), role.getOrganizationId(), e );
            Util.deleteSafely( roles, role.getRoleKey() );
            reservations.release( role.getId() );
            throw new IllegalStateException(
                    "Unable to create role: " + role.getTitle() + " in organization " + role.getOrganizationId()
            );
        }
    }

    @Override
    public void updateTitle( RoleKey roleKey, String title ) {

        String roleReservationName = Role.generateReservationName( roleKey.getOrganizationId(), title );
        reservations.renameReservation( roleKey.getRoleId(), roleReservationName );
        roles.executeOnKey( roleKey, new RoleTitleUpdater( title ) );
    }

    @Override
    public void updateDescription( RoleKey roleKey, String description ) {
        roles.executeOnKey( roleKey, new RoleDescriptionUpdater( description ) );
    }

    @Override
    public Role getRole( RoleKey roleKey ) {
        return roles.get( roleKey );
    }

    @Override
    public List<Role> getAllRolesInOrganization( UUID organizationId ) {
        return rqs.getAllRolesInOrganization( organizationId );
    }

    @Override
    public void deleteRole( RoleKey roleKey ) {

        for ( Principal user : getAllUsersOfRole( roleKey ) ) {
            removeRoleFromUser( roleKey, user );
        }

        roles.delete( roleKey );
        reservations.release( roleKey.getRoleId() );
        authorizations.deletePermissions( roleKey.getAclKey() );
        securableObjectTypes.deleteSecurableObjectType( roleKey.getAclKey() );
    }

    @Override
    public void deleteAllRolesInOrganization( UUID organizationId, Iterable<Principal> users ) {

        List<Role> allRolesInOrg = getAllRolesInOrganization( organizationId );

        for ( Principal user : users ) {
            uds.removeAllRolesInOrganizationFromUser( user.getId(), allRolesInOrg );
            tokenTracker.trackUser( user.getId() );
        }

        for ( Role role : allRolesInOrg ) {
            authorizations.deletePermissions( role.getAclKey() );
            reservations.release( role.getId() );
            securableObjectTypes.deleteSecurableObjectType( role.getAclKey() );
        }

        rqs.deleteAllRolesInOrganization( organizationId );
    }

    @Override
    public void addRoleToUser( RoleKey roleKey, Principal user ) {

        Preconditions.checkArgument(
                user.getType() == PrincipalType.USER,
                "Cannot add roles to another ROLE object."
        );

        usersWithRole.executeOnKey( roleKey, new OrganizationMemberRoleMerger( ImmutableSet.of( user ) ) );
        uds.addRoleToUser( user.getId(), roleKey.getRoleId().toString() );

        tokenTracker.trackUser( user.getId() );
    }

    @Override
    public void removeRoleFromUser( RoleKey roleKey, Principal user ) {

        Preconditions.checkArgument(
                user.getType() == PrincipalType.USER,
                "Cannot remove roles from another ROLE object."
        );

        usersWithRole.executeOnKey( roleKey, new OrganizationMemberRoleRemover( ImmutableSet.of( user ) ) );
        uds.removeRoleFromUser( user.getId(), roleKey.getRoleId().toString() );

        tokenTracker.trackUser( user.getId() );
    }

    @Override
    public Iterable<Principal> getAllUsersOfRole( RoleKey roleKey ) {
        return usersWithRole.get( roleKey ).unwrap();
    }

    @Override
    public Iterable<Auth0UserBasic> getAllUserProfilesOfRole( RoleKey roleKey ) {
        return stream( getAllUsersOfRole( roleKey ) )
                .map( principal -> uds.getUser( principal.getId() ) )
                .collect( Collectors.toList() );
    }

    @Override
    public RoleKey getRoleKey( Principal principal ) {

        Preconditions.checkArgument(
                principal.getType() == PrincipalType.ROLE,
                "Only PrincipalType.ROLE is allowed."
        );

        UUID roleId;
        try {
            roleId = UUID.fromString( principal.getId() );
        } catch ( IllegalArgumentException | NullPointerException e ) {
            throw new IllegalArgumentException(
                    "Principals of type PrincipalType.ROLE must have a UUID for ID."
            );
        }

        UUID organizationId = roles.get( roleId ).getOrganizationId();
        return new RoleKey( organizationId, roleId );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

}
