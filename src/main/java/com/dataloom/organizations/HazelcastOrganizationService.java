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

package com.dataloom.organizations;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Iterables;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.Organization;
import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organization.roles.RoleKey;
import com.dataloom.organizations.events.OrganizationCreatedEvent;
import com.dataloom.organizations.events.OrganizationDeletedEvent;
import com.dataloom.organizations.events.OrganizationUpdatedEvent;
import com.dataloom.organizations.processors.EmailDomainsMerger;
import com.dataloom.organizations.processors.EmailDomainsRemover;
import com.dataloom.organizations.processors.PrincipalMerger;
import com.dataloom.organizations.processors.PrincipalRemover;
import com.dataloom.organizations.roles.RolesManager;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDSet;

public class HazelcastOrganizationService {

    @Inject
    private EventBus                                eventBus;

    private static final Logger                     logger = LoggerFactory
            .getLogger( HazelcastOrganizationService.class );

    private final AuthorizationManager              authorizations;
    private final HazelcastAclKeyReservationService reservations;
    private final UserDirectoryService              principals;
    private final RolesManager                      rolesManager;

    private final IMap<UUID, String>                titles;
    private final IMap<UUID, String>                descriptions;
    private final IMap<UUID, DelegatedUUIDSet>      trustedOrgsOf;
    private final IMap<UUID, DelegatedStringSet>    autoApprovedEmailDomainsOf;
    private final IMap<UUID, PrincipalSet>          membersOf;
    private final List<IMap<UUID, ?>>               allMaps;

    public HazelcastOrganizationService(
            String keyspace,
            Session session,
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService reservations,
            AuthorizationManager authorizations,
            UserDirectoryService principals,
            RolesManager rolesManager ) {
        this.titles = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_TITLES.name() );
        this.descriptions = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_DESCRIPTIONS.name() );
        this.trustedOrgsOf = hazelcastInstance.getMap( HazelcastMap.TRUSTED_ORGANIZATIONS.name() );
        this.autoApprovedEmailDomainsOf = hazelcastInstance.getMap( HazelcastMap.ALLOWED_EMAIL_DOMAINS.name() );
        this.membersOf = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_MEMBERS.name() );
        this.authorizations = authorizations;
        this.reservations = reservations;
        this.allMaps = ImmutableList.of( titles,
                descriptions,
                trustedOrgsOf,
                autoApprovedEmailDomainsOf,
                membersOf );
        this.principals = checkNotNull( principals );
        this.rolesManager = rolesManager;
    }

    public void createOrganization( Principal principal, Organization organization ) {
        reservations.reserveId( organization );
        authorizations.addPermission( ImmutableList.of( organization.getId() ),
                principal,
                EnumSet.allOf( Permission.class ) );
        authorizations.createEmptyAcl( ImmutableList.of( organization.getId() ), SecurableObjectType.Organization );
        UUID organizationId = organization.getId();
        titles.set( organizationId, organization.getTitle() );
        descriptions.set( organizationId, organization.getDescription() );
        trustedOrgsOf.set( organizationId, DelegatedUUIDSet.wrap( organization.getTrustedOrganizations() ) );
        autoApprovedEmailDomainsOf.set( organizationId,
                DelegatedStringSet.wrap( organization.getAutoApprovedEmails() ) );
        membersOf.set( organizationId, PrincipalSet.wrap( organization.getMembers() ) );
        eventBus.post( new OrganizationCreatedEvent( organization, principal ) );

    }

    public Organization getOrganization( UUID organizationId ) {
        Future<String> title = titles.getAsync( organizationId );
        Future<String> description = descriptions.getAsync( organizationId );
        Future<DelegatedUUIDSet> trustedOrgs = trustedOrgsOf.getAsync( organizationId );
        Future<PrincipalSet> members = membersOf.getAsync( organizationId );
        Future<DelegatedStringSet> autoApprovedEmailDomains = autoApprovedEmailDomainsOf.getAsync( organizationId );

        Set<Principal> roles = getRoles( organizationId );
        try {
            return new Organization(
                    Optional.of( organizationId ),
                    title.get(),
                    Optional.fromNullable( description.get() ),
                    Optional.fromNullable( trustedOrgs.get() ),
                    autoApprovedEmailDomains.get(),
                    members.get(),
                    roles );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to load organization. {}", organizationId, e );
            return null;
        }
    }

    public void destroyOrganization( UUID organizationId ) {
        // Remove all roles
        rolesManager.deleteAllRolesInOrganization( organizationId, getMembers( organizationId ) );

        allMaps.stream().forEach( m -> m.delete( organizationId ) );
        reservations.release( organizationId );
        eventBus.post( new OrganizationDeletedEvent( organizationId ) );
    }

    public void updateTitle( UUID organizationId, String title ) {
        titles.set( organizationId, title );
        eventBus.post( new OrganizationUpdatedEvent( organizationId, Optional.of( title ), Optional.absent() ) );
    }

    public void updateDescription( UUID organizationId, String description ) {
        descriptions.set( organizationId, description );
        eventBus.post( new OrganizationUpdatedEvent( organizationId, Optional.absent(), Optional.of( description ) ) );
    }

    public Set<String> getAutoApprovedEmailDomains( UUID organizationId ) {
        return Util.getSafely( autoApprovedEmailDomainsOf, organizationId );
    }

    public void setAutoApprovedEmailDomains( UUID organizationId, Set<String> emailDomains ) {
        autoApprovedEmailDomainsOf.set( organizationId, DelegatedStringSet.wrap( emailDomains ) );
    }

    public void addAutoApprovedEmailDomains( UUID organizationId, Set<String> emailDomains ) {
        autoApprovedEmailDomainsOf.submitToKey( organizationId, new EmailDomainsMerger( emailDomains ) );
    }

    public void removeAutoApprovedEmailDomains( UUID organizationId, Set<String> emailDomains ) {
        autoApprovedEmailDomainsOf.submitToKey( organizationId, new EmailDomainsRemover( emailDomains ) );
    }

    public Set<Principal> getMembers( UUID organizationId ) {
        return Util.getSafely( membersOf, organizationId );
    }

    public void addMembers( UUID organizationId, Set<Principal> members ) {
        membersOf.submitToKey( organizationId, new PrincipalMerger( members ) );
        addOrganizationToMembers( organizationId, members );
    }

    public void setMembers( UUID organizationId, Set<Principal> members ) {
        Set<Principal> current = Util.getSafely( membersOf, organizationId );
        Set<Principal> removed = current
                .stream()
                .filter( member -> !members.contains( member ) && current.contains( member ) )
                .collect( Collectors.toSet() );

        Set<Principal> added = current
                .stream()
                .filter( member -> members.contains( member ) && !current.contains( member ) )
                .collect( Collectors.toSet() );

        addMembers( organizationId, added );
        removeMembers( organizationId, removed );
    }

    public void removeMembers( UUID organizationId, Set<Principal> members ) {
        membersOf.submitToKey( organizationId, new PrincipalRemover( members ) );
        removeRolesFromMembers( Iterables.transform( getRolesInFull( organizationId ), OrganizationRole::getRoleKey ),
                members );
        removeOrganizationFromMembers( organizationId, members );
    }

    private void addOrganizationToMembers( UUID organizationId, Set<Principal> members ) {
        if ( members.stream().map( Principal::getType ).allMatch( PrincipalType.USER::equals ) ) {
            members.forEach( member -> principals.addOrganizationToUser( member.getId(), organizationId ) );
        } else {
            throw new IllegalArgumentException( "Cannot add a non-user role as a member of an organization." );
        }
    }

    private void removeOrganizationFromMembers( UUID organizationId, Set<Principal> members ) {
        if ( members.stream().map( Principal::getType ).allMatch( PrincipalType.USER::equals ) ) {
            members.forEach( member -> principals.removeOrganizationFromUser( member.getId(), organizationId ) );
        } else {
            throw new IllegalArgumentException( "Cannot add a non-user role as a member of an organization." );
        }
    }

    public void addPrincipal( Principal callingUser, UUID organizationId, Principal principal ) {
        switch ( principal.getType() ) {
            case ROLE:
                OrganizationRole role = new OrganizationRole(
                        Optional.absent(),
                        organizationId,
                        principal.getId(),
                        Optional.absent() );
                Principals.ensureUser( callingUser );
                createRoleIfNotExists( callingUser, role );
                break;
            case USER:
                addMembers( organizationId, ImmutableSet.of( principal ) );
                break;
            default:
        }
    }

    public void removePrincipal( UUID organizationId, Principal principal ) {
        switch ( principal.getType() ) {
            case ROLE:
                RoleKey roleKey = getRoleKey( organizationId, principal );
                deleteRole( roleKey );
                break;
            case USER:
                removeMembers( organizationId, ImmutableSet.of( principal ) );
                break;
            default:
        }
    }

    private void removeRolesFromMembers( Iterable<RoleKey> roleKeys, Set<Principal> members ) {
        if ( members.stream().map( Principal::getType ).allMatch( PrincipalType.USER::equals ) ) {
            members.forEach( member -> roleKeys
                    .forEach( roleKey -> removeRoleFromUser( roleKey, member ) ) );
        } else {
            throw new IllegalArgumentException( "Cannot remove a non-user role from member of an organization." );
        }
    }

    public void createRoleIfNotExists( Principal callingUser, OrganizationRole role ) {
        rolesManager.createRoleIfNotExists( callingUser, role );
    }

    public void updateRoleTitle( RoleKey roleKey, String title ) {
        rolesManager.updateTitle( roleKey, title );
    }

    public void updateRoleDescription( RoleKey roleKey, String description ) {
        rolesManager.updateDescription( roleKey, description );
    }

    public OrganizationRole getRoleInFull( RoleKey roleKey ) {
        return rolesManager.getRole( roleKey );
    }

    public Iterable<OrganizationRole> getRolesInFull( UUID organizationId ) {
        return rolesManager.getAllRolesInOrganization( organizationId );
    }

    public Set<Principal> getRoles( UUID organizationId ) {
        return StreamUtil.stream( getRolesInFull( organizationId ) ).map( role -> role.getPrincipal() )
                .collect( Collectors.toSet() );
    }

    public RoleKey getRoleKey( UUID organizationId, Principal principal ) {
        return rolesManager.getRoleKey( organizationId, principal );
    }

    public void deleteRole( RoleKey roleKey ) {
        rolesManager.deleteRole( roleKey );
    }

    public void addRoleToUser( RoleKey roleKey, Principal user ) {
        rolesManager.addRoleToUser( roleKey, user );
    }

    public void removeRoleFromUser( RoleKey roleKey, Principal user ) {
        rolesManager.removeRoleFromUser( roleKey, user );
    }

    public Iterable<Auth0UserBasic> getAllUserProfilesOfRole( RoleKey roleKey ) {
        return rolesManager.getAllUserProfilesOfRole( roleKey );
    }

    /**
     * Validation methods
     */

    public void ensureValidOrganizationRole( OrganizationRole role ) {
        rolesManager.ensureValidOrganizationRole( role );
    }

}
