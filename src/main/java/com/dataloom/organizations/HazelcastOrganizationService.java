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

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.Organization;
import com.dataloom.organization.roles.Role;
import com.dataloom.organizations.events.OrganizationCreatedEvent;
import com.dataloom.organizations.events.OrganizationDeletedEvent;
import com.dataloom.organizations.events.OrganizationUpdatedEvent;
import com.dataloom.organizations.processors.EmailDomainsMerger;
import com.dataloom.organizations.processors.EmailDomainsRemover;
import com.dataloom.organizations.processors.OrganizationMemberMerger;
import com.dataloom.organizations.processors.OrganizationMemberRemover;
import com.dataloom.organizations.roles.RolesManager;
import com.dataloom.organizations.roles.processors.PrincipalDescriptionUpdater;
import com.dataloom.organizations.roles.processors.PrincipalTitleUpdater;
import com.dataloom.streams.StreamUtil;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.openlattice.authorization.SecurablePrincipal;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastOrganizationService {

    private static final Logger logger = LoggerFactory
            .getLogger( HazelcastOrganizationService.class );
    private final AuthorizationManager              authorizations;
    private final HazelcastAclKeyReservationService reservations;
    private final UserDirectoryService              principals;
    private final RolesManager                      rolesManager;
    private final IMap<UUID, String>                titles;
    private final IMap<UUID, String>                descriptions;
    private final IMap<UUID, DelegatedStringSet>    autoApprovedEmailDomainsOf;
    private final IMap<UUID, PrincipalSet>          membersOf;
    private final List<IMap<UUID, ?>>               allMaps;
    @Inject
    private       EventBus                          eventBus;

    public HazelcastOrganizationService(
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService reservations,
            AuthorizationManager authorizations,
            UserDirectoryService principals,
            RolesManager rolesManager ) {
        this.titles = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_TITLES.name() );
        this.descriptions = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_DESCRIPTIONS.name() );
        this.autoApprovedEmailDomainsOf = hazelcastInstance.getMap( HazelcastMap.ALLOWED_EMAIL_DOMAINS.name() );
        this.membersOf = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_MEMBERS.name() );
        this.authorizations = authorizations;
        this.reservations = reservations;
        this.allMaps = ImmutableList.of( titles,
                descriptions,
                autoApprovedEmailDomainsOf,
                membersOf );
        this.principals = checkNotNull( principals );
        this.rolesManager = rolesManager;
    }

    public void createOrganization( Principal principal, Organization organization ) {
        rolesManager.createSecurablePrincipalIfNotExists( principal, organization.getPrincipal() );
        createOrganization( organization );
        eventBus.post( new OrganizationCreatedEvent( organization, principal ) );
    }

    public void createOrganization( Organization organization ) {
        UUID organizationId = organization.getPrincipal().getId();
        titles.set( organizationId, organization.getTitle() );
        descriptions.set( organizationId, organization.getDescription() );
        autoApprovedEmailDomainsOf.set( organizationId,
                DelegatedStringSet.wrap( organization.getAutoApprovedEmails() ) );
        membersOf.set( organizationId, PrincipalSet.wrap( organization.getMembers() ) );
    }

    public Organization getOrganization( UUID organizationId ) {
        Future<PrincipalSet> members = membersOf.getAsync( organizationId );
        Future<DelegatedStringSet> autoApprovedEmailDomains = autoApprovedEmailDomainsOf.getAsync( organizationId );

        Collection<SecurablePrincipal> maybeOrgs =
                rolesManager.getPrincipals( getOrganizationPredicate( organizationId ) );
        SecurablePrincipal principal = Iterables.getOnlyElement( maybeOrgs );
        Set<Role> roles = getRoles( organizationId );
        try {
            return new Organization(
                    principal,
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
        rolesManager
                .executeOnPrincipal( new PrincipalTitleUpdater( title ), getOrganizationPredicate( organizationId ) );
        eventBus.post( new OrganizationUpdatedEvent( organizationId, Optional.of( title ), Optional.absent() ) );
    }

    public void updateDescription( UUID organizationId, String description ) {
        rolesManager
                .executeOnPrincipal( new PrincipalDescriptionUpdater( description ),
                        getOrganizationPredicate( organizationId ) );
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
        membersOf.submitToKey( organizationId, new OrganizationMemberMerger( members ) );
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
        membersOf.submitToKey( organizationId, new OrganizationMemberRemover( members ) );
        removeRolesFromMembers(
                getRolesInFull( organizationId ).stream().map( Role::getPrincipal )::iterator,
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

    private void removeRolesFromMembers( Iterable<Principal> roles, Set<Principal> members ) {
        if ( members.stream().map( Principal::getType ).allMatch( PrincipalType.USER::equals ) ) {
            members.forEach( member -> roles.forEach( role -> removeRoleFromUser( role, member ) ) );
        } else {
            throw new IllegalArgumentException( "Cannot remove a non-user role from member of an organization." );
        }
    }

    public void createRoleIfNotExists( Principal callingUser, Role role ) {
        rolesManager.createSecurablePrincipalIfNotExists( callingUser, role );
    }

    public void updateRoleTitle( Principal role, String title ) {
        rolesManager.updateTitle( role, title );
    }

    public void updateRoleDescription( Principal role, String description ) {
        rolesManager.updateDescription( role, description );
    }

    public Role getRoleInFull( Principal role ) {
        return (Role) rolesManager.getPrincipal( role );
    }

    private Collection<Role> getRolesInFull( UUID organizationId ) {
        return rolesManager.getAllRolesInOrganization( organizationId )
                .stream()
                .map( sp -> (Role) sp )
                .collect( Collectors.toList() );
    }

    public Set<Role> getRoles( UUID organizationId ) {
        return StreamUtil.stream( getRolesInFull( organizationId ) ).collect( Collectors.toSet() );
    }

    public void deleteRole( Principal role ) {
        rolesManager.deletePrincipal( role );
    }

    public void addRoleToUser( Principal principal, Principal user ) {
        rolesManager.addPrincipalToPrincipal( principal, user );
    }

    public void removeRoleFromUser( Principal role, Principal user ) {
        rolesManager.removePrincipalFromPrincipal( role, user );
    }

    public Iterable<Auth0UserBasic> getAllUserProfilesOfRole( Principal role ) {
        return rolesManager.getAllUserProfilesWithPrincipal( role );
    }

    private static Predicate getOrganizationPredicate( UUID organizationId ) {
        return Predicates.and(
                Predicates.equal( "principalType", PrincipalType.ORGANIZATION ),
                Predicates.equal( "aclKey[0]", organizationId ) );
    }
}
