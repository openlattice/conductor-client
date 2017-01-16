package com.dataloom.organizations;

import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.Organization;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastOrganizationService {
    private static final Logger logger = LoggerFactory.getLogger( HazelcastOrganizationService.class );

    private final AuthorizationManager              authorizations;
    private final HazelcastAclKeyReservationService reservations;
    private final IMap<UUID, String>                titles;
    private final IMap<UUID, String>                descriptions;
    private final IMap<UUID, Set<UUID>>             trustedOrgsOf;
    private final IMap<UUID, Set<String>>           autoApprovedEmailDomainsOf;
    private final IMap<UUID, Set<Principal>>        membersOf;
    private final IMap<UUID, Set<Principal>>        rolesOf;

    public HazelcastOrganizationService(
            String keyspace,
            Session session,
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService reservations,
            AuthorizationManager authorizations ) {
        this.trustedOrgsOf = hazelcastInstance.getMap( HazelcastMap.TRUSTED_ORGANIZATIONS.name() );
        this.titles = hazelcastInstance.getMap( HazelcastMap.TITLES.name() );
        this.descriptions = hazelcastInstance.getMap( HazelcastMap.DESCRIPTIONS.name() );
        this.autoApprovedEmailDomainsOf = hazelcastInstance.getMap( HazelcastMap.ALLOWED_EMAIL_DOMAINS.name() );
        this.membersOf = hazelcastInstance.getMap( HazelcastMap.MEMBERS.name() );
        this.rolesOf = hazelcastInstance.getMap( HazelcastMap.ROLES.name() );
        this.authorizations = authorizations;
        this.reservations = reservations;
    }

//    public Iterable<Organization> getOrganizations( Principal principal ) {
//        Iterable<AclKeyPathFragment> accessibleOrganizations = authorizations.getAuthorizedObjectsOfType( principal,
//                SecurableObjectType.Organization,
//                EnumSet.of( Permission.READ ) );
//        return Iterables.transform( accessibleOrganizations, this::loadOrganization );
//    }

    public void createOrganization( Principal principal, Organization organization ) {
        reservations.reserveAclKey( organization );
        authorizations.addPermission( ImmutableList.of( organization.getId() ),
                principal,
                EnumSet.allOf( Permission.class ) );
        UUID organizationId = organization.getId();
        titles.set( organizationId, organization.getTitle() );
        descriptions.set( organizationId, organization.getDescription() );
        trustedOrgsOf.set( organizationId, organization.getTrustedOrganizations() );
        autoApprovedEmailDomainsOf.set( organizationId, organization.getAutoApprovedEmails() );
        membersOf.set( organizationId, organization.getMembers() );
        rolesOf.set( organizationId, organization.getRoles() );

    }

    private Organization getOrganization( UUID organizationId ) {
        Future<String> title = titles.getAsync( organizationId );
        Future<String> description = descriptions.getAsync( organizationId );
        Future<Set<UUID>> trustedOrgs = trustedOrgsOf.getAsync( organizationId );
        Future<Set<Principal>> members = membersOf.getAsync( organizationId );
        Future<Set<String>> autoApprovedEmailDomains = autoApprovedEmailDomainsOf.getAsync( organizationId );
        Future<Set<Principal>> roles = rolesOf.getAsync( organizationId );

        try {
            return new Organization(
                    Optional.of( organizationId ),
                    title.get(),
                    Optional.of( description.get() ),
                    trustedOrgs.get(),
                    autoApprovedEmailDomains.get(),
                    members.get(),
                    roles.get() );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to load organization. {}", organizationId, e );
            return null;
        }
    }
}
