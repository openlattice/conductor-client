package com.dataloom.organizations;

import static com.google.common.base.Preconditions.checkNotNull;

import java.security.InvalidParameterException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.Organization;
import com.dataloom.organizations.processors.EmailDomainsMerger;
import com.dataloom.organizations.processors.EmailDomainsRemover;
import com.dataloom.organizations.processors.PrincipalMerger;
import com.dataloom.organizations.processors.PrincipalRemover;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDSet;

public class HazelcastOrganizationService {
    private static final Logger                     logger = LoggerFactory
            .getLogger( HazelcastOrganizationService.class );

    private final AuthorizationManager              authorizations;
    private final HazelcastAclKeyReservationService reservations;
    private final UserDirectoryService              principals;
    private final IMap<UUID, String>                titles;
    private final IMap<UUID, String>                descriptions;
    private final IMap<UUID, DelegatedUUIDSet>      trustedOrgsOf;
    private final IMap<UUID, DelegatedStringSet>    autoApprovedEmailDomainsOf;
    private final IMap<UUID, PrincipalSet>          membersOf;
    private final IMap<UUID, PrincipalSet>          rolesOf;
    private final List<IMap<UUID, ?>>               allMaps;

    public HazelcastOrganizationService(
            String keyspace,
            Session session,
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService reservations,
            AuthorizationManager authorizations,
            UserDirectoryService principals ) {
        this.titles = hazelcastInstance.getMap( HazelcastMap.TITLES.name() );
        this.descriptions = hazelcastInstance.getMap( HazelcastMap.DESCRIPTIONS.name() );
        this.trustedOrgsOf = hazelcastInstance.getMap( HazelcastMap.TRUSTED_ORGANIZATIONS.name() );
        this.autoApprovedEmailDomainsOf = hazelcastInstance.getMap( HazelcastMap.ALLOWED_EMAIL_DOMAINS.name() );
        this.membersOf = hazelcastInstance.getMap( HazelcastMap.MEMBERS.name() );
        this.rolesOf = hazelcastInstance.getMap( HazelcastMap.ROLES.name() );
        this.authorizations = authorizations;
        this.reservations = reservations;
        this.allMaps = ImmutableList.of( titles,
                descriptions,
                trustedOrgsOf,
                autoApprovedEmailDomainsOf,
                membersOf,
                rolesOf );
        this.principals = checkNotNull( principals );
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
        rolesOf.set( organizationId, PrincipalSet.wrap( organization.getRoles() ) );

    }

    public Organization getOrganization( UUID organizationId ) {
        Future<String> title = titles.getAsync( organizationId );
        Future<String> description = descriptions.getAsync( organizationId );
        Future<DelegatedUUIDSet> trustedOrgs = trustedOrgsOf.getAsync( organizationId );
        Future<PrincipalSet> members = membersOf.getAsync( organizationId );
        Future<DelegatedStringSet> autoApprovedEmailDomains = autoApprovedEmailDomainsOf.getAsync( organizationId );
        Future<PrincipalSet> roles = rolesOf.getAsync( organizationId );

        try {
            return new Organization(
                    Optional.of( organizationId ),
                    title.get(),
                    Optional.fromNullable( description.get() ),
                    Optional.fromNullable( trustedOrgs.get() ),
                    autoApprovedEmailDomains.get(),
                    members.get(),
                    roles.get() );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to load organization. {}", organizationId, e );
            return null;
        }
    }

    public void destroyOrganization( UUID organizationId ) {
        allMaps.stream().forEach( m -> m.delete( organizationId ) );
        reservations.release( organizationId );
    }

    public void updateTitle( UUID organizationId, String title ) {
        titles.set( organizationId, title );
    }

    public void updateDescription( UUID organizationId, String description ) {
        descriptions.set( organizationId, description );
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

    public Set<Principal> getPrincipals( UUID organizationId ) {
        Set<Principal> principals = getMembers( organizationId );
        principals.addAll( getRoles( organizationId ) );
        return principals;
    }

    public Set<Principal> getMembers( UUID organizationId ) {
        return Util.getSafely( membersOf, organizationId );
    }

    public Set<Principal> getRoles( UUID organizationId ) {
        return Util.getSafely( rolesOf, organizationId );
    }

    public void addPrincipals( UUID organizationId, Set<Principal> principals ) {
        /*
         * We collect to a set here instead of just passing the stream to the entry processor to avoid serialization of
         * the entire set as part of the stream closure.
         */
        Set<Principal> roles = principals
                .stream()
                .filter( p -> p.getType().equals( PrincipalType.ROLE ) )
                .collect( Collectors.toSet() );
        addRoles( organizationId, roles );
        Set<Principal> members = principals
                .stream()
                .filter( p -> p.getType().equals( PrincipalType.USER ) )
                .collect( Collectors.toSet() );
        addMembers( organizationId, members );
    }

    public void addRoles( UUID organizationId, Set<Principal> roles ) {
        rolesOf.submitToKey( organizationId, new PrincipalMerger( roles ) );
    }

    public void addMembers( UUID organizationId, Set<Principal> members ) {
        membersOf.submitToKey( organizationId, new PrincipalMerger( members ) );
        addOrganizationRoleToMembers( organizationId, members );
    }

    public void setPrincipals( UUID organizationId, Set<Principal> principals ) {
        /*
         * We collect to a set here instead of just passing the stream to the entry processor to avoid serialization of
         * the entire set as part of the stream closure.
         */
        Set<Principal> roles = principals
                .stream()
                .filter( p -> p.getType().equals( PrincipalType.ROLE ) )
                .collect( Collectors.toSet() );
        setRoles( organizationId, roles );
        Set<Principal> members = principals
                .stream()
                .filter( p -> p.getType().equals( PrincipalType.USER ) )
                .collect( Collectors.toSet() );
        setMembers( organizationId, members );
    }

    public void setRoles( UUID organizationId, Set<Principal> roles ) {
        rolesOf.set( organizationId, PrincipalSet.wrap( roles ) );
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

        membersOf.set( organizationId, PrincipalSet.wrap( members ) );
        
        addOrganizationRoleToMembers( organizationId, added );
        removeOrganizationRoleFromMembers( organizationId, removed );
    }

    public void removePrincipals( UUID organizationId, Set<Principal> principals ) {
        /*
         * We collect to a set here instead of just passing the stream to the entry processor to avoid serialization of
         * the entire set as part of the stream closure.
         */
        Set<Principal> roles = principals
                .stream()
                .filter( p -> p.getType().equals( PrincipalType.ROLE ) )
                .collect( Collectors.toSet() );
        removeRoles( organizationId, roles );
        Set<Principal> members = principals
                .stream()
                .filter( p -> p.getType().equals( PrincipalType.USER ) )
                .collect( Collectors.toSet() );
        removeMembers( organizationId, members );
    }

    public void removeRoles( UUID organizationId, Set<Principal> roles ) {
        rolesOf.submitToKey( organizationId, new PrincipalRemover( roles ) );
    }

    public void removeMembers( UUID organizationId, Set<Principal> members ) {
        membersOf.submitToKey( organizationId, new PrincipalRemover( members ) );
        removeOrganizationRoleFromMembers( organizationId, members );
    }

    private void addOrganizationRoleToMembers( UUID organizationId, Set<Principal> members ) {
        if ( members.stream().map( Principal::getType ).allMatch( PrincipalType.USER::equals ) ) {
            members.forEach( member -> principals.addOrganizationToUser( member.getId(), organizationId ) );
        } else {
            throw new InvalidParameterException( "Cannot add a non-user role as a member of an organization." );
        }
    }

    private void removeOrganizationRoleFromMembers( UUID organizationId, Set<Principal> members ) {
        if ( members.stream().map( Principal::getType ).allMatch( PrincipalType.USER::equals ) ) {
            members.forEach( member -> principals.removeOrganizationFromUser( member.getId(), organizationId ) );
        } else {
            throw new InvalidParameterException( "Cannot add a non-user role as a member of an organization." );
        }
    }
}
