package com.dataloom.hazelcast.pods;

import com.dataloom.auditing.AuditMetric;
import com.dataloom.auditing.mapstores.LeaderboardMapstore;
import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.AclKey;
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.mapstores.PermissionMapstore;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.mapstores.*;
import com.dataloom.edm.schemas.mapstores.SchemaMapstore;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organizations.PrincipalSet;
import com.dataloom.organizations.mapstores.*;
import com.dataloom.requests.AclRootRequestDetailsPair;
import com.dataloom.requests.PermissionsRequestDetails;
import com.dataloom.requests.Status;
import com.dataloom.requests.mapstores.*;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDSet;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.pods.CassandraPod;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.inject.Inject;
import java.util.UUID;

@Configuration
@Import( CassandraPod.class )
public class MapstoresPod {

    @Inject
    Session session;

    @Inject CassandraConfiguration cc;

    @Bean
    public SelfRegisteringMapStore<AceKey, DelegatedPermissionEnumSet> permissionMapstore() {
        return new PermissionMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, PropertyType> propertyTypeMapstore() {
        return new PropertyTypeMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntityType> entityTypeMapstore() {
        return new EntityTypeMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntitySet> entitySetMapstore() {
        return new EntitySetMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<String, DelegatedStringSet> schemaMapstore() {
        return new SchemaMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<String, UUID> aclKeysMapstore() {
        return new AclKeysMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> namesMapstore() {
        return new NamesMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<AclRootPrincipalPair, PermissionsRequestDetails> unresolvedRequestsMapstore() {
        return new UnresolvedPermissionsRequestsMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<PrincipalRequestIdPair, AclRootRequestDetailsPair> resolvedRequestsMapstore() {
        return new ResolvedPermissionsRequestsMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<AceKey, Status> requestMapstore() {
        return new RequestMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<AclKey, AuditMetric> auditMetricsMapstore() {
        return new LeaderboardMapstore( cc.getKeyspace(), session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> orgTitlesMapstore() {
        return new StringMapstore(
                HazelcastMap.TITLES,
                session,
                Tables.ORGANIZATIONS,
                CommonColumns.ID,
                CommonColumns.TITLE );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> orgDescsMapstore() {
        return new StringMapstore(
                HazelcastMap.DESCRIPTIONS,
                session,
                Tables.ORGANIZATIONS,
                CommonColumns.ID,
                CommonColumns.DESCRIPTION );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedUUIDSet> trustedOrgsMapstore() {
        return new UUIDSetMapstore(
                HazelcastMap.TRUSTED_ORGANIZATIONS,
                session,
                Tables.ORGANIZATIONS,
                CommonColumns.ID,
                CommonColumns.TRUSTED_ORGANIZATIONS );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedStringSet> aaEmailDomainsMapstore() {
        return new StringSetMapstore(
                HazelcastMap.ALLOWED_EMAIL_DOMAINS,
                session,
                Tables.ORGANIZATIONS,
                CommonColumns.ID,
                CommonColumns.ALLOWED_EMAIL_DOMAINS );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, PrincipalSet> rolesMapstore() {
        return new RoleSetMapstore(
                HazelcastMap.ROLES,
                session,
                Tables.ORGANIZATIONS,
                CommonColumns.ID,
                CommonColumns.ROLES );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, PrincipalSet> membersMapstore() {
        return new UserSetMapstore(
                HazelcastMap.MEMBERS,
                session,
                Tables.ORGANIZATIONS,
                CommonColumns.ID,
                CommonColumns.MEMBERS );
    }
}
