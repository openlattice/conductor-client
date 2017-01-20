package com.dataloom.hazelcast.pods;

import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.mapstores.PermissionMapstore;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.mapstores.AclKeysMapstore;
import com.dataloom.edm.mapstores.EntitySetMapstore;
import com.dataloom.edm.mapstores.EntityTypeMapstore;
import com.dataloom.edm.mapstores.FqnsMapstore;
import com.dataloom.edm.mapstores.PropertyTypeMapstore;
import com.dataloom.edm.schemas.mapstores.SchemaMapstore;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organizations.PrincipalSet;
import com.dataloom.organizations.mapstores.RoleSetMapstore;
import com.dataloom.organizations.mapstores.StringMapstore;
import com.dataloom.organizations.mapstores.StringSetMapstore;
import com.dataloom.organizations.mapstores.UUIDSetMapstore;
import com.dataloom.organizations.mapstores.UserSetMapstore;
import com.dataloom.requests.AclRootRequestDetailsPair;
import com.dataloom.requests.PermissionsRequestDetails;
import com.dataloom.requests.mapstores.AclRootPrincipalPair;
import com.dataloom.requests.mapstores.PrincipalRequestIdPair;
import com.dataloom.requests.mapstores.ResolvedPermissionsRequestsMapstore;
import com.dataloom.requests.mapstores.UnresolvedPermissionsRequestsMapstore;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDSet;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.pods.CassandraPod;

@Configuration
@Import( CassandraPod.class )
public class MapstoresPod {

    @Inject
    Session session;

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
    public SelfRegisteringMapStore<FullQualifiedName, UUID> aclKeysMapstore() {
        return new AclKeysMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, FullQualifiedName> fqnsMapstore() {
        return new FqnsMapstore( session );
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
