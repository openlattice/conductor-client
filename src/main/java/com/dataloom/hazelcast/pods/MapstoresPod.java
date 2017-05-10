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

package com.dataloom.hazelcast.pods;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.auditing.AuditMetric;
import com.dataloom.auditing.mapstores.LeaderboardMapstore;
import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.AclKey;
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.mapstores.PermissionMapstore;
import com.dataloom.authorization.mapstores.SecurableObjectTypeMapstore;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.data.EntityKey;
import com.dataloom.data.mapstores.EntityKeyIdsMapstore;
import com.dataloom.data.mapstores.EntityKeysMapstore;
import com.dataloom.data.mapstores.SyncIdsMapstore;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.mapstores.AclKeysMapstore;
import com.dataloom.edm.mapstores.AssociationTypeMapstore;
import com.dataloom.edm.mapstores.ComplexTypeMapstore;
import com.dataloom.edm.mapstores.EntitySetMapstore;
import com.dataloom.edm.mapstores.EntityTypeMapstore;
import com.dataloom.edm.mapstores.EnumTypesMapstore;
import com.dataloom.edm.mapstores.NamesMapstore;
import com.dataloom.edm.mapstores.PropertyTypeMapstore;
import com.dataloom.edm.schemas.mapstores.SchemaMapstore;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.ComplexType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.EnumType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.LinkingEntityKey;
import com.dataloom.linking.LinkingVertex;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.mapstores.LinkedEntitySetsMapstore;
import com.dataloom.linking.mapstores.LinkedEntityTypesMapstore;
import com.dataloom.linking.mapstores.LinkingEdgesMapstore;
import com.dataloom.linking.mapstores.LinkingEntityVerticesMapstore;
import com.dataloom.linking.mapstores.LinkingVerticesMapstore;
import com.dataloom.linking.mapstores.VertexIdsAfterLinkingMapstore;
import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organization.roles.RoleKey;
import com.dataloom.organizations.PrincipalSet;
import com.dataloom.organizations.mapstores.StringMapstore;
import com.dataloom.organizations.mapstores.StringSetMapstore;
import com.dataloom.organizations.mapstores.UserSetMapstore;
import com.dataloom.organizations.roles.mapstores.RolesMapstore;
import com.dataloom.organizations.roles.mapstores.UsersWithRoleMapstore;
import com.dataloom.requests.AclRootRequestDetailsPair;
import com.dataloom.requests.PermissionsRequestDetails;
import com.dataloom.requests.Status;
import com.dataloom.requests.mapstores.AclRootPrincipalPair;
import com.dataloom.requests.mapstores.PrincipalRequestIdPair;
import com.dataloom.requests.mapstores.RequestMapstore;
import com.dataloom.requests.mapstores.ResolvedPermissionsRequestsMapstore;
import com.dataloom.requests.mapstores.UnresolvedPermissionsRequestsMapstore;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDSet;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.pods.CassandraPod;

@Configuration
@Import( CassandraPod.class )
public class MapstoresPod {

    @Inject
    Session                session;

    @Inject
    CassandraConfiguration cc;

    @Bean
    public SelfRegisteringMapStore<AceKey, DelegatedPermissionEnumSet> permissionMapstore() {
        return new PermissionMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<List<UUID>, SecurableObjectType> securableObjectTypeMapstore() {
        return new SecurableObjectTypeMapstore( session );
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
    public SelfRegisteringMapStore<UUID, ComplexType> complexTypeMapstore() {
        return new ComplexTypeMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EnumType> enumTypeMapstore() {
        return new EnumTypesMapstore( session );
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
                HazelcastMap.ORGANIZATIONS_TITLES,
                session,
                Table.ORGANIZATIONS,
                CommonColumns.ID,
                CommonColumns.TITLE );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> orgDescsMapstore() {
        return new StringMapstore(
                HazelcastMap.ORGANIZATIONS_DESCRIPTIONS,
                session,
                Table.ORGANIZATIONS,
                CommonColumns.ID,
                CommonColumns.DESCRIPTION );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedStringSet> aaEmailDomainsMapstore() {
        return new StringSetMapstore(
                HazelcastMap.ALLOWED_EMAIL_DOMAINS,
                session,
                Table.ORGANIZATIONS,
                CommonColumns.ID,
                CommonColumns.ALLOWED_EMAIL_DOMAINS );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, PrincipalSet> membersMapstore() {
        return new UserSetMapstore(
                HazelcastMap.ORGANIZATIONS_MEMBERS,
                session,
                Table.ORGANIZATIONS,
                CommonColumns.ID,
                CommonColumns.MEMBERS );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedUUIDSet> linkedEntityTypesMapstore() {
        return new LinkedEntityTypesMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedUUIDSet> linkedEntitySetsMapstore() {
        return new LinkedEntitySetsMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<LinkingEdge, Double> linkingEdgesMapstore() {
        return new LinkingEdgesMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<LinkingVertexKey, LinkingVertex> linkingVerticesMapstore() {
        return new LinkingVerticesMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<LinkingEntityKey, UUID> linkingEntityVerticesMapstore() {
        return new LinkingEntityVerticesMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, AssociationType> edgeTypeMapstore() {
        return new AssociationTypeMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<RoleKey, OrganizationRole> rolesMapstore() {
        return new RolesMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<RoleKey, PrincipalSet> usersWithRolesMapstore() {
        return new UsersWithRoleMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, UUID> syncIdsMapstore() {
        return new SyncIdsMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<EntityKey, UUID> idsMapstore() {
        return new EntityKeyIdsMapstore( HazelcastMap.IDS.name(), session, Table.IDS.getBuilder() );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntityKey> keysMapstore() {
        return new EntityKeysMapstore( HazelcastMap.KEYS.name(), session, Table.KEYS.getBuilder() );
    }

    @Bean
    public SelfRegisteringMapStore<LinkingVertexKey, UUID> vertexIdsAfterLinkingMapstore() {
        return new VertexIdsAfterLinkingMapstore( session );
    }

}
