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

import com.dataloom.apps.App;
import com.dataloom.apps.AppConfigKey;
import com.dataloom.apps.AppType;
import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.mapstores.PermissionMapstore;
import com.dataloom.authorization.mapstores.SecurableObjectTypeMapstore;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.blocking.LinkingEntity;
import com.dataloom.data.EntityKey;
import com.dataloom.data.hazelcast.DataKey;
import com.dataloom.data.mapstores.*;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.mapstores.*;
import com.dataloom.edm.schemas.mapstores.SchemaMapstore;
import com.dataloom.edm.set.EntitySetPropertyKey;
import com.dataloom.edm.set.EntitySetPropertyMetadata;
import com.dataloom.edm.type.*;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.graph.mapstores.PostgresEdgeMapstore;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.LinkingEntityKey;
import com.dataloom.linking.LinkingVertex;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.WeightedLinkingVertexKeySet;
import com.dataloom.linking.mapstores.*;
import com.dataloom.organization.roles.Role;
import com.dataloom.organization.roles.RoleKey;
import com.dataloom.organizations.PrincipalSet;
import com.dataloom.organizations.mapstores.StringMapstore;
import com.dataloom.organizations.mapstores.StringSetMapstore;
import com.dataloom.organizations.mapstores.UserSetMapstore;
import com.dataloom.organizations.roles.mapstores.RolesMapstore;
import com.dataloom.organizations.roles.mapstores.UsersWithRoleMapstore;
import com.dataloom.requests.Status;
import com.dataloom.requests.mapstores.RequestMapstore;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDSet;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.pods.hazelcast.QueueConfigurer;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.postgres.mapstores.AppConfigMapstore;
import com.openlattice.postgres.mapstores.AppMapstore;
import com.openlattice.postgres.mapstores.AppTypeMapstore;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

@Configuration
@Import( { CassandraPod.class, PostgresPod.class } )
public class MapstoresPod {

    @Inject
    private Session                session;
    @Inject
    private CassandraConfiguration cc;
    @Inject
    private HikariDataSource       hikariDataSource;

    @Inject
    private PostgresTableManager ptMgr;
    //    @Bean
    //    public SelfRegisteringMapStore<UUID, Neighborhood> edgesMapstore() {
    //        return new EdgesMapstore( session );
    //    }

    //    @Bean
    //    public SelfRegisteringMapStore<UUID, Neighborhood> backedgesMapstore() {
    //        return new BackedgesMapstore( session );
    //    }

    @Bean
    public SelfRegisteringMapStore<EdgeKey, LoomEdge> edgesMapstore() throws SQLException {
        return new PostgresEdgeMapstore( HazelcastMap.EDGES.name(), hikariDataSource );
    }

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
        PropertyTypeMapstore cptm = new PropertyTypeMapstore( session );
        //        com.openlattice.postgres.mapstores.PropertyTypeMapstore ptm = new com.openlattice.postgres.mapstores.PropertyTypeMapstore(
        //                HazelcastMap.PROPERTY_TYPES.name(),
        //                PROPERTY_TYPES,
        //                hikariDataSource );
        //        for ( UUID id : cptm.loadAllKeys() ) {
        //            ptm.store( id, cptm.load( id ) );
        //        }
        return cptm;
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
    public SelfRegisteringMapStore<String, UUID> edmVersionMapstore() {
        return new EdmVersionMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> namesMapstore() {
        return new NamesMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<AceKey, Status> requestMapstore() {
        return new RequestMapstore( session );
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
    public SelfRegisteringMapStore<LinkingVertexKey, WeightedLinkingVertexKeySet> linkingEdgesMapstore() {
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
    public SelfRegisteringMapStore<RoleKey, Role> rolesMapstore() {
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

    //Still using Cassandra for mapstores below to avoid contention on data integrations
    @Bean
    public SelfRegisteringMapStore<EntityKey, UUID> idsMapstore() throws SQLException {
        //return new PostgresEntityKeyIdsMapstore( HazelcastMap.IDS.name(), hikariDataSource, keysMapstore() );
        return new EntityKeyIdsMapstore( keysMapstore(), HazelcastMap.IDS.name(), session, Table.IDS.getBuilder() );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntityKey> keysMapstore() throws SQLException {
        //        return new PostgresEntityKeysMapstore( HazelcastMap.KEYS.name(), hikariDataSource );
        return new EntityKeysMapstore( HazelcastMap.KEYS.name(), session, Table.KEYS.getBuilder() );
    }

    @Bean
    public SelfRegisteringMapStore<LinkingVertexKey, UUID> vertexIdsAfterLinkingMapstore() {
        return new VertexIdsAfterLinkingMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<DataKey, ByteBuffer> dataMapstore() throws SQLException {
        return new PostgresDataMapstore( HazelcastMap.DATA.name(), session, hikariDataSource );
        //        ObjectMapper mapper = ObjectMappers.getJsonMapper();
        //        FullQualifedNameJacksonSerializer.registerWithMapper( mapper );
        //        FullQualifedNameJacksonDeserializer.registerWithMapper( mapper );
        //        return new DataMapstore( HazelcastMap.DATA.name(),
        //                Table.DATA.getBuilder(),
        //                session,
        //                propertyTypeMapstore(),
        //                mapper );
    }

    @Bean
    public SelfRegisteringMapStore<EntitySetPropertyKey, EntitySetPropertyMetadata> entitySetPropertyMetadataMapstore() {
        return new EntitySetPropertyMetadataMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<GraphEntityPair, LinkingEntity> linkingEntityMapstore() {
        return new LinkingEntityMapstore( session );
    }

    @Bean
    public QueueConfigurer defaultQueueConfigurer() {
        return config -> config.setMaxSize( 10000 ).setEmptyQueueTtl( 60 );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, App> appMapstore() {
        return new AppMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, AppType> appTypeMapstore() {
        return new AppTypeMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<AppConfigKey, UUID> appConfigMapstore() {
        return new AppConfigMapstore( hikariDataSource );
    }
}
