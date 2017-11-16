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
import com.dataloom.apps.AppTypeSetting;
import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.data.EntityKey;
import com.dataloom.data.hazelcast.DataKey;
import com.dataloom.data.mapstores.EntityKeyIdsMapstore;
import com.dataloom.data.mapstores.EntityKeysMapstore;
import com.dataloom.data.mapstores.PostgresDataMapstore;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.set.EntitySetPropertyKey;
import com.dataloom.edm.set.EntitySetPropertyMetadata;
import com.dataloom.edm.type.*;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.graph.mapstores.PostgresEdgeMapstore;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.LinkingVertex;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.WeightedLinkingVertexKeySet;
import com.dataloom.linking.mapstores.LinkedEntityTypesMapstore;
import com.dataloom.organizations.PrincipalSet;
import com.dataloom.requests.Status;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.pods.hazelcast.QueueConfigurer;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AclKeySet;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.mapstores.PermissionMapstore;
import com.openlattice.authorization.mapstores.PostgresCredentialMapstore;
import com.openlattice.authorization.mapstores.PrincipalMapstore;
import com.openlattice.authorization.mapstores.PrincipalTreeMapstore;
import com.openlattice.authorization.mapstores.UserMapstore;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.postgres.mapstores.*;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static com.openlattice.postgres.PostgresTable.PROPERTY_TYPES;

@Configuration
@Import( { CassandraPod.class, PostgresPod.class } )
public class MapstoresPod {

    @Inject
    private Session session;

    @Inject
    private CassandraConfiguration cc;

    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private PostgresTableManager ptMgr;

    @Bean
    public SelfRegisteringMapStore<EdgeKey, LoomEdge> edgesMapstore() throws SQLException {
        return new PostgresEdgeMapstore( HazelcastMap.EDGES.name(), hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<AceKey, DelegatedPermissionEnumSet> permissionMapstore() {
        PermissionMapstore ppm = new PermissionMapstore( hikariDataSource );
        return ppm;
    }

    @Bean
    public SelfRegisteringMapStore<List<UUID>, SecurableObjectType> securableObjectTypeMapstore() {
        SecurableObjectTypeMapstore psotm = new SecurableObjectTypeMapstore( hikariDataSource );
        return psotm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, PropertyType> propertyTypeMapstore() {
        //        PropertyTypeMapstore cptm = new PropertyTypeMapstore( session );
        com.openlattice.postgres.mapstores.PropertyTypeMapstore ptm = new com.openlattice.postgres.mapstores.PropertyTypeMapstore(
                HazelcastMap.PROPERTY_TYPES.name(),
                PROPERTY_TYPES,
                hikariDataSource );
        //        for ( UUID id : cptm.loadAllKeys() ) {
        //            ptm.store( id, cptm.load( id ) );
        //        }
        return ptm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntityType> entityTypeMapstore() {
        EntityTypeMapstore petm = new EntityTypeMapstore( hikariDataSource );

        //        com.dataloom.edm.mapstores.EntityTypeMapstore etm = new com.dataloom.edm.mapstores.EntityTypeMapstore( session );
        //        for ( UUID id : etm.loadAllKeys() ) {
        //            petm.store( id, etm.load( id ) );
        //        }
        return petm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, ComplexType> complexTypeMapstore() {
        return new ComplexTypeMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EnumType> enumTypeMapstore() {
        return new EnumTypesMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntitySet> entitySetMapstore() {
        EntitySetMapstore pesm = new EntitySetMapstore( hikariDataSource );

        //        com.dataloom.edm.mapstores.EntitySetMapstore esm = new com.dataloom.edm.mapstores.EntitySetMapstore( session );
        //        for ( UUID id : esm.loadAllKeys() ) {
        //            pesm.store( id, esm.load( id ) );
        //        }
        return pesm;
    }

    @Bean
    public SelfRegisteringMapStore<String, DelegatedStringSet> schemaMapstore() {
        return new SchemasMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<String, UUID> aclKeysMapstore() {
        AclKeysMapstore pakm = new AclKeysMapstore( hikariDataSource );

        //        com.dataloom.edm.mapstores.AclKeysMapstore akm = new com.dataloom.edm.mapstores.AclKeysMapstore( session );
        //        for ( String name : akm.loadAllKeys() ) {
        //            pakm.store( name, akm.load( name ) );
        //        }
        return pakm;
    }

    @Bean
    public SelfRegisteringMapStore<String, UUID> edmVersionMapstore() {
        return new EdmVersionsMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> namesMapstore() {
        NamesMapstore pnm = new NamesMapstore( hikariDataSource );

        //        com.dataloom.edm.mapstores.NamesMapstore nm = new com.dataloom.edm.mapstores.NamesMapstore( session );
        //        for ( UUID key : nm.loadAllKeys() ) {
        //            pnm.store( key, nm.load( key ) );
        //        }
        return pnm;
    }

    @Bean
    public SelfRegisteringMapStore<AceKey, Status> requestMapstore() {
        return new RequestsMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> orgTitlesMapstore() {
        OrganizationTitlesMapstore potm = new OrganizationTitlesMapstore( hikariDataSource );

        //        StringMapstore otm = new StringMapstore(
        //                HazelcastMap.ORGANIZATIONS_TITLES,
        //                session,
        //                Table.ORGANIZATIONS,
        //                CommonColumns.ID,
        //                CommonColumns.TITLE );
        //
        //        for ( UUID id : otm.loadAllKeys() ) {
        //            potm.store( id, otm.load( id ) );
        //        }
        return potm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> orgDescsMapstore() {
        OrganizationDescriptionsMapstore podm = new OrganizationDescriptionsMapstore( hikariDataSource );

        //        StringMapstore odm = new StringMapstore(
        //                HazelcastMap.ORGANIZATIONS_DESCRIPTIONS,
        //                session,
        //                Table.ORGANIZATIONS,
        //                CommonColumns.ID,
        //                CommonColumns.DESCRIPTION );
        //        for ( UUID id : odm.loadAllKeys() ) {
        //            podm.store( id, odm.load( id ) );
        //        }
        return podm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedStringSet> aaEmailDomainsMapstore() {
        OrganizationEmailDomainsMapstore pedm = new OrganizationEmailDomainsMapstore( hikariDataSource );
        return pedm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, PrincipalSet> membersMapstore() {
        OrganizationMembersMapstore pmm = new OrganizationMembersMapstore( hikariDataSource );
        return pmm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedUUIDSet> orgAppsMapstore() {
        return new OrganizationAppsMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedUUIDSet> linkedEntityTypesMapstore() {
        return new LinkedEntityTypesMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedUUIDSet> linkedEntitySetsMapstore() {
        com.openlattice.postgres.mapstores.LinkedEntitySetsMapstore plesm =
                new com.openlattice.postgres.mapstores.LinkedEntitySetsMapstore( hikariDataSource );
        return plesm;
    }

    @Bean
    public SelfRegisteringMapStore<LinkingVertexKey, WeightedLinkingVertexKeySet> linkingEdgesMapstore() {
        return new LinkingEdgesMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<LinkingVertexKey, LinkingVertex> linkingVerticesMapstore() {
        LinkingVerticesMapstore plvm = new LinkingVerticesMapstore( hikariDataSource );
        return plvm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, AssociationType> edgeTypeMapstore() {
        AssociationTypeMapstore patm = new AssociationTypeMapstore( hikariDataSource );
        return patm;
    }

    @Bean
    public SelfRegisteringMapStore<AclKey, SecurablePrincipal> principalsMapstore() {
        return new PrincipalMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, UUID> syncIdsMapstore() {
        SyncIdsMapstore psim = new SyncIdsMapstore( hikariDataSource );

        com.dataloom.data.mapstores.SyncIdsMapstore sim = new com.dataloom.data.mapstores.SyncIdsMapstore( session );
        for ( UUID key : sim.loadAllKeys() ) {
            psim.store( key, sim.load( key ) );
        }
        return psim;
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
        return new VertexIdsAfterLinkingMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<AclKey, AclKeySet> aclKeySetMapstore() {
        return new PrincipalTreeMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<String, String> dbCredentialsMapstore() {
        return new PostgresCredentialMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<DataKey, ByteBuffer> dataMapstore() throws SQLException {
        return new PostgresDataMapstore( HazelcastMap.DATA.name(), hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<String, Auth0UserBasic> userMapstore() {
        return new UserMapstore();
    }

    @Bean
    public SelfRegisteringMapStore<EntitySetPropertyKey, EntitySetPropertyMetadata> entitySetPropertyMetadataMapstore() {
        EntitySetPropertyMetadataMapstore pespm = new EntitySetPropertyMetadataMapstore( hikariDataSource );
        return pespm;
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
    public SelfRegisteringMapStore<AppConfigKey, AppTypeSetting> appConfigMapstore() {
        return new AppConfigMapstore( hikariDataSource );
    }

}
