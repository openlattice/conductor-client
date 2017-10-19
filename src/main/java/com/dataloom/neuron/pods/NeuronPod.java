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

package com.dataloom.neuron.pods;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizationQueryService;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.HazelcastAuthorizationService;
import com.dataloom.data.DataGraphManager;
import com.dataloom.data.DataGraphService;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.EntityKeyIdService;
import com.dataloom.data.ids.HazelcastEntityKeyIdService;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.edm.properties.PostgresTypeManager;
import com.dataloom.edm.schemas.SchemaQueryService;
import com.dataloom.edm.schemas.cassandra.CassandraSchemaQueryService;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.dataloom.edm.schemas.postgres.PostgresSchemaQueryService;
import com.dataloom.graph.core.GraphQueryService;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.linking.CassandraLinkingGraphsQueryService;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.neuron.Neuron;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.services.PostgresEntitySetManager;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.inject.Inject;

@Configuration
@Import( {
        AuditEntitySetPod.class,
        CassandraPod.class
} )
public class NeuronPod {

    @Inject
    private CassandraConfiguration cassandraConfiguration;

    @Inject
    private EventBus eventBus;

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private Session session;

    @Inject
    private HikariDataSource hikariDataSource;

    /*
     *
     * Neuron bean
     *
     */

    @Bean
    public Neuron neuron() {
        return new Neuron(
                dataGraphService(),
                idService(),
                cassandraConfiguration,
                session
        );
    }

    /*
     *
     * other dependency beans
     *
     */

    @Bean
    public AuthorizationQueryService authorizationQueryService() {
        return new AuthorizationQueryService( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public AuthorizationManager authorizationManager() {
        return new HazelcastAuthorizationService( hazelcastInstance, authorizationQueryService(), eventBus );
    }

    @Bean
    public CassandraEntityDatastore cassandraDataManager() {
        return new CassandraEntityDatastore(
                session,
                hazelcastInstance,
                executor,
                defaultObjectMapper(),
                idService(),
                linkingGraph(),
                loomGraph(),
                dataSourceManager()
        );
    }

    @Bean
    public PostgresEntitySetManager entitySetManager() {
        return new PostgresEntitySetManager( hikariDataSource );
    }

    @Bean
    public CassandraLinkingGraphsQueryService linkingGraphQueryService() {
        return new CassandraLinkingGraphsQueryService( cassandraConfiguration.getKeyspace(), session );
    }

    @Bean
    public PostgresTypeManager entityTypeManager() {
        return new PostgresTypeManager( hikariDataSource );
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService(
                hazelcastInstance,
                cassandraDataManager(),
                loomGraph(),
                idService(),
                executor,
                eventBus
        );
    }

    @Bean
    public DatasourceManager dataSourceManager() {
        return new DatasourceManager( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService(
                cassandraConfiguration.getKeyspace(),
                session,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                entitySetManager(),
                entityTypeManager(),
                schemaManager() );
    }

    @Bean
    public EntityKeyIdService idService() {
        return new HazelcastEntityKeyIdService( hazelcastInstance, executor );
    }

    @Bean
    public GraphQueryService graphQueryService() {
        return new GraphQueryService( session );
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public HazelcastLinkingGraphs linkingGraph() {
        return new HazelcastLinkingGraphs( hazelcastInstance );
    }

    @Bean
    public HazelcastSchemaManager schemaManager() {
        return new HazelcastSchemaManager(
                cassandraConfiguration.getKeyspace(),
                hazelcastInstance,
                schemaQueryService() );
    }

    @Bean
    public LoomGraph loomGraph() {
        return new LoomGraph( executor, hazelcastInstance );
    }

    @Bean
    public ObjectMapper defaultObjectMapper() {
        return ObjectMappers.getJsonMapper();
    }

    @Bean
    public SchemaQueryService schemaQueryService() {
        return new PostgresSchemaQueryService( hikariDataSource );
    }
}
