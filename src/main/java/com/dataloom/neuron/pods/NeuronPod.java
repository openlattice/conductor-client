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

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.data.DataGraphManager;
import com.dataloom.data.DataGraphService;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.EntityKeyIdService;
import com.dataloom.data.ids.HazelcastEntityKeyIdService;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.graph.core.GraphQueryService;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.neuron.Neuron;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.pods.CassandraPod;

@Configuration
@Import( {
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

    @Bean
    public Neuron neuron() {
        return new Neuron(
                dataGraphService(),
                idService(),
                cassandraConfiguration,
                session
        );
    }

    @Bean
    public CassandraEntityDatastore cassandraDataManager() {
        return new CassandraEntityDatastore(
                session,
                defaultObjectMapper(),
                linkingGraph(),
                loomGraph(),
                datasourceManager() );
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService(
                hazelcastInstance,
                cassandraDataManager(),
                loomGraph(),
                idService(),
                executor,
                eventBus );
    }

    @Bean
    public DatasourceManager datasourceManager() {
        return new DatasourceManager( session, hazelcastInstance );
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
    public HazelcastLinkingGraphs linkingGraph() {
        return new HazelcastLinkingGraphs( hazelcastInstance );
    }

    @Bean
    public LoomGraph loomGraph() {
        return new LoomGraph( graphQueryService(), hazelcastInstance );
    }

    @Bean
    public ObjectMapper defaultObjectMapper() {
        return ObjectMappers.getJsonMapper();
    }
}
