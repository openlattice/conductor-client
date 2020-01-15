/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.search;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;
import com.dataloom.streams.StreamUtil;
import com.google.common.base.Stopwatch;
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.openlattice.IdConstants;
import com.openlattice.apps.App;
import com.openlattice.apps.AppType;
import com.openlattice.authorization.*;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.collections.EntitySetCollection;
import com.openlattice.collections.EntityTypeCollection;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DeleteType;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.events.EntitiesDeletedEvent;
import com.openlattice.data.events.EntitiesUpsertedEvent;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.data.requests.NeighborEntityIds;
import com.openlattice.data.storage.EntityDatastore;
import com.openlattice.data.storage.IndexingMetadataManager;
import com.openlattice.data.storage.MetadataOption;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.edm.EdmConstants;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.events.*;
import com.openlattice.edm.set.EntitySetFlag;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.core.GraphService;
import com.openlattice.graph.edge.Edge;
import com.openlattice.ids.IdCipherManager;
import com.openlattice.organizations.Organization;
import com.openlattice.organizations.events.OrganizationCreatedEvent;
import com.openlattice.organizations.events.OrganizationDeletedEvent;
import com.openlattice.organizations.events.OrganizationUpdatedEvent;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.openlattice.search.requests.*;
import kotlin.Pair;
import org.apache.commons.collections4.MapUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.openlattice.authorization.EdmAuthorizationHelper.READ_PERMISSION;

public class SearchService {
    private static final Logger logger = LoggerFactory.getLogger( SearchService.class );

    @Inject
    private EventBus eventBus;

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private ConductorElasticsearchApi elasticsearchApi;

    @Inject
    private EdmManager dataModelService;

    @Inject
    private EntitySetManager entitySetService;

    @Inject
    private GraphService graphService;

    @Inject
    private EntityDatastore dataManager;

    @Inject
    private DataGraphManager dgm;

    @Inject
    private IndexingMetadataManager indexingMetadataManager;

    @Inject
    private IdCipherManager idCipher;

    private Timer indexEntitiesTimer;

    private Timer deleteEntitiesTimer;

    private Timer deleteEntitySetDataTimer;

    private Timer markAsIndexedTimer;

    public SearchService( EventBus eventBus, MetricRegistry metricRegistry ) {
        eventBus.register( this );
        indexEntitiesTimer = metricRegistry.timer(
                MetricRegistry.name( SearchService.class, "indexEntities" )
        );
        deleteEntitiesTimer = metricRegistry.timer(
                MetricRegistry.name( SearchService.class, "deleteEntities" )
        );
        deleteEntitySetDataTimer = metricRegistry.timer(
                MetricRegistry.name( SearchService.class, "entitySetDataCleared" )
        );
        markAsIndexedTimer = metricRegistry.timer(
                MetricRegistry.name( SearchService.class, "markAsIndexed" )
        );
    }

    @Timed
    public SearchResult executeEntitySetKeywordSearchQuery(
            Optional<String> optionalQuery,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            int start,
            int maxHits ) {

        Set<AclKey> authorizedEntitySetIds = authorizations
                .getAuthorizedObjectsOfType( Principals.getCurrentPrincipals(),
                        SecurableObjectType.EntitySet,
                        READ_PERMISSION ).collect( Collectors.toSet() );
        if ( authorizedEntitySetIds.size() == 0 ) {
            return new SearchResult( 0, Lists.newArrayList() );
        }

        return elasticsearchApi.executeEntitySetMetadataSearch(
                optionalQuery,
                optionalEntityType,
                optionalPropertyTypes,
                authorizedEntitySetIds,
                start,
                maxHits );
    }

    @Timed
    public SearchResult executeEntitySetCollectionQuery(
            String searchTerm,
            int start,
            int maxHits ) {

        Set<AclKey> authorizedEntitySetCollectionIds = authorizations
                .getAuthorizedObjectsOfType( Principals.getCurrentPrincipals(),
                        SecurableObjectType.EntitySetCollection,
                        READ_PERMISSION ).collect( Collectors.toSet() );
        if ( authorizedEntitySetCollectionIds.size() == 0 ) {
            return new SearchResult( 0, Lists.newArrayList() );
        }

        return elasticsearchApi.executeEntitySetCollectionSearch(
                searchTerm,
                authorizedEntitySetCollectionIds,
                start,
                maxHits );
    }

    /**
     * Executes a search on the requested entity sets.
     *
     * @param searchConstraints                  The constraints to apply on the search (e.g. maximum number of hits,
     *                                           sorting...)
     * @param authorizedPropertyTypesByEntitySet The authorized property types mapped by the requested entity set ids.
     * @return A {@link DataSearchResult} containing a list of entity data hits along with the number of possible
     * total hits.
     */
    @Timed
    public DataSearchResult executeSearch(
            SearchConstraints searchConstraints,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet ) {

        final Set<UUID> entitySetIds = Sets.newHashSet( Arrays.asList( searchConstraints.getEntitySetIds() ) );
        final Map<UUID, EntitySet> entitySetsById = entitySetService.getEntitySetsAsMap( entitySetIds );
        final var linkingEntitySets = entitySetsById.values().stream()
                .filter( EntitySet::isLinking )
                .collect( Collectors.toMap(
                        EntitySet::getId,
                        linkingEntitySet -> DelegatedUUIDSet.wrap( linkingEntitySet.getLinkedEntitySets() ) ) );

        final Map<UUID, DelegatedUUIDSet> authorizedPropertiesByEntitySet = authorizedPropertyTypesByEntitySet
                .entrySet().stream().collect( Collectors.toMap( Map.Entry::getKey,
                        entry -> DelegatedUUIDSet.wrap( entry.getValue().keySet() ) ) );

        if ( authorizedPropertiesByEntitySet.isEmpty() ) {
            return new DataSearchResult( 0, Lists.newArrayList() );
        }

        Map<UUID, UUID> entityTypesByEntitySet = entitySetService
                .getEntitySetsAsMap( Sets.newHashSet( Arrays.asList( searchConstraints.getEntitySetIds() ) ) ).values()
                .stream()
                .collect( Collectors.toMap( EntitySet::getId, EntitySet::getEntityTypeId ) );

        EntityDataKeySearchResult result = elasticsearchApi.executeSearch(
                searchConstraints,
                entityTypesByEntitySet,
                authorizedPropertiesByEntitySet,
                linkingEntitySets );

        Map<UUID, Set<UUID>> entityKeyIdsByEntitySetId = Maps.newHashMap();
        result.getEntityDataKeys()
                .forEach( edk -> {
                    entityKeyIdsByEntitySetId.putIfAbsent( edk.getEntitySetId(), Sets.newHashSet() );
                    entityKeyIdsByEntitySetId.get( edk.getEntitySetId() ).add( edk.getEntityKeyId() );
                } );

        Map<UUID, Map<FullQualifiedName, Set<Object>>> entitiesById = entityKeyIdsByEntitySetId.keySet()
                .parallelStream()
                .map( entitySetId -> getResults(
                        entitySetsById.get( entitySetId ),
                        entityKeyIdsByEntitySetId.get( entitySetId ),
                        authorizedPropertyTypesByEntitySet,
                        entitySetsById.get( entitySetId ).isLinking() ) )
                .flatMap( Collection::stream )
                .collect( Collectors.toMap( SearchService::getEntityKeyId, Function.identity() ) );

        List<Map<FullQualifiedName, Set<Object>>> results = result.getEntityDataKeys().stream()
                .map( edk -> entitiesById.get( edk.getEntityKeyId() ) ).filter( Objects::nonNull )
                .collect( Collectors.toList() );

        return new DataSearchResult( result.getNumHits(), results );
    }

    @Timed
    @Subscribe
    public void createEntitySet( EntitySetCreatedEvent event ) {
        EntityType entityType = dataModelService.getEntityType( event.getEntitySet().getEntityTypeId() );
        elasticsearchApi.saveEntitySetToElasticsearch( entityType, event.getEntitySet(), event.getPropertyTypes() );
    }

    @Timed
    @Subscribe
    public void deleteEntitySet( EntitySetDeletedEvent event ) {
        elasticsearchApi.deleteEntitySet( event.getEntitySetId(), event.getEntityTypeId() );
    }

    @Subscribe
    public void deleteEntities( EntitiesDeletedEvent event ) {
        final var deleteEntitiesContext = deleteEntitiesTimer.time();
        UUID entityTypeId = entitySetService.getEntityTypeByEntitySetId( event.getEntitySetId() ).getId();
        final var entitiesDeleted = elasticsearchApi.deleteEntityDataBulk( entityTypeId, event.getEntityKeyIds() );
        deleteEntitiesContext.stop();

        if ( entitiesDeleted ) {
            // mark them as (un)indexed:
            // - set last_index to now() when it's a hard delete (it does not matter when we
            // get last_write, because the only action take here is to remove the documents)
            // - let background task take soft deletes
            if ( event.getDeleteType() == DeleteType.Hard ) {
                final var markAsIndexedContext = markAsIndexedTimer.time();
                indexingMetadataManager.markAsUnIndexed( Map.of( event.getEntitySetId(), event.getEntityKeyIds() ) );
                markAsIndexedContext.stop();
            }
        }
    }

    @Subscribe
    public void entitySetDataCleared( EntitySetDataDeletedEvent event ) {
        final var deleteEntitySetDataContext = deleteEntitySetDataTimer.time();
        UUID entityTypeId = entitySetService.getEntityTypeByEntitySetId( event.getEntitySetId() ).getId();
        final var entitySetDataDeleted = elasticsearchApi.clearEntitySetData( event.getEntitySetId(), entityTypeId );
        deleteEntitySetDataContext.stop();

        if ( entitySetDataDeleted ) {
            // mark them as (un)indexed:
            // - set last_index to now() when it's a hard delete (it does not matter when we
            // get last_write, because the only action take here is to remove the documents)
            // - let background task take soft deletes (would be too much overhead for clear calls)
            if ( event.getDeleteType() == DeleteType.Hard ) {
                final var markAsIndexedContext = markAsIndexedTimer.time();
                indexingMetadataManager.markAsUnIndexed( event.getEntitySetId() );
                markAsIndexedContext.stop();
            }
        }
    }

    @Timed
    @Subscribe
    public void createOrganization( OrganizationCreatedEvent event ) {
        elasticsearchApi.createOrganization( event.getOrganization() );
    }

    @Timed
    public SearchResult executeOrganizationKeywordSearch( SearchTerm searchTerm ) {
        Set<AclKey> authorizedOrganizationIds = authorizations
                .getAuthorizedObjectsOfType( Principals.getCurrentPrincipals(),
                        SecurableObjectType.Organization,
                        READ_PERMISSION ).collect( Collectors.toSet() );
        if ( authorizedOrganizationIds.size() == 0 ) {
            return new SearchResult( 0, Lists.newArrayList() );
        }

        return elasticsearchApi.executeOrganizationSearch( searchTerm.getSearchTerm(),
                authorizedOrganizationIds,
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @Timed
    @Subscribe
    public void updateOrganization( OrganizationUpdatedEvent event ) {
        elasticsearchApi.updateOrganization( event.getId(),
                event.getOptionalTitle(),
                event.getOptionalDescription() );
    }

    @Subscribe
    public void deleteOrganization( OrganizationDeletedEvent event ) {
        elasticsearchApi
                .deleteSecurableObjectFromElasticsearch( SecurableObjectType.Organization, event.getOrganizationId() );
    }

    /**
     * Handles when entities are created or updated.
     * In both cases it is enough to re-index the document, ElasticSearch will mark the old document as deleted.
     */
    @Subscribe
    public void indexEntities( EntitiesUpsertedEvent event ) {
        final var indexEntitiesContext = indexEntitiesTimer.time();
        UUID entityTypeId = entitySetService.getEntityTypeByEntitySetId( event.getEntitySetId() ).getId();
        final var entitiesIndexed = elasticsearchApi
                .createBulkEntityData( entityTypeId, event.getEntitySetId(), event.getEntities() );
        indexEntitiesContext.stop();

        if ( entitiesIndexed ) {
            final var markAsIndexedContext = markAsIndexedTimer.time();
            // mark them as indexed
            indexingMetadataManager.markAsIndexed(
                    Map.of(
                            event.getEntitySetId(),
                            event.getEntities().entrySet().stream().collect( Collectors.toMap(
                                    Map.Entry::getKey,
                                    entity ->
                                            ( OffsetDateTime ) entity.getValue()
                                                    .get( IdConstants.LAST_WRITE_ID.getId() ).iterator().next()
                            ) )
                    )
            );
            markAsIndexedContext.stop();
        }
    }

    @Subscribe
    public void updateEntitySetMetadata( EntitySetMetadataUpdatedEvent event ) {
        elasticsearchApi.updateEntitySetMetadata( event.getEntitySet() );
    }

    @Subscribe
    public void updatePropertyTypesInEntitySet( PropertyTypesInEntitySetUpdatedEvent event ) {
        elasticsearchApi.updatePropertyTypesInEntitySet( event.getEntitySetId(), event.getUpdatedPropertyTypes() );
    }

    /**
     * If 1 or more property types are added to an entity type, the corresponding mapping needs to be updated
     */
    @Subscribe
    public void addPropertyTypesToEntityType( PropertyTypesAddedToEntityTypeEvent event ) {
        elasticsearchApi.addPropertyTypesToEntityType( event.getEntityType(), event.getNewPropertyTypes() );
    }

    @Subscribe
    public void createEntityType( EntityTypeCreatedEvent event ) {
        EntityType entityType = event.getEntityType();
        List<PropertyType> propertyTypes = Lists
                .newArrayList( dataModelService.getPropertyTypes( entityType.getProperties() ) );
        elasticsearchApi.saveEntityTypeToElasticsearch( entityType, propertyTypes );
    }

    @Subscribe
    public void createAssociationType( AssociationTypeCreatedEvent event ) {
        AssociationType associationType = event.getAssociationType();
        List<PropertyType> propertyTypes = Lists
                .newArrayList( dataModelService
                        .getPropertyTypes( associationType.getAssociationEntityType().getProperties() ) );
        elasticsearchApi.saveAssociationTypeToElasticsearch( associationType, propertyTypes );
    }

    @Subscribe
    public void createPropertyType( PropertyTypeCreatedEvent event ) {
        PropertyType propertyType = event.getPropertyType();
        elasticsearchApi
                .saveSecurableObjectToElasticsearch( SecurableObjectType.PropertyTypeInEntitySet, propertyType );
    }

    @Subscribe
    public void createApp( AppCreatedEvent event ) {
        App app = event.getApp();
        elasticsearchApi.saveSecurableObjectToElasticsearch( SecurableObjectType.App, app );
    }

    @Subscribe
    public void createAppType( AppTypeCreatedEvent event ) {
        AppType appType = event.getAppType();
        elasticsearchApi.saveSecurableObjectToElasticsearch( SecurableObjectType.AppType, appType );
    }

    @Subscribe
    public void deleteEntityType( EntityTypeDeletedEvent event ) {
        UUID entityTypeId = event.getEntityTypeId();
        elasticsearchApi.deleteSecurableObjectFromElasticsearch( SecurableObjectType.EntityType, entityTypeId );
    }

    @Subscribe
    public void deleteAssociationType( AssociationTypeDeletedEvent event ) {
        UUID associationTypeId = event.getAssociationTypeId();
        elasticsearchApi
                .deleteSecurableObjectFromElasticsearch( SecurableObjectType.AssociationType, associationTypeId );
    }

    @Subscribe
    public void createEntityTypeCollection( EntityTypeCollectionCreatedEvent event ) {
        EntityTypeCollection entityTypeCollection = event.getEntityTypeCollection();
        elasticsearchApi
                .saveSecurableObjectToElasticsearch( SecurableObjectType.EntityTypeCollection, entityTypeCollection );
    }

    @Subscribe
    public void deleteEntityTypeCollection( EntityTypeCollectionDeletedEvent event ) {
        UUID entityTypeCollectionId = event.getEntityTypeCollectionId();
        elasticsearchApi
                .deleteSecurableObjectFromElasticsearch( SecurableObjectType.EntityTypeCollection,
                        entityTypeCollectionId );
    }

    @Subscribe
    public void createEntitySetCollection( EntitySetCollectionCreatedEvent event ) {
        EntitySetCollection entitySetCollection = event.getEntitySetCollection();
        elasticsearchApi
                .saveSecurableObjectToElasticsearch( SecurableObjectType.EntitySetCollection, entitySetCollection );
    }

    @Subscribe
    public void deleteEntitySetCollection( EntitySetCollectionDeletedEvent event ) {
        UUID entitySetCollectionId = event.getEntitySetCollectionId();
        elasticsearchApi
                .deleteSecurableObjectFromElasticsearch( SecurableObjectType.EntitySetCollection,
                        entitySetCollectionId );
    }

    /**
     * Handle deleting the index for that property type.
     * At this point, none of the entity sets should contain this property type anymore, so the entity set data mappings
     * are not affected.
     */
    @Subscribe
    public void deletePropertyType( PropertyTypeDeletedEvent event ) {
        UUID propertyTypeId = event.getPropertyTypeId();
        elasticsearchApi
                .deleteSecurableObjectFromElasticsearch( SecurableObjectType.PropertyTypeInEntitySet, propertyTypeId );
    }

    @Subscribe
    public void deleteApp( AppDeletedEvent event ) {
        UUID appId = event.getAppId();
        elasticsearchApi.deleteSecurableObjectFromElasticsearch( SecurableObjectType.App, appId );
    }

    @Subscribe
    public void deleteAppType( AppTypeDeletedEvent event ) {
        UUID appTypeId = event.getAppTypeId();
        elasticsearchApi.deleteSecurableObjectFromElasticsearch( SecurableObjectType.AppType, appTypeId );
    }

    public SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi
                .executeSecurableObjectSearch( SecurableObjectType.EntityType, searchTerm, start, maxHits );
    }

    public SearchResult executeAssociationTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi
                .executeSecurableObjectSearch( SecurableObjectType.AssociationType, searchTerm, start, maxHits );
    }

    public SearchResult executePropertyTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeSecurableObjectSearch( SecurableObjectType.PropertyTypeInEntitySet,
                searchTerm,
                start,
                maxHits );
    }

    public SearchResult executeAppSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeSecurableObjectSearch( SecurableObjectType.App, searchTerm, start, maxHits );
    }

    public SearchResult executeAppTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeSecurableObjectSearch( SecurableObjectType.AppType, searchTerm, start, maxHits );
    }

    public SearchResult executeEntityTypeCollectionSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeSecurableObjectSearch( SecurableObjectType.EntityTypeCollection,
                searchTerm,
                start,
                maxHits );
    }

    public SearchResult executeFQNEntityTypeSearch( String namespace, String name, int start, int maxHits ) {
        return elasticsearchApi
                .executeSecurableObjectFQNSearch( SecurableObjectType.EntityType, namespace, name, start, maxHits );
    }

    public SearchResult executeFQNPropertyTypeSearch( String namespace, String name, int start, int maxHits ) {
        return elasticsearchApi.executeSecurableObjectFQNSearch( SecurableObjectType.PropertyTypeInEntitySet,
                namespace,
                name,
                start,
                maxHits );
    }

    @Timed
    public Map<UUID, List<NeighborEntityDetails>> executeEntityNeighborSearch(
            EntityNeighborsFilter filter,
            Set<Principal> principals
    ) {
        // todo decrypt/encrypt
        final Stopwatch sw1 = Stopwatch.createStarted();
        final Stopwatch sw2 = Stopwatch.createStarted();

        logger.info( "Starting Entity Neighbor Search..." );
        if ( checkAssociationFilterMissing( filter ) ) {
            logger.info( "Missing association entity set ids.. returning empty result" );
            return ImmutableMap.of();
        }

        var linkingEntitySets = entitySetService
                .getEntitySetsWithFlags( filter.getEntityKeyIds().keySet(), EnumSet.of( EntitySetFlag.LINKING ) );

        var groupedEntityKeyIds = filter.getEntityKeyIds()
                .entrySet().stream()
                .collect( Collectors.groupingBy( entry -> linkingEntitySets.keySet().contains( entry.getKey() ) ) );

        Map<UUID, List<NeighborEntityDetails>> entityNeighbors = Maps.newConcurrentMap();

        if ( linkingEntitySets.size() > 0 ) {
            entityNeighbors = executeLinkingEntityNeighborSearch(
                    linkingEntitySets,
                    new EntityNeighborsFilter(
                            groupedEntityKeyIds.get( true ).stream()
                                    .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) ),
                            filter.getSrcEntitySetIds(),
                            filter.getDstEntitySetIds(),
                            filter.getAssociationEntitySetIds() ),
                    principals
            );
        }

        List<Edge> edges = Lists.newArrayList();
        Set<UUID> allEntitySetIds = Sets.newHashSet();
        Map<UUID, Set<UUID>> entitySetIdToEntityKeyId = Maps.newHashMap();
        var normalEntityKeyIds = groupedEntityKeyIds.get( false ).stream()
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
        var entityKeyIds = normalEntityKeyIds.values().stream().flatMap( Set::stream ).collect( Collectors.toSet() );

        graphService.getEdgesAndNeighborsForVerticesBulk(
                new EntityNeighborsFilter(
                        normalEntityKeyIds,
                        filter.getSrcEntitySetIds(),
                        filter.getDstEntitySetIds(),
                        filter.getAssociationEntitySetIds() ) )
                .forEach( edge -> {
                    edges.add( edge );
                    allEntitySetIds.add( edge.getEdge().getEntitySetId() );
                    allEntitySetIds.add( entityKeyIds.contains( edge.getSrc().getEntityKeyId() ) ?
                            edge.getDst().getEntitySetId() : edge.getSrc().getEntitySetId() );
                } );
        logger.info( "Get edges and neighbors for vertices query for {} ids finished in {} ms",
                filter.getEntityKeyIds().size(),
                sw1.elapsed( TimeUnit.MILLISECONDS ) );
        sw1.reset().start();

        Set<UUID> authorizedEntitySetIds = authorizations.accessChecksForPrincipals( allEntitySetIds.stream()
                .map( esId -> new AccessCheck( new AclKey( esId ), READ_PERMISSION ) )
                .collect( Collectors.toSet() ), principals )
                .filter( auth -> auth.getPermissions().get( Permission.READ ) ).map( auth -> auth.getAclKey().get( 0 ) )
                .collect( Collectors.toSet() );

        Map<UUID, EntitySet> entitySetsById = entitySetService.getEntitySetsAsMap( authorizedEntitySetIds );

        Map<UUID, Map<UUID, PropertyType>> entitySetsIdsToAuthorizedProps =
                Maps.newHashMapWithExpectedSize( entitySetsById.size() );
        Map<UUID, Set<UUID>> authorizedEdgeESIdsToVertexESIds =
                Maps.newHashMapWithExpectedSize( entitySetsById.size() );
        Map<UUID, EntityType> entityTypesById = dataModelService
                .getEntityTypesAsMap( entitySetsById.values().stream().map( entitySet -> {
                    entitySetsIdsToAuthorizedProps.put( entitySet.getId(), Maps.newHashMap() );
                    authorizedEdgeESIdsToVertexESIds.put( entitySet.getId(), Sets.newHashSet() );
                    return entitySet.getEntityTypeId();
                } ).collect( Collectors.toSet() ) );

        Map<UUID, PropertyType> propertyTypesById = dataModelService
                .getPropertyTypesAsMap( entityTypesById.values().stream()
                        .flatMap( entityType -> entityType.getProperties().stream() ).collect(
                                Collectors.toSet() ) );

        Set<AccessCheck> accessChecks = entitySetsById.values().stream()
                .flatMap( entitySet -> entityTypesById.get( entitySet.getEntityTypeId() ).getProperties().stream()
                        .map( propertyTypeId -> new AccessCheck( new AclKey( entitySet.getId(), propertyTypeId ),
                                READ_PERMISSION ) ) ).collect( Collectors.toSet() );

        authorizations.accessChecksForPrincipals( accessChecks, principals ).forEach( auth -> {
            if ( auth.getPermissions().get( Permission.READ ) ) {
                UUID esId = auth.getAclKey().get( 0 );
                UUID propertyTypeId = auth.getAclKey().get( 1 );
                entitySetsIdsToAuthorizedProps.get( esId )
                        .put( propertyTypeId, propertyTypesById.get( propertyTypeId ) );
            }
        } );
        logger.info( "Access checks for entity sets and their properties finished in {} ms",
                sw1.elapsed( TimeUnit.MILLISECONDS ) );
        sw1.reset().start();

        edges.forEach( edge -> {
            UUID edgeEntityKeyId = edge.getEdge().getEntityKeyId();
            UUID neighborEntityKeyId = ( entityKeyIds.contains( edge.getSrc().getEntityKeyId() ) ) ? edge.getDst()
                    .getEntityKeyId()
                    : edge.getSrc().getEntityKeyId();
            UUID edgeEntitySetId = edge.getEdge().getEntitySetId();
            UUID neighborEntitySetId = ( entityKeyIds.contains( edge.getSrc().getEntityKeyId() ) ) ? edge.getDst()
                    .getEntitySetId()
                    : edge.getSrc().getEntitySetId();

            if ( entitySetsIdsToAuthorizedProps.containsKey( edgeEntitySetId ) ) {
                entitySetIdToEntityKeyId.putIfAbsent( edgeEntitySetId, Sets.newHashSet() );
                entitySetIdToEntityKeyId.get( edgeEntitySetId ).add( edgeEntityKeyId );

                if ( entitySetsIdsToAuthorizedProps.containsKey( neighborEntitySetId ) ) {
                    authorizedEdgeESIdsToVertexESIds.get( edgeEntitySetId ).add( neighborEntitySetId );
                    entitySetIdToEntityKeyId.putIfAbsent( neighborEntitySetId, Sets.newHashSet() );
                    entitySetIdToEntityKeyId.get( neighborEntitySetId ).add( neighborEntityKeyId );
                }
            }

        } );
        logger.info( "Edge and neighbor entity key ids collected in {} ms", sw1.elapsed( TimeUnit.MILLISECONDS ) );
        sw1.reset().start();

        Collection<Map<FullQualifiedName, Set<Object>>> entitiesAcrossEntitySetIds = dataManager
                .getEntitiesAcrossEntitySets( entitySetIdToEntityKeyId, entitySetsIdsToAuthorizedProps );
        logger.info( "Get entities across entity sets query finished in {} ms", sw1.elapsed( TimeUnit.MILLISECONDS ) );
        sw1.reset().start();

        Map<UUID, Map<FullQualifiedName, Set<Object>>> entities = entitiesAcrossEntitySetIds.stream()
                .collect( Collectors.toMap( SearchService::getEntityKeyId, Function.identity() ) );

        // create a NeighborEntityDetails object for each edge based on authorizations
        edges.parallelStream().forEach( edge -> {
            boolean vertexIsSrc = entityKeyIds.contains( edge.getKey().getSrc().getEntityKeyId() );
            UUID entityId = ( vertexIsSrc )
                    ? edge.getKey().getSrc().getEntityKeyId()
                    : edge.getKey().getDst().getEntityKeyId();
            entityNeighbors.putIfAbsent( entityId, Collections.synchronizedList( Lists.newArrayList() ) );

            NeighborEntityDetails neighbor = getNeighborEntityDetails( edge,
                    authorizedEdgeESIdsToVertexESIds,
                    entitySetsById,
                    vertexIsSrc,
                    entities );
            if ( neighbor != null ) {
                entityNeighbors.get( entityId ).add( neighbor );
            }
        } );
        logger.info( "Neighbor entity details collected in {} ms", sw1.elapsed( TimeUnit.MILLISECONDS ) );

        logger.info( "Finished entity neighbor search in {} ms", sw2.elapsed( TimeUnit.MILLISECONDS ) );
        return entityNeighbors;
    }

    @Timed
    private Map<UUID, List<NeighborEntityDetails>> executeLinkingEntityNeighborSearch(
            Map<UUID, EntitySet> linkedEntitySets,
            EntityNeighborsFilter filter,
            Set<Principal> principals
    ) {
        final Stopwatch sw1 = Stopwatch.createStarted();
        final Stopwatch sw2 = Stopwatch.createStarted();

        Map<UUID, List<NeighborEntityDetails>> linkingEntityNeighbors = Maps.newConcurrentMap();

        filter.getEntityKeyIds().forEach( ( linkedEntitySetId, linkingIds ) -> {
            logger.info( "Starting search for linked entity set {}.", linkedEntitySetId );

            var normalEntitySetIds = linkedEntitySets.get( linkedEntitySetId ).getLinkedEntitySets();
            var entityKeyIdsByLinkingId =
                    getEntityKeyIdsOfLinkingIds( linkingIds, linkedEntitySets.get( linkedEntitySetId ) );
            var entityKeyIds = entityKeyIdsByLinkingId.values().stream()
                    .flatMap( Set::stream ).collect( Collectors.toSet() );

            List<Edge> edges = Lists.newArrayList();
            Set<UUID> allEntitySetIds = Sets.newHashSet();

            graphService.getEdgesAndNeighborsForVerticesBulk(
                    new EntityNeighborsFilter(
                            normalEntitySetIds.stream()
                                    .collect( Collectors.toMap( Function.identity(), entry -> entityKeyIds ) ),
                            filter.getSrcEntitySetIds(),
                            filter.getDstEntitySetIds(),
                            filter.getAssociationEntitySetIds() ) )
                    .forEach( edge -> {
                        edges.add( edge );
                        allEntitySetIds.add( edge.getEdge().getEntitySetId() );
                        allEntitySetIds.add( entityKeyIds.contains( edge.getSrc().getEntityKeyId() ) ?
                                edge.getDst().getEntitySetId() : edge.getSrc().getEntitySetId() );
                    } );

            logger.info(
                    "Get edges and neighbors for vertices query for {} ids finished in {} ms",
                    linkingIds.size(),
                    sw1.elapsed( TimeUnit.MILLISECONDS )
            );
            sw1.reset().start();

            Set<UUID> authorizedEntitySetIds = authorizations.accessChecksForPrincipals(
                    allEntitySetIds.stream()
                            .map( esId -> new AccessCheck( new AclKey( esId ), READ_PERMISSION ) )
                            .collect( Collectors.toSet() ), principals )
                    .filter( auth -> auth.getPermissions().get( Permission.READ ) ).map( auth -> auth.getAclKey().get( 0 ) )
                    .collect( Collectors.toSet() );

            Map<UUID, EntitySet> entitySetsById = entitySetService.getEntitySetsAsMap( authorizedEntitySetIds );

            Map<UUID, Map<UUID, PropertyType>> entitySetsIdsToAuthorizedProps =
                    Maps.newHashMapWithExpectedSize( entitySetsById.size() );
            Map<UUID, Set<UUID>> authorizedEdgeESIdsToVertexESIds =
                    Maps.newHashMapWithExpectedSize( entitySetsById.size() );
            Map<UUID, EntityType> entityTypesById = dataModelService
                    .getEntityTypesAsMap( entitySetsById.values().stream().map( entitySet -> {
                        entitySetsIdsToAuthorizedProps.put( entitySet.getId(), Maps.newHashMap() );
                        authorizedEdgeESIdsToVertexESIds.put( entitySet.getId(), Sets.newHashSet() );
                        return entitySet.getEntityTypeId();
                    } ).collect( Collectors.toSet() ) );

            Map<UUID, PropertyType> propertyTypesById = dataModelService
                    .getPropertyTypesAsMap( entityTypesById.values().stream()
                            .flatMap( entityType -> entityType.getProperties().stream() ).collect(
                                    Collectors.toSet() ) );

            Set<AccessCheck> accessChecks = entitySetsById.values().stream()
                    .flatMap( entitySet -> entityTypesById.get( entitySet.getEntityTypeId() ).getProperties().stream()
                            .map( propertyTypeId -> new AccessCheck( new AclKey( entitySet.getId(), propertyTypeId ),
                                    READ_PERMISSION ) ) ).collect( Collectors.toSet() );

            authorizations.accessChecksForPrincipals( accessChecks, principals ).forEach( auth -> {
                if ( auth.getPermissions().get( Permission.READ ) ) {
                    UUID esId = auth.getAclKey().get( 0 );
                    UUID propertyTypeId = auth.getAclKey().get( 1 );
                    entitySetsIdsToAuthorizedProps.get( esId )
                            .put( propertyTypeId, propertyTypesById.get( propertyTypeId ) );
                }
            } );

            logger.info(
                    "Access checks for entity sets and their properties finished in {} ms",
                    sw1.elapsed( TimeUnit.MILLISECONDS )
            );
            sw1.reset().start();

            Map<UUID, Set<UUID>> entitySetIdToEntityKeyId = Maps.newHashMap();
            edges.forEach( edge -> {
                UUID edgeEntityKeyId = edge.getEdge().getEntityKeyId();
                UUID neighborEntityKeyId = ( entityKeyIds.contains( edge.getSrc().getEntityKeyId() ) ) ? edge.getDst()
                        .getEntityKeyId()
                        : edge.getSrc().getEntityKeyId();
                UUID edgeEntitySetId = edge.getEdge().getEntitySetId();
                UUID neighborEntitySetId = ( entityKeyIds.contains( edge.getSrc().getEntityKeyId() ) ) ? edge.getDst()
                        .getEntitySetId()
                        : edge.getSrc().getEntitySetId();

                if ( entitySetsIdsToAuthorizedProps.containsKey( edgeEntitySetId ) ) {
                    entitySetIdToEntityKeyId.putIfAbsent( edgeEntitySetId, Sets.newHashSet() );
                    entitySetIdToEntityKeyId.get( edgeEntitySetId ).add( edgeEntityKeyId );

                    if ( entitySetsIdsToAuthorizedProps.containsKey( neighborEntitySetId ) ) {
                        authorizedEdgeESIdsToVertexESIds.get( edgeEntitySetId ).add( neighborEntitySetId );
                        entitySetIdToEntityKeyId.putIfAbsent( neighborEntitySetId, Sets.newHashSet() );
                        entitySetIdToEntityKeyId.get( neighborEntitySetId ).add( neighborEntityKeyId );
                    }
                }

            } );

            logger.info( "Edge and neighbor entity key ids collected in {} ms", sw1.elapsed( TimeUnit.MILLISECONDS ) );
            sw1.reset().start();

            Collection<Map<FullQualifiedName, Set<Object>>> entitiesAcrossEntitySetIds = dataManager
                    .getEntitiesAcrossEntitySets( entitySetIdToEntityKeyId, entitySetsIdsToAuthorizedProps );

            logger.info(
                    "Get entities across entity sets query finished in {} ms",
                    sw1.elapsed( TimeUnit.MILLISECONDS )
            );
            sw1.reset().start();

            Map<UUID, Map<FullQualifiedName, Set<Object>>> entities = entitiesAcrossEntitySetIds.stream()
                    .collect( Collectors.toMap( SearchService::getEntityKeyId, Function.identity() ) );

            // create a NeighborEntityDetails object for each edge based on authorizations
            Map<UUID, List<NeighborEntityDetails>> entityNeighbors = Maps.newConcurrentMap();
            edges.parallelStream().forEach( edge -> {
                boolean vertexIsSrc = entityKeyIds.contains( edge.getKey().getSrc().getEntityKeyId() );
                UUID entityId = ( vertexIsSrc )
                        ? edge.getKey().getSrc().getEntityKeyId()
                        : edge.getKey().getDst().getEntityKeyId();
                entityNeighbors.putIfAbsent( entityId, Collections.synchronizedList( Lists.newArrayList() ) );

                NeighborEntityDetails neighbor = getNeighborEntityDetails( edge,
                        authorizedEdgeESIdsToVertexESIds,
                        entitySetsById,
                        vertexIsSrc,
                        entities );
                if ( neighbor != null ) {
                    entityNeighbors.get( entityId ).add( neighbor );
                }
            } );
            logger.info( "Neighbor entity details collected in {} ms", sw1.elapsed( TimeUnit.MILLISECONDS ) );
            sw1.reset().start();

            /* Map linkingIds to the collection of neighbors for all entityKeyIds in the cluster */
            var encryptedLinkingIds = idCipher.encryptIdsAsMap( linkedEntitySetId, linkingIds );
            entityKeyIdsByLinkingId.forEach( ( linkingId, normalEntityKeyIds ) ->
                    linkingEntityNeighbors.put(
                            encryptedLinkingIds.get( linkingId ),
                            normalEntityKeyIds.stream()
                                    .flatMap( entityKeyId -> entityNeighbors
                                            .getOrDefault( entityKeyId, Lists.newArrayList() ).stream() )
                                    .collect( Collectors.toList() ) )
            );

            logger.info(
                    "Finished entity neighbor search for linked entity set {} in {} ms",
                    linkedEntitySetId,
                    sw1.elapsed( TimeUnit.MILLISECONDS )
            );
            sw1.reset().start();
        } );

        logger.info( "Finished entity neighbor search in {} ms", sw2.elapsed( TimeUnit.MILLISECONDS ) );
        return linkingEntityNeighbors;
    }

    private NeighborEntityDetails getNeighborEntityDetails(
            Edge edge,
            Map<UUID, Set<UUID>> authorizedEdgeESIdsToVertexESIds,
            Map<UUID, EntitySet> entitySetsById,
            boolean vertexIsSrc,
            Map<UUID, Map<FullQualifiedName, Set<Object>>> entities ) {

        UUID edgeEntitySetId = edge.getEdge().getEntitySetId();
        if ( authorizedEdgeESIdsToVertexESIds.containsKey( edgeEntitySetId ) ) {
            UUID neighborEntityKeyId = ( vertexIsSrc )
                    ? edge.getDst().getEntityKeyId()
                    : edge.getSrc().getEntityKeyId();
            UUID neighborEntitySetId = ( vertexIsSrc )
                    ? edge.getDst().getEntitySetId()
                    : edge.getSrc().getEntitySetId();

            Map<FullQualifiedName, Set<Object>> edgeDetails = entities.get( edge.getEdge().getEntityKeyId() );
            if ( edgeDetails != null ) {
                if ( authorizedEdgeESIdsToVertexESIds.get( edgeEntitySetId )
                        .contains( neighborEntitySetId ) ) {
                    Map<FullQualifiedName, Set<Object>> neighborDetails = entities.get( neighborEntityKeyId );

                    if ( neighborDetails != null ) {
                        return new NeighborEntityDetails(
                                entitySetsById.get( edgeEntitySetId ),
                                edgeDetails,
                                entitySetsById.get( neighborEntitySetId ),
                                neighborEntityKeyId,
                                neighborDetails,
                                vertexIsSrc );
                    }

                } else {
                    return new NeighborEntityDetails(
                            entitySetsById.get( edgeEntitySetId ),
                            edgeDetails,
                            vertexIsSrc );
                }
            }
        }
        return null;
    }

    @Timed
    public Map<UUID, Map<UUID, Map<UUID, Set<NeighborEntityIds>>>> executeLinkingEntityNeighborIdsSearch(
            Map<UUID, EntitySet> linkedEntitySets,
            EntityNeighborsFilter filter,
            Set<Principal> principals
    ) {
        if ( checkAssociationFilterMissing( filter ) ) {
            return ImmutableMap.of();
        }

        // If there are multiple linked entity sets for one linking id, they need to be returned as a separate entry
        Map<UUID, Map<UUID, Map<UUID, Set<NeighborEntityIds>>>> linkingEntityNeighbors = new HashMap<>();

        filter.getEntityKeyIds().forEach( ( linkedEntitySetId, linkingIds ) -> {
            var entityKeyIdsByLinkingIds =
                    getEntityKeyIdsOfLinkingIds( linkingIds, linkedEntitySets.get( linkedEntitySetId ) );
            var entityKeyIds =
                    entityKeyIdsByLinkingIds.values().stream().flatMap( Collection::stream ).collect( Collectors.toSet() );

            // Will return only entries, where there is at least 1 neighbor
            Map<UUID, Map<UUID, Map<UUID, Set<NeighborEntityIds>>>> entityNeighbors = executeEntityNeighborIdsSearch(
                    new EntityNeighborsFilter(
                            linkedEntitySets.get( linkedEntitySetId ).getLinkedEntitySets().stream()
                                    .collect( Collectors.toMap( Function.identity(), esId -> entityKeyIds ) ),
                            filter.getSrcEntitySetIds(),
                            filter.getDstEntitySetIds(),
                            filter.getAssociationEntitySetIds() ),
                    principals
            );

            if ( !entityNeighbors.isEmpty() ) {
                var encryptedLinkingIds = idCipher.encryptIdsAsMap( linkedEntitySetId,
                        entityKeyIdsByLinkingIds.keySet() );
                entityKeyIdsByLinkingIds.entrySet().stream()
                        .filter( entityKeyIdsEntry ->
                                entityNeighbors.keySet().stream().anyMatch( entityKeyIdsEntry.getValue()::contains ) )
                        .forEach(
                                entityKeyIdsEntry -> {
                                    Map<UUID, Map<UUID, Set<NeighborEntityIds>>> neighborIds =
                                            Maps.newHashMapWithExpectedSize( entityKeyIdsEntry.getValue().size() );
                                    entityKeyIdsEntry.getValue().stream()
                                            .filter( entityNeighbors::containsKey )
                                            .forEach( entityKeyId ->
                                                    entityNeighbors.get( entityKeyId ).forEach( neighborIds::put ) );

                                    linkingEntityNeighbors.put(
                                            encryptedLinkingIds.get( entityKeyIdsEntry.getKey() ),
                                            neighborIds );
                                }
                        );
            }
        } );

        return linkingEntityNeighbors;
    }

    @Timed
    public Map<UUID, Map<UUID, Map<UUID, Set<NeighborEntityIds>>>> executeEntityNeighborIdsSearch(
            EntityNeighborsFilter filter,
            Set<Principal> principals ) {
        final Stopwatch sw = Stopwatch.createStarted();

        logger.info( "Starting Reduced Entity Neighbor Search..." );
        if ( checkAssociationFilterMissing( filter ) ) {
            logger.info( "Missing association entity set ids. Returning empty result." );
            return ImmutableMap.of();
        }

        Set<UUID> entityKeyIds = filter.getEntityKeyIds().values().stream()
                .flatMap( Set::stream ).collect( Collectors.toSet() );
        Set<UUID> allEntitySetIds = Sets.newHashSet();

        Map<UUID, Map<UUID, Map<UUID, Set<NeighborEntityIds>>>> neighbors = Maps.newHashMap();

        graphService.getEdgesAndNeighborsForVerticesBulk( filter ).forEach( edge -> {

            boolean isSrc = entityKeyIds.contains( edge.getSrc().getEntityKeyId() );
            UUID entityKeyId = isSrc ? edge.getSrc().getEntityKeyId() : edge.getDst().getEntityKeyId();
            UUID edgeEsId = edge.getEdge().getEntitySetId();
            EntityDataKey neighborEntityDataKey = isSrc ? edge.getDst() : edge.getSrc();
            UUID neighborEsId = neighborEntityDataKey.getEntitySetId();

            NeighborEntityIds neighborEntityIds = new NeighborEntityIds( edge.getEdge().getEntityKeyId(),
                    neighborEntityDataKey.getEntityKeyId(),
                    isSrc );

            neighbors.putIfAbsent( entityKeyId, Maps.newHashMap() );
            neighbors.get( entityKeyId ).putIfAbsent( edgeEsId, Maps.newHashMap() );
            neighbors.get( entityKeyId ).get( edgeEsId ).putIfAbsent( neighborEsId, Sets.newHashSet() );

            neighbors.get( entityKeyId ).get( edgeEsId ).get( neighborEsId ).add( neighborEntityIds );

            allEntitySetIds.add( edgeEsId );
            allEntitySetIds.add( neighborEsId );

        } );

        Set<UUID> unauthorizedEntitySetIds = authorizations.accessChecksForPrincipals( allEntitySetIds.stream()
                .map( esId -> new AccessCheck( new AclKey( esId ), READ_PERMISSION ) )
                .collect( Collectors.toSet() ), principals )
                .filter( auth -> !auth.getPermissions().get( Permission.READ ) )
                .map( auth -> auth.getAclKey().get( 0 ) )
                .collect( Collectors.toSet() );

        if ( unauthorizedEntitySetIds.size() > 0 ) {
            neighbors.values().forEach( associationMap -> {
                associationMap.values().forEach( neighborsMap -> neighborsMap.entrySet().removeIf( neighborEntry ->
                        unauthorizedEntitySetIds.contains( neighborEntry.getKey() ) )
                );
                associationMap.entrySet().removeIf( entry ->
                        unauthorizedEntitySetIds.contains( entry.getKey() ) || entry.getValue().size() == 0
                );
            } );
        }

        logger.info( "Reduced entity neighbor search took {} ms", sw.elapsed( TimeUnit.MILLISECONDS ) );

        return neighbors;
    }

    private Collection<Map<FullQualifiedName, Set<Object>>> getResults(
            EntitySet entitySet,
            Set<UUID> entityKeyIds,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes,
            boolean linking
    ) {
        if ( entityKeyIds.size() == 0 ) { return ImmutableList.of(); }
        if ( linking ) {
            final var authorizedPropertiesOfNormalEntitySets = entitySet.getLinkedEntitySets().stream()
                    .collect(
                            Collectors.toMap(
                                    Function.identity(),
                                    normalEntitySetId -> authorizedPropertyTypes.get( entitySet.getId() )
                            )
                    );

            return dgm.getLinkingEntities(
                    entitySet,
                    entityKeyIds,
                    authorizedPropertiesOfNormalEntitySets,
                    EnumSet.of( MetadataOption.LAST_WRITE )
            );
        } else {
            return dgm.getEntities(
                    entitySet.getId(),
                    ImmutableSet.copyOf( entityKeyIds ),
                    authorizedPropertyTypes.get( entitySet.getId() ),
                    EnumSet.of( MetadataOption.LAST_WRITE )
            );
        }
    }

    /**
     * Decrypts and collects entity key ids belonging to the requested encrypted linking ids.
     *
     * @param linkingIds      Encrypted linking ids.
     * @param linkedEntitySet The linked entity sets.
     * @return Map of entity key ids mapped by their linking ids.
     */
    private Map<UUID, Set<UUID>> getEntityKeyIdsOfLinkingIds( Set<UUID> linkingIds, EntitySet linkedEntitySet ) {
        var decryptedLinkingIds = idCipher.decryptIds( linkedEntitySet.getId(), linkingIds );
        return dataManager
                .getEntityKeyIdsByLinkingIds( decryptedLinkingIds, linkedEntitySet.getLinkedEntitySets() )
                .stream().collect( Collectors.toMap( Pair::getFirst, Pair::getSecond ) );
    }

    private boolean checkAssociationFilterMissing( EntityNeighborsFilter filter ) {
        return filter.getAssociationEntitySetIds().isPresent() && filter.getAssociationEntitySetIds().get().isEmpty();
    }

    public static UUID getEntityKeyId( Map<FullQualifiedName, Set<Object>> entity ) {
        return UUID.fromString( entity.get( EdmConstants.ID_FQN ).iterator().next().toString() );
    }

    @Subscribe
    public void clearAllData( ClearAllDataEvent event ) {
        elasticsearchApi.clearAllData();
    }

    public void triggerPropertyTypeIndex( List<PropertyType> propertyTypes ) {
        elasticsearchApi.triggerSecurableObjectIndex( SecurableObjectType.PropertyTypeInEntitySet, propertyTypes );
    }

    public void triggerEntityTypeIndex( List<EntityType> entityTypes ) {
        elasticsearchApi.triggerSecurableObjectIndex( SecurableObjectType.EntityType, entityTypes );
    }

    public void triggerAssociationTypeIndex( List<AssociationType> associationTypes ) {
        elasticsearchApi.triggerSecurableObjectIndex( SecurableObjectType.AssociationType, associationTypes );
    }

    public void triggerEntitySetIndex() {
        Map<EntitySet, Set<UUID>> entitySets = StreamUtil.stream( entitySetService.getEntitySets() ).collect( Collectors
                .toMap( entitySet -> entitySet,
                        entitySet -> dataModelService.getEntityType( entitySet.getEntityTypeId() ).getProperties() ) );
        Map<UUID, PropertyType> propertyTypes = StreamUtil.stream( dataModelService.getPropertyTypes() )
                .collect( Collectors.toMap( PropertyType::getId, pt -> pt ) );
        elasticsearchApi.triggerEntitySetIndex( entitySets, propertyTypes );
    }

    public void triggerEntitySetDataIndex( UUID entitySetId ) {
        EntityType entityType = entitySetService.getEntityTypeByEntitySetId( entitySetId );
        Map<UUID, PropertyType> propertyTypes = dataModelService.getPropertyTypesAsMap( entityType.getProperties() );
        List<PropertyType> propertyTypeList = Lists.newArrayList( propertyTypes.values() );

        elasticsearchApi.deleteEntitySet( entitySetId, entityType.getId() );
        elasticsearchApi.saveEntitySetToElasticsearch(
                entityType,
                entitySetService.getEntitySet( entitySetId ),
                propertyTypeList );

        EntitySet entitySet = entitySetService.getEntitySet( entitySetId );
        Set<UUID> entitySetIds = ( entitySet.isLinking() ) ? entitySet.getLinkedEntitySets() : Set.of( entitySetId );
        indexingMetadataManager.markEntitySetsAsNeedsToBeIndexed( entitySetIds, entitySet.isLinking() );
    }

    public void triggerAllEntitySetDataIndex() {
        entitySetService.getEntitySets().forEach( entitySet -> triggerEntitySetDataIndex( entitySet.getId() ) );
    }

    public void triggerAppIndex( List<App> apps ) {
        elasticsearchApi.triggerSecurableObjectIndex( SecurableObjectType.App, apps );
    }

    public void triggerAppTypeIndex( List<AppType> appTypes ) {
        elasticsearchApi.triggerSecurableObjectIndex( SecurableObjectType.AppType, appTypes );
    }

    public void triggerAllOrganizationsIndex( List<Organization> allOrganizations ) {
        elasticsearchApi.triggerOrganizationIndex( allOrganizations );
    }

    public void triggerOrganizationIndex( Organization organization ) {
        elasticsearchApi.triggerOrganizationIndex( Lists.newArrayList( organization ) );
    }

}
