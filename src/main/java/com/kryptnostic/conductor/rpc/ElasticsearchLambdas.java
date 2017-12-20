package com.kryptnostic.conductor.rpc;

import com.dataloom.apps.App;
import com.dataloom.apps.AppType;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.Principals;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.organization.Organization;
import com.dataloom.search.requests.SearchResult;
import com.google.common.base.Optional;
import com.openlattice.authorization.AclKey;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

public class ElasticsearchLambdas implements Serializable {
    private static final long serialVersionUID = -4180766624983725307L;

    public static Function<ConductorElasticsearchApi, Boolean> submitEntitySetToElasticsearch(
            EntitySet entitySet,
            List<PropertyType> propertyTypes,
            Principal principal ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .saveEntitySetToElasticsearch( entitySet, propertyTypes, principal );
    }

    public static Function<ConductorElasticsearchApi, Boolean> createSecurableObjectIndex(
            UUID entitySetId,
            UUID syncId,
            List<PropertyType> propertyTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .createSecurableObjectIndex( entitySetId, syncId, propertyTypes );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeEntitySetMetadataQuery(
            Optional<String> optionalQuery,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            Set<AclKey> authorizedAclKeys,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeEntitySetMetadataSearch( optionalQuery,
                        optionalEntityType,
                        optionalPropertyTypes,
                        authorizedAclKeys,
                        start,
                        maxHits );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteEntitySet( UUID entitySetId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteEntitySet( entitySetId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteEntitySetForSyncId(
            UUID entitySetId,
            UUID syncId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteEntitySetForSyncId( entitySetId, syncId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> createOrganization(
            Organization organization,
            Principal principal ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .createOrganization( organization, principal );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeOrganizationKeywordSearch(
            String searchTerm,
            Set<AclKey> authorizedOrganizationIds,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeOrganizationSearch( searchTerm, authorizedOrganizationIds, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, Boolean> updateOrganization(
            UUID id,
            Optional<String> optionalTitle,
            Optional<String> optionalDescription ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .updateOrganization( id, optionalTitle, optionalDescription );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteOrganization( UUID organizationId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteOrganization( organizationId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> updateEntitySetMetadata( EntitySet entitySet ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .updateEntitySetMetadata( entitySet );
    }

    public static Function<ConductorElasticsearchApi, Boolean> updatePropertyTypesInEntitySet(
            UUID entitySetId,
            List<PropertyType> newPropertyTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .updatePropertyTypesInEntitySet( entitySetId, newPropertyTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> saveEntityTypeToElasticsearch( EntityType entityType ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .saveEntityTypeToElasticsearch( entityType );
    }

    public static Function<ConductorElasticsearchApi, Boolean> saveAssociationTypeToElasticsearch( AssociationType associationType ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .saveAssociationTypeToElasticsearch( associationType );
    }

    public static Function<ConductorElasticsearchApi, Boolean> savePropertyTypeToElasticsearch( PropertyType propertyType ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .savePropertyTypeToElasticsearch( propertyType );
    }

    public static Function<ConductorElasticsearchApi, Boolean> saveAppToElasticsearch( App app ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .saveAppToElasticsearch( app );
    }

    public static Function<ConductorElasticsearchApi, Boolean> saveAppTypeToElasticsearch( AppType appType ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .saveAppTypeToElasticsearch( appType );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteEntityType( UUID entityTypeId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteEntityType( entityTypeId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteAssociationType( UUID associationTypeId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteAssociationType( associationTypeId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deletePropertyType( UUID propertyTypeId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deletePropertyType( propertyTypeId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteApp( UUID appId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteApp( appId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteAppType( UUID appTypeId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteAppType( appTypeId );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeEntityTypeSearch(
            String searchTerm,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeEntityTypeSearch( searchTerm, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeAssociationTypeSearch(
            String searchTerm,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeAssociationTypeSearch( searchTerm, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeAppSearch(
            String searchTerm,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeAppSearch( searchTerm, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeAppTypeSearch(
            String searchTerm,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeAppTypeSearch( searchTerm, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executePropertyTypeSearch(
            String searchTerm,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executePropertyTypeSearch( searchTerm, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeFQNEntityTypeSearch(
            String namespace,
            String name,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeFQNEntityTypeSearch( namespace, name, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeFQNPropertyTypeSearch(
            String namespace,
            String name,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeFQNPropertyTypeSearch( namespace, name, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteEntityData(
            UUID entitySetId,
            UUID syncId,
            String entityId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteEntityData( entitySetId, syncId, entityId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> clearAllData() {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .clearAllData();
    }

    public static Function<ConductorElasticsearchApi, Double> getModelScore( double[][] features ) {
        return (Function<ConductorElasticsearchApi, Double> & Serializable) ( api ) -> api
                .getModelScore( features );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerPropertyTypeIndex( List<PropertyType> propertyTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerPropertyTypeIndex( propertyTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerEntityTypeIndex( List<EntityType> entityTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerEntityTypeIndex( entityTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerAssociationTypeIndex( List<AssociationType> associationTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerAssociationTypeIndex( associationTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerAppIndex( List<App> apps ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerAppIndex( apps );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerAppTypeIndex( List<AppType> appTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerAppTypeIndex( appTypes );
    }



}
