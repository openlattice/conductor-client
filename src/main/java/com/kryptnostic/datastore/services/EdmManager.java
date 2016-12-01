package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.requests.Principal;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.requests.GetSchemasRequest.TypeDetails;
import com.google.common.base.Optional;

public interface EdmManager {
    void createSchema(
            String namespace,
            String name,
            UUID aclId,
            Set<FullQualifiedName> entityTypes,
            Set<FullQualifiedName> propertyTypes );

    // would attach all property types of the entityTypes to Schema
    void createSchema( String namespace, String name, UUID aclId, Set<FullQualifiedName> entityTypes );

    void upsertSchema( Schema namespace );

    Iterable<Schema> getSchemas();

    Iterable<Schema> getSchemas( Set<TypeDetails> requestedDetails );

    Iterable<Schema> getSchemasInNamespace( String namespace, Set<TypeDetails> requestedDetails );

    Schema getSchema( String namespace, String name, Set<TypeDetails> requestedDetails );

    void deleteSchema( Schema namespaces );

    void createEntitySet( FullQualifiedName type, String name, String title );

    void createEntitySet( String typename, String name, String title );

    void createEntitySet( EntitySet entitySet );

    void createEntitySet( Principal principal, EntitySet entitySet );

    void upsertEntitySet( EntitySet entitySet );

    EntitySet getEntitySet( String name );

    Iterable<EntitySet> getEntitySets();

    Iterable<EntitySet> getEntitySetsUserOwns( String username );

    Iterable<String> getEntitySetNamesUserOwns( String username );

    void deleteEntitySet( EntitySet entitySet );

    void deleteEntitySet( String name );

    void createEntityType( Principal principal, EntityType entityType );

    void assignEntityToEntitySet( UUID entityId, String entitySetName );

    void assignEntityToEntitySet( UUID entityId, EntitySet entitySet );

    EntityType getEntityType( String namespace, String name );

    Iterable<EntityType> getEntityTypes();

    void deleteEntityType( FullQualifiedName entityTypeFqn );

    void addEntityTypesToSchema( String namespace, String name, Set<FullQualifiedName> entityTypes );

    void removeEntityTypesFromSchema( String namespace, String name, Set<FullQualifiedName> entityTypes );

    void addPropertyTypesToSchema( String namespace, String name, Set<FullQualifiedName> properties );

    void removePropertyTypesFromSchema( String namespace, String name, Set<FullQualifiedName> properties );

    void createPropertyType( PropertyType propertyType );

    void deletePropertyType( FullQualifiedName propertyTypeFqn );

    PropertyType getPropertyType( FullQualifiedName prop );

    Iterable<PropertyType> getPropertyTypesInNamespace( String namespace );

    Iterable<PropertyType> getPropertyTypes();

    EntityDataModel getEntityDataModel();

    EntityType getEntityType( FullQualifiedName fqn );

    FullQualifiedName getPropertyTypeFullQualifiedName( String typename );

    FullQualifiedName getEntityTypeFullQualifiedName( String typename );

    void addPropertyTypesToEntityType(
            String entityTypeNamespace,
            String entityTypeName,
            Set<FullQualifiedName> properties );

    void removePropertyTypesFromEntityType(
            String entityTypeNamespace,
            String entityTypeName,
            Set<FullQualifiedName> properties );

    void removePropertyTypesFromEntityType( EntityType entityType, Set<FullQualifiedName> properties );

    // Helper methods to check existence
    boolean checkPropertyTypesExist( Set<FullQualifiedName> properties );

    boolean checkPropertyTypeExists( FullQualifiedName propertyTypeFqn );

    boolean checkEntityTypeExists( FullQualifiedName entityTypeFqn );

    boolean checkEntitySetExists( String name );

    boolean checkSchemaExists( String namespace, String name );


}