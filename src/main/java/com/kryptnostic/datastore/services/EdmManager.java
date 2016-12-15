package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.requests.Principal;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.requests.GetSchemasRequest.TypeDetails;

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

    void createEntitySet( Principal principal, FullQualifiedName type, String name, String title );

    void createEntitySet( Principal principal, EntitySet entitySet );

    void createEntitySet( String typename, String name, String title );

    void createEntitySet( EntitySet entitySet );

    void upsertEntitySet( EntitySet entitySet );

    EntitySet getEntitySet( String name );

    Iterable<EntitySet> getEntitySets();

    Iterable<EntitySet> getEntitySetsUserOwns( String userId );

    Iterable<String> getEntitySetNamesUserOwns( String userId );

    void deleteEntitySet( EntitySet entitySet );

    void deleteEntitySet( String name );

    void createEntityType( Principal principal, EntityType entityType );

    void createEntityType( EntityType objectType );

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
    
    //Validation methods, and Helper methods to check existence
    public void ensureValidEntityType( EntityType entityType );
    
    public void ensureValidSchema( Schema schema );
    
    public void ensureSchemaExists( String namespace, String name );

    public void ensureEntityTypesExist( Set<FullQualifiedName> entityTypes );

    public void ensureEntityTypeExists( FullQualifiedName entityTypeFqn );
    
    public void ensureEntityTypeExists( String typename );

    public void ensurePropertyTypesExist( Set<FullQualifiedName> propertyTypes );

    public void ensurePropertyTypeExists( FullQualifiedName propertyTypeFqn );
    
    public void ensureEntitySetExists( String typename, String entitySetName );

    public void ensureEntityTypeDoesNotExist( FullQualifiedName entityTypeFqn );
    
    public void ensurePropertyTypeDoesNotExist( FullQualifiedName propertyTypeFqn );

    public void ensureSchemaDoesNotExist( String namespace, String name );
    
    public void ensureEntitySetDoesNotExist( String typename, String entitySetName );

    boolean checkPropertyTypesExist( Set<FullQualifiedName> properties );

    boolean checkPropertyTypeExists( FullQualifiedName propertyTypeFqn );

    boolean checkEntityTypesExist( Set<FullQualifiedName> entityTypes );

    boolean checkEntityTypeExists( FullQualifiedName entityTypeFqn );

    boolean checkEntitySetExists( String name );

    boolean checkSchemaExists( String namespace, String name );

    void upsertEntityType( Principal principal, EntityType entityType );

    void upsertPropertyType( PropertyType propertyType );
}