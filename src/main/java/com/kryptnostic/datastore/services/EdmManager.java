package com.kryptnostic.datastore.services;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.AclKey;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;

public interface EdmManager {
    EntityDataModel getEntityDataModel();

    PropertyType getPropertyType( FullQualifiedName prop );

    void createPropertyTypeIfNotExists( PropertyType propertyType );

    void deletePropertyType( UUID propertyTypeId );

    Iterable<PropertyType> getPropertyTypesInNamespace( String namespace );

    Iterable<PropertyType> getPropertyTypes();

    void createEntitySet( Principal principal, EntitySet entitySet );

    EntitySet getEntitySet( String name );

    Iterable<EntitySet> getEntitySets();

    Iterable<EntitySet> getEntitySetsUserOwns( String userId );

    Iterable<String> getEntitySetNamesUserOwns( String userId );

    void deleteEntitySet( EntitySet entitySet );

    void deleteEntitySet( String name );

    void createEntityType( EntityType objectType );

    void assignEntityToEntitySet( UUID entityId, String entitySetName );

    void assignEntityToEntitySet( UUID entityId, EntitySet entitySet );

    EntityType getEntityType( String namespace, String name );

    Iterable<EntityType> getEntityTypes();

    void deleteEntityType( UUID entityTypeId );

    EntityType getEntityType( UUID entityTypeId );

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

    void upsertEntityType( Principal principal, EntityType entityType );


    Collection<PropertyType> getPropertyTypes( Set<UUID> properties );

    Set<AclKey> getAclKeys( Set<FullQualifiedName> fqns );

    AclKey getTypeAclKey( FullQualifiedName fqns );

    Set<UUID> getEntityTypeUuids( Set<FullQualifiedName> fqns );

    Set<UUID> getPropertyTypeUuids( Set<FullQualifiedName> fqns );

    EntityType getEntityType( FullQualifiedName type );
}