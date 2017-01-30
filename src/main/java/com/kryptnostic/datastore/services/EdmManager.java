package com.kryptnostic.datastore.services;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.Principal;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.hazelcast.map.EntryProcessor;

public interface EdmManager {
    PropertyType getPropertyType( FullQualifiedName propertyTypeFqn );

    PropertyType getPropertyType( UUID propertyTypeId );

    void createPropertyTypeIfNotExists( PropertyType propertyType );

    void deletePropertyType( UUID propertyTypeId );

    Iterable<PropertyType> getPropertyTypesInNamespace( String namespace );

    Iterable<PropertyType> getPropertyTypes();

    void createEntitySet( Principal principal, EntitySet entitySet );

    EntitySet getEntitySet( UUID entitySetId );

    Iterable<EntitySet> getEntitySets();

    void deleteEntitySet( UUID entitySetId );

    void createEntityType( EntityType objectType );

    void assignEntityToEntitySet( UUID syncId, String entityId, String name );

    void assignEntityToEntitySet( UUID syncId, String entityId, EntitySet es );

    EntityType getEntityType( String namespace, String name );

    Iterable<EntityType> getEntityTypes();

    void deleteEntityType( UUID entityTypeId );

    EntityType getEntityType( UUID entityTypeId );

    void addPropertyTypesToEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds );

    void removePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds );
    
    void renamePropertyType( UUID typeId, FullQualifiedName newFqn );

    void renameEntityType( UUID typeId, FullQualifiedName newFqn );

    void renameEntitySet( UUID typeId, String newName );

    // Helper methods to check existence
    boolean checkPropertyTypesExist( Set<UUID> properties );

    boolean checkPropertyTypeExists( UUID propertyTypeId );

    boolean checkEntityTypeExists( UUID entityTypeId );

    boolean checkEntitySetExists( String name );

    Collection<PropertyType> getPropertyTypes( Set<UUID> properties );

    Set<UUID> getAclKeys( Set<?> fqnsOrNames );

    UUID getTypeAclKey( FullQualifiedName fqns );

    Set<UUID> getEntityTypeUuids( Set<FullQualifiedName> fqns );

    Set<UUID> getPropertyTypeUuids( Set<FullQualifiedName> fqns );

    EntityType getEntityType( FullQualifiedName type );

    EntitySet getEntitySet( String entitySetName );

    FullQualifiedName getPropertyTypeFqn( UUID propertyTypeId );

    FullQualifiedName getEntityTypeFqn( UUID entityTypeId );
    
    Map<UUID, PropertyType> getPropertyTypesAsMap( Set<UUID> propertyTypeIds );

    Map<UUID, EntityType> getEntityTypesAsMap( Set<UUID> entityTypeIds );

    Map<UUID, EntitySet> getEntitySetsAsMap( Set<UUID> entitySetIds );

    <V> Map<UUID, V> fromPropertyTypes( Set<UUID> propertyTypeIds, EntryProcessor<UUID, PropertyType> ep );

}