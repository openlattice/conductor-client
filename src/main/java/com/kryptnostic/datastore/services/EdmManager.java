package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.datastore.services.requests.GetSchemasRequest.TypeDetails;

public interface EdmManager {
	/** 
	 * Being of debug
	 */
	void setCurrentUserForDebug( String username, Set<String> roles );
	/**
	 * End of debug
	 */
    boolean createSchema( String namespace, String name, UUID aclId, Set<FullQualifiedName> entityTypes, Set<FullQualifiedName> propertyTypes );
    //would attach all property types of the entityTypes to Schema
    boolean createSchema( String namespace, String name, UUID aclId, Set<FullQualifiedName> entityTypes);

    void upsertSchema( Schema namespace );

    Iterable<Schema> getSchemas();

    Iterable<Schema> getSchemas( Set<TypeDetails> requestedDetails );

    Iterable<Schema> getSchemasInNamespace( String namespace, Set<TypeDetails> requestedDetails );

    Iterable<Schema> getSchema( String namespace, String name, Set<TypeDetails> requestedDetails );

    void deleteSchema( Schema namespaces );

    boolean createEntitySet( FullQualifiedName type, String name, String title );

    boolean createEntitySet( String typename, String name, String title );

    boolean createEntitySet( EntitySet entitySet );

    void upsertEntitySet( EntitySet entitySet );

    EntitySet getEntitySet( FullQualifiedName entityType, String name );

    EntitySet getEntitySet( String name );

    Iterable<EntitySet> getEntitySets();

    void deleteEntitySet( EntitySet entitySet );
    
    void deleteEntitySet( FullQualifiedName entityType, String name );

    boolean createEntityType( EntityType objectType );

    boolean assignEntityToEntitySet( UUID entityId, String entitySetName );

    boolean assignEntityToEntitySet( UUID entityId, EntitySet entitySet );

    void upsertEntityType( EntityType objectType );

    EntityType getEntityType( String namespace, String name );

    Iterable<EntityType> getEntityTypes();

    void deleteEntityType( FullQualifiedName entityTypeFqn );

    void addEntityTypesToSchema( String namespace, String name, Set<FullQualifiedName> entityTypes );

    void removeEntityTypesFromSchema( String namespace, String name, Set<FullQualifiedName> entityTypes );

	void addPropertyTypesToSchema(String namespace, String name, Set<FullQualifiedName> properties);
	
	void removePropertyTypesFromSchema(String namespace, String name, Set<FullQualifiedName> properties);

    boolean createPropertyType( PropertyType propertyType );

    void upsertPropertyType( PropertyType propertyType );

    void deletePropertyType( FullQualifiedName propertyTypeFqn );

    PropertyType getPropertyType( FullQualifiedName prop );

    Iterable<PropertyType> getPropertyTypesInNamespace( String namespace );

    Iterable<PropertyType> getPropertyTypes();

    EntityDataModel getEntityDataModel();

    boolean isExistingEntitySet( FullQualifiedName type, String name );

    EntityType getEntityType( FullQualifiedName fqn );

    FullQualifiedName getPropertyTypeFullQualifiedName( String typename );

    FullQualifiedName getEntityTypeFullQualifiedName( String typename );

    void addPropertyTypesToEntityType(String entityTypeNamespace, String entityTypeName, Set<FullQualifiedName> properties);

    void removePropertyTypesFromEntityType(String entityTypeNamespace, String entityTypeName, Set<FullQualifiedName> properties);
    
    void removePropertyTypesFromEntityType(EntityType entityType, Set<FullQualifiedName> properties);

}