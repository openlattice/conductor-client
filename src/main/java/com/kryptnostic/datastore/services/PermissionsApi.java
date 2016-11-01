package com.kryptnostic.datastore.services;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.datastore.services.requests.EntityTypeAclRequest;
import com.kryptnostic.datastore.services.requests.EntitySetAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.EntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRequest;

import retrofit.client.Response;
import retrofit.http.Body;
import retrofit.http.DELETE;
import retrofit.http.POST;

/**
 * 
 * @author Ho Chung Siu
 *
 */
public interface PermissionsApi {

    String NAME                    = "name";
    String NAMESPACE               = "namespace";
    String ACTION                  = "action";

    String CONTROLLER              = "/acl";
    String ENTITY_SETS_BASE_PATH   = "/entity/set";
    String ENTITY_TYPE_BASE_PATH   = "/entity/type";
    String PROPERTY_TYPE_BASE_PATH = "/property/type";
    String ALL_PATH                = "/all";

    /**
     * 
     * @param requests Set of EntityTypeAclRequest, each updating access rights of one entity type for one role.
     * Format of one EntityTypeAclRequest is as follows:
     * - role: [String] role where access rights will be updated.
     * - action: [Enum add/set/remove] action for access rights
     * - type: [FullQualifiedName] FullQualifiedName of entity type to be updated
     * - permissions: [Set<Enum discover/read/write/alter>] set of permissions to be added/set/removed, according to the action.
     * @return
     */
    @POST( CONTROLLER + ENTITY_TYPE_BASE_PATH )
    Response updateEntityTypesAcls( @Body Set<EntityTypeAclRequest> requests );

    /**
     * 
     * @param requests Set of EntitySetAclRequest, each updating access rights of one entity set for one role.
     * Format of one EntitySetRequest is as follows:
     * - role: [String] role where access rights will be updated.
     * - action: [Enum add/set/remove] action for access rights
     * - type: [FullQualifiedName] FullQualifiedName of entity type of entity set to be updated
     * - name: [String] name of entity set to be updated
     * - permissions: [Set<Enum discover/read/write/alter>] set of permissions to be added/set/removed, according to the action.
     * @return
     */

    @POST( CONTROLLER + ENTITY_SETS_BASE_PATH )
    Response updateEntitySetsAcls( @Body Set<EntitySetAclRequest> requests );

    /**
     * 
     * @param requests Set of PropertyTypeInEntityTypeAclRequest, each updating access rights of one entity set for one role.
     * Format of one PropertyTypeInEntityTypeAclRequest is as follows:
     * - role: [String] role where access rights will be updated.
     * - action: [Enum add/set/remove] action for access rights
     * - type: [FullQualifiedName] FullQualifiedName of entity type to be updated
     * - properties: [FullQualifiedName] FullQualifiedName of property type to be updated 
     * - permissions: [Set<Enum discover/read/write/alter>] set of permissions to be added/set/removed, according to the action.
     * @return
     */
    @POST( CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH )
    Response updatePropertyTypeInEntityTypeAcls( @Body Set<PropertyTypeInEntityTypeAclRequest> requests );

    /**
     * 
     * @param requests Set of PropertyTypeInEntitySetAclRequest, each updating access rights of one entity set for one role.
     * Format of one PropertyTypeInEntitySet is as follows:
     * - role: [String] role where access rights will be updated.
     * - action: [Enum add/set/remove] action for access rights
     * - type: [FullQualifiedName] FullQualifiedName of entity type of entity set to be updated
     * - name: [String] name of entity set to be updated
     * - properties: [FullQualifiedName] FullQualifiedName of property type to be updated
     * - permissions: [Set<Enum discover/read/write/alter>] set of permissions to be added/set/removed, according to the action.
     * @return
     */
    @POST( CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH )
    Response updatePropertyTypeInEntitySetAcls( @Body Set<PropertyTypeInEntitySetAclRequest> requests );

    /**
     * 
     * @param entityTypeFqns Set of FullQualifiedName of entity Types, where all the access rights associated to them are to be removed.
     * @return
     */    
    @DELETE( CONTROLLER + ENTITY_TYPE_BASE_PATH )
    Response removeEntityTypeAcls( @Body Set<FullQualifiedName> entityTypeFqns );

    /**
     * 
     * @param requests Set of EntitySetAclRemovalRequest, where all the access rights associated to the entity sets are to be removed.
     * Format of one EntitySetAclRemovalRequest is as follows:
     * - type: [FullQualifiedName] FullQualifiedName of entity type of entity set
     * - name: [String] name of entity set
     * @return
     */
    @DELETE( CONTROLLER + ENTITY_SETS_BASE_PATH )
    Response removeEntitySetAcls( @Body Set<EntitySetAclRemovalRequest> requests );

    /**
     * 
     * @param requests Set of PropertyTypeInEntityTypeAclRemovalRequest, where all the access rights associated to the (entity type, property type) pairs are to be removed.
     * Format of one PropertyTypeInEntityTypeAclRemovalRequest is as follows:
     * - type: [FullQualifiedName] FullQualifiedName of entity type
     * - properties: [Set<FullQualifiedName>] FullQualifiedName of properties to be removed.
     * @return
     */    
    @DELETE( CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH )
    Response removePropertyTypeInEntityTypeAcls( @Body Set<PropertyTypeInEntityTypeAclRemovalRequest> requests );

    /**
     * 
     * @param requests Set of FullQualifiedName of entity types, where the access rights of all property types associated to each entity type are removed.
     * @return
     */    
    @DELETE( CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH + ALL_PATH )
    Response removeAllPropertyTypesInEntityTypeAcls( @Body Set<FullQualifiedName> entityTypeFqns );

    /**
     * 
     * @param requests Set of PropertyTypeInEntitySetAclRemovalRequest, where all the access rights associated to the (entity set, property type) pairs are to be removed.
     * Format of one PropertyTypeInEntitySetAclRemovalRequest is as follows:
     * - type: [FullQualifiedName] FullQualifiedName of entity type of the entity set
     * - name: [String] name of the entity set
     * - properties: [Set<FullQualifiedName>] FullQualifiedName of properties to be removed.
     * @return
     */    
    @DELETE( CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH )
    Response removePropertyTypeInEntitySetAcls( @Body Set<PropertyTypeInEntitySetAclRemovalRequest> requests );

    /**
     * 
     * @param requests Set of EntitySetAclRemovalRequest, where all the access rights associated to each entity set are removed.
     * Format of one EntitySetAclRemovalRequest is as follows:
     * - type: [FullQualifiedName] FullQualifiedName of entity type of the entity set.
     * - name: [String] name of the entity set.
     * @return
     */    
    @DELETE( CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH + ALL_PATH )
    Response removeAllPropertyTypesInEntitySetAcls( @Body Set<EntitySetAclRemovalRequest> requests );

}
