package com.kryptnostic.datastore.services;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.web.bind.annotation.RequestParam;

import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.services.requests.AclRequest;
import com.kryptnostic.datastore.services.requests.DeriveEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.DerivePropertyTypeInEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.DerivePropertyTypeInEntityTypeAclRequest;
import com.kryptnostic.datastore.services.requests.EntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.EntitySetAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRequest;

import retrofit.client.Response;
import retrofit.http.Body;
import retrofit.http.DELETE;
import retrofit.http.GET;
import retrofit.http.POST;
import retrofit.http.Path;

/**
 * 
 * @author Ho Chung Siu
 *
 */
public interface PermissionsApi {
    
    String ALIAS          = "alias";
    String ACL_ID         = "aclId";
    String LOAD_DETAILS   = "loadDetails";
    String NAME           = "name";
    String NAMESPACE      = "namespace";
    String NAMESPACES     = "namespaces";
    String ENTITY_SETS    = "entitySets";
    String ENTITY_TYPES   = "objectTypes";
    String PROPERTY_TYPES = "propertyTypes";
    String SCHEMA         = "schema";
    String SCHEMAS        = "schemas";
    String ACTION         = "action";
    String INHERIT        = "inherit";

    // {namespace}/{schema_name}/{class}/{FQN}/{FQN}
    /*
     * /entity/type/{namespace}/{name} /entity/set/{namespace}/{name} /schema/{namespace}/{name}
     * /property/{namespace}/{name}
     */
    String CONTROLLER = "/acl";
    String SCHEMA_BASE_PATH        = "/schema";
    String ENTITY_SETS_BASE_PATH   = "/entity/set";
    String ENTITY_TYPE_BASE_PATH   = "/entity/type";
    String PROPERTY_TYPE_BASE_PATH = "/property/type";
    String NAMESPACE_PATH          = "/{" + NAMESPACE + "}";
    String NAME_PATH               = "/{" + NAME + "}";
    String ALL_PATH = "/all";

    @POST( CONTROLLER + ENTITY_TYPE_BASE_PATH )
    Response updateEntityTypesAcls( @Body Set<AclRequest> requests );

    @POST( CONTROLLER + ENTITY_SETS_BASE_PATH )
    Response updateEntitySetsAcls( @Body Set<EntitySetAclRequest> requests );
    
    @POST( CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH )
    Response updatePropertyTypeInEntityTypeAcls( @Body Set<PropertyTypeInEntityTypeAclRequest> requests );

    @POST( CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH )
    Response updatePropertyTypeInEntitySetAcls( @Body Set<PropertyTypeInEntitySetAclRequest> requests );

    @DELETE( CONTROLLER + ENTITY_TYPE_BASE_PATH )
    Response removeEntityTypeAcls( @Body Set<FullQualifiedName> entityTypeFqns );

    @DELETE( CONTROLLER + ENTITY_SETS_BASE_PATH )
    Response removeEntitySetAcls( @Body Set<EntitySetAclRemovalRequest> requests );

    @DELETE( CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH )
    Response removePropertyTypeInEntityTypeAcls( @Body Set<PropertyTypeInEntityTypeAclRemovalRequest> requests );
 
    @DELETE( CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH + ALL_PATH )
    Response removeAllPropertyTypesInEntityTypeAcls( @Body Set<FullQualifiedName> entityTypeFqns );
    
    @DELETE( CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH )
    Response removePropertyTypeInEntitySetAcls( @Body Set<PropertyTypeInEntitySetAclRemovalRequest> requests );
 
    @DELETE( CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH + ALL_PATH )
    Response removeAllPropertyTypesInEntitySetAcls( @Body Set<EntitySetAclRemovalRequest> requests );

}

