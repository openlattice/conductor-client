package com.kryptnostic.datastore.services;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.services.requests.AclRequest;

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

    // {namespace}/{schema_name}/{class}/{FQN}/{FQN}
    /*
     * /entity/type/{namespace}/{name} /entity/set/{namespace}/{name} /schema/{namespace}/{name}
     * /property/{namespace}/{name}
     */
    String SCHEMA_BASE_PATH        = "/schema";
    String ENTITY_SETS_BASE_PATH   = "/entity/set";
    String ENTITY_TYPE_BASE_PATH   = "/entity/type";
    String PROPERTY_TYPE_BASE_PATH = "/property/type";
    String NAMESPACE_PATH          = "/{" + NAMESPACE + "}";
    String NAME_PATH               = "/{" + NAME + "}";
    String ACL_PATH                = "/acl";

/** TODO: there should be an endpoint returning all users's rights on a type
    @GET( PROPERTY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH + ACL_PATH )
    Map< UUID, Set<String> > getPropertyTypeAcls( @Path( NAMESPACE ) String namespace, @Path( NAME ) String name );
*/ 
    @POST( PROPERTY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH + ACL_PATH )
    Response setPropertyTypeAcls( @Path( NAMESPACE ) String namespace, @Path( NAME ) String name, @Body Set<AclRequest> requests );

    @DELETE( PROPERTY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH + ACL_PATH )
    Response removePropertyTypeAcls( @Path( NAMESPACE ) String namespace, @Path( NAME ) String name, @Body Set<UUID> users );

    @POST( ENTITY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH + ACL_PATH )
    Response setEntityTypeAcls( @Path( NAMESPACE ) String namespace, @Path( NAME ) String name, @Body Set<ModifyEntityTypeAclRequest> requests );

    @DELETE( ENTITY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH + ACL_PATH )
    Response removeEntityTypeAcls( @Path( NAMESPACE ) String namespace, @Path( NAME ) String name, @Body Set<RemoveEntityTypeAclRequest> requests );
    
}
