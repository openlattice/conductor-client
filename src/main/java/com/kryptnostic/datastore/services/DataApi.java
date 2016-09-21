package com.kryptnostic.datastore.services;

import com.google.common.collect.Multimap;
import com.kryptnostic.conductor.rpc.*;
import com.squareup.okhttp.Response;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import retrofit.http.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Created by yao on 9/20/16.
 */
public interface DataApi {
    String CONTROLLER = "/data";

    String ENTITYSET   = "/entityset";
    String ENTITY_DATA = "/entitydata";
    String FILTERED    = "/filtered";
    String INTEGRATION = "/integration";

    @GET( CONTROLLER + ENTITYSET )
    List<UUID> getAllEntitySet( LoadEntitySetRequest loadEntitySetRequest );

    @GET( CONTROLLER + ENTITYSET + FILTERED )
    List<UUID> getFilteredEntitySet( LookupEntitySetRequest lookupEntitiesRequest );

    @GET( CONTROLLER + ENTITY_DATA )
    Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType( LoadAllEntitiesOfTypeRequest loadAllEntitiesOfTypeRequest );

    @GET( CONTROLLER + ENTITY_DATA + FILTERED )
    Iterable<Multimap<FullQualifiedName, Object>> getFilteredEntitiesOfType( LookupEntitiesRequest lookupEntitiesRequest );

    @POST( CONTROLLER + ENTITY_DATA )
    Response createEntityData( CreateEntityRequest createEntityRequest );

    @GET( CONTROLLER + INTEGRATION )
    Map<String, String> getAllIntegrationScripts();

    @PUT( CONTROLLER + INTEGRATION )
    Map<String, String> getIntegrationScript( @Body Set<String> url );

    @POST( CONTROLLER + INTEGRATION )
    Response createIntegrationScript( @Body Map<String, String> integrationScripts );

}
