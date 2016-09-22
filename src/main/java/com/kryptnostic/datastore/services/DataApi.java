package com.kryptnostic.datastore.services;

import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
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

    String FULLQUALIFIEDNAME = "fullqulifiedname";
    String NAME              = "name";
    String NAME_SPACE        = "namespace";

    String ENTITYSET              = "/entityset";
    String ENTITY_DATA            = "/entitydata";
    String FILTERED               = "/filtered";
    String INTEGRATION            = "/integration";
    String FULLQUALIFIEDNAME_PATH = "/{" + FULLQUALIFIEDNAME + "}";
    String NAME_PATH              = "/{" + NAME + "}";
    String NAME_SPACE_PATH        = "/{" + NAME_SPACE + "}";

    @GET( CONTROLLER + ENTITYSET )
    List<UUID> getAllEntitySet( LoadEntitySetRequest loadEntitySetRequest );

    @GET( CONTROLLER + ENTITYSET + FILTERED )
    List<UUID> getFilteredEntitySet( LookupEntitySetRequest lookupEntitiesRequest );

    @GET( CONTROLLER + ENTITY_DATA )
    Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType( LoadAllEntitiesOfTypeRequest loadAllEntitiesOfTypeRequest );

    @PUT( CONTROLLER + ENTITY_DATA )
    Iterable<SetMultimap<FullQualifiedName, Object>> getAllEntitiesOfType( @Body FullQualifiedName fqn );

    @GET( CONTROLLER + ENTITY_DATA + FULLQUALIFIEDNAME_PATH )
    Iterable<SetMultimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @Path( FULLQUALIFIEDNAME ) String fanAsString );

    @GET( CONTROLLER + ENTITY_DATA + NAME_SPACE_PATH + NAME_SPACE )
    Iterable<SetMultimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @Path( NAME_SPACE ) String namespace,
            @Path( NAME ) String name );

    @GET( CONTROLLER + ENTITY_DATA + FILTERED )
    Iterable<SetMultimap<FullQualifiedName, Object>> getFilteredEntitiesOfType( LookupEntitiesRequest lookupEntitiesRequest );

    @POST( CONTROLLER + ENTITY_DATA )
    Response createEntityData( CreateEntityRequest createEntityRequest );

    @GET( CONTROLLER + INTEGRATION )
    Map<String, String> getAllIntegrationScripts();

    @PUT( CONTROLLER + INTEGRATION )
    Map<String, String> getIntegrationScript( @Body Set<String> url );

    @POST( CONTROLLER + INTEGRATION )
    Response createIntegrationScript( @Body Map<String, String> integrationScripts );

    @GET( CONTROLLER + "/multimap" )
    SetMultimap<FullQualifiedName, Object> getSetMultimap();

    @POST( CONTROLLER + "/multimap" )
    Response createSetMultimap( @Body SetMultimap<FullQualifiedName, Object> setMultimap );

}
