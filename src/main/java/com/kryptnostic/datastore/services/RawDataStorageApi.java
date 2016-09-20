package com.kryptnostic.datastore.services;

import com.google.common.collect.SetMultimap;
import com.kryptnostic.conductor.rpc.*;
import com.squareup.okhttp.Response;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import retrofit.http.GET;
import retrofit.http.POST;

import java.util.List;
import java.util.UUID;

/**
 * Created by yao on 9/20/16.
 */
public interface RawDataStorageApi {
    String CONTROLLER  = "／raw";

    String ENTITYSET   = "/entityset";
    String ENTITY_DATA = "/entitydata";
    String FILTERED    = "filtered";

    @GET( CONTROLLER + ENTITYSET )
    List<UUID> getAllEntitySet( LoadEntitySetRequest loadEntitySetRequest );

    @GET( CONTROLLER + ENTITYSET + FILTERED )
    List<UUID> getFilteredEntitySet( LookupEntitySetRequest lookupEntitiesRequest );

    @GET( CONTROLLER + ENTITY_DATA )
    Iterable<SetMultimap<FullQualifiedName, Object>> getAllEntitiesOfType( LoadAllEntitiesOfTypeRequest loadAllEntitiesOfTypeRequest );

    @GET( CONTROLLER + ENTITY_DATA + FILTERED )
    Iterable<SetMultimap<FullQualifiedName, Object>> getFilteredEntitiesOfType( LookupEntitiesRequest lookupEntitiesRequest );

    @POST( CONTROLLER + ENTITY_DATA )
    Response createEntityData( CreateEntityRequest createEntityRequest );

}
