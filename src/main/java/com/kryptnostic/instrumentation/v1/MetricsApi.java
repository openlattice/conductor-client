package com.kryptnostic.instrumentation.v1;

import retrofit.http.Body;
import retrofit.http.POST;
import retrofit.http.PUT;
import retrofit.http.Path;

import com.kryptnostic.instrumentation.v1.exceptions.types.BadRequestException;
import com.kryptnostic.instrumentation.v1.exceptions.types.ResourceNotFoundException;
import com.kryptnostic.instrumentation.v1.exceptions.types.ResourceNotLockedException;
import com.kryptnostic.instrumentation.v1.models.*;
import com.kryptnostic.storage.v1.models.ObjectMetadata;

public interface MetricsApi {
    final String CONTROLLER             = "/metrics";
    final String ID                     = "id";
    final String REALM                  = "realm";
    final String USER                   = "user";
    final String OBJECT_ID_PATH         = "/{" + ID + "}";
    final String OFFSET                 = "offset";
    final String PAGE_SIZE              = "pageSize";
    final String OBJECT_LIST_PAGED_PATH = "/{" + OFFSET + "}/{" + PAGE_SIZE + "}";
    final String TYPE                   = "type";
    final String TYPE_PATH              = "/type/{" + TYPE + "}";
    final String OBJECT_APPEND_PATH     = "/append";
    final String OBJECT_METADATA_PATH   = "/metadata";

    /**
     */
    @PUT( CONTROLLER )
    BasicResponse<String> createMetricsObject( @Body MetricsRequest request ) throws BadRequestException;

    /**
     */
    @POST( CONTROLLER + OBJECT_ID_PATH )
    BasicResponse<String> logMetricsObject( @Path( ID ) String id, @Body MetricsObject met )
            throws ResourceNotFoundException, ResourceNotLockedException, BadRequestException;
    
    @POST( CONTROLLER + OBJECT_ID_PATH )
    BasicResponse<String> createAndLogMetrics( @Body String type, String message )
            throws ResourceNotFoundException, ResourceNotLockedException, BadRequestException;
    
    
    @POST( CONTROLLER + OBJECT_ID_PATH )
	MetricsMetadata getObjectMetadata(String id)
			throws ResourceNotFoundException;

//    /**
//     * Retrieve an object's contents
//     *
//     * @param id
//     * @return Contents of object
//     */
//    @GET( CONTROLLER + OBJECT_ID_PATH )
//    KryptnosticObject getObject( @Path( ID ) String id ) throws ResourceNotFoundException;
//
//    @GET( CONTROLLER + OBJECT_ID_PATH + OBJECT_METADATA_PATH )
//    MetricsMetadata getObjectMetadata( @Path( ID ) String id ) throws ResourceNotFoundException;
//
//    @POST( CONTROLLER )
//    BasicResponse<List<KryptnosticObject>> getObjects( @Body List<String> objectIds ) throws ResourceNotFoundException;

    /**
     *
     * @return Collection of object ids
     */
//    @GET( CONTROLLER )
//    BasicResponse<Collection<String>> getObjectIds();
//
//    @GET( CONTROLLER + OBJECT_LIST_PAGED_PATH )
//    BasicResponse<Collection<String>> getObjectIds( @Path( OFFSET ) Integer offset, @Path( PAGE_SIZE ) Integer pageSize );
//
//    @GET( CONTROLLER + TYPE_PATH + OBJECT_LIST_PAGED_PATH )
//    BasicResponse<Collection<String>> getObjectIdsByType(
//            @Path( TYPE ) String type,
//            @Path( OFFSET ) Integer offset,
//            @Path( PAGE_SIZE ) Integer pageSize );
//
//    @GET( CONTROLLER + TYPE_PATH )
//    BasicResponse<Collection<String>> getObjectIdsByType( @Path( TYPE ) String type );
//
//    @POST( CONTROLLER + OBJECT_ID_PATH + "/blocks" )
//    BasicResponse<List<MetricsObject>> getObjectBlocks( @Path( ID ) String id, @Body List<Integer> indices )
//            throws ResourceNotFoundException;
//
//    @DELETE( CONTROLLER + OBJECT_ID_PATH )
//    BasicResponse<String> delete( @Path( ID ) String id );
//
//    @POST( CONTROLLER + OBJECT_ID_PATH + OBJECT_APPEND_PATH )
//    BasicResponse<String> appendObject( @Path( ID ) String objectId, @Body MetricsObject blockToAppend )
//            throws ResourceNotFoundException;

}
