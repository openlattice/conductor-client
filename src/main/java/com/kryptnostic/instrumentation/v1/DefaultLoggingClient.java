package com.kryptnostic.instrumentation.v1;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kryptnostic.instrumentation.v1.LoggingClient;
import com.kryptnostic.instrumentation.v1.MetricsApi;
import com.kryptnostic.instrumentation.v1.exceptions.types.BadRequestException;
import com.kryptnostic.instrumentation.v1.exceptions.types.ResourceNotFoundException;
import com.kryptnostic.instrumentation.v1.exceptions.types.ResourceNotLockedException;
import com.kryptnostic.instrumentation.v1.exceptions.types.SecurityConfigurationException;
import com.kryptnostic.instrumentation.v1.models.MetricsMetadata;
import com.kryptnostic.instrumentation.v1.models.MetricsObject;
import com.kryptnostic.instrumentation.v1.models.MetricsRequest;

/**
 * @author Julianna Lamb
 *
 */
public class DefaultLoggingClient implements LoggingClient {
	private static final int 		  DEFAULT_VERSION          = 0;
    private static final int          PARALLEL_NETWORK_THREADS = 16;
    private static final int          METADATA_BATCH_SIZE      = 500;
    ExecutorService                   exec                     = Executors
                                                                       .newFixedThreadPool( PARALLEL_NETWORK_THREADS );

    /**
     * Server-side
     */
    private final MetricsApi		  metricsApi;
    /**
     * Client-side
     */
//    private final KryptnosticContext  context;


    /**
     * @param metricsApi
     */
    public DefaultLoggingClient(
            MetricsApi metricsApi ) {
        this.metricsApi = metricsApi;
    }
    
    
    public MetricsMetadata getMetadata(String id) throws ResourceNotFoundException {
    	return metricsApi.getObjectMetadata(id);
    }

	@Override
	public String uploadObject(MetricsRequest req) throws BadRequestException,
			SecurityConfigurationException, ResourceNotFoundException, ResourceNotLockedException {
		
		return metricsApi.createAndLogMetrics(req.getType(), req.getRequestBody()).getData().toString();
		
//        String id = req.getObjectId();
//
//        if ( id == null ) {
//            id = metricsApi.createMetricsObject(req).getData();
//        } else {
//            //handle this 
//        }
//        MetricsMetadata met = new MetricsMetadata(id, DEFAULT_VERSION, req.getRequestBody() );
//        MetricsObject obj = new MetricsObject( met );
//
//        String objId = met.getId();
// 
//        try {
//			metricsApi.logMetricsObject(objId, obj);
//		} catch (Exception e) {
//			//TODO catch exception
//			System.out.println("resource not locked exception");
//		}

      //  return objId;
    }
	

}
