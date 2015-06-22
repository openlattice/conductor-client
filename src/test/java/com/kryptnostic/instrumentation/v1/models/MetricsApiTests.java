package com.kryptnostic.instrumentation.v1.models;


import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.util.concurrent.ExecutionException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.kryptnostic.instrumentation.v1.MetricsApi;
import com.kryptnostic.instrumentation.v1.exceptions.types.BadRequestException;
import com.kryptnostic.instrumentation.v1.exceptions.types.ResourceNotFoundException;
import com.kryptnostic.instrumentation.v1.exceptions.types.ResourceNotLockedException;
import com.kryptnostic.instrumentation.v1.exceptions.types.SecurityConfigurationException;
import com.kryptnostic.metrics.v1.LoggingMetricsService;


@SuppressWarnings( "javadoc" )
public class MetricsApiTests {

//    private StorageClient            storageService;
//    private UserKey                  userKey;
	private DefaultLoggingClient	   loggingClient;
	private static MetricsApi			   loggingMetrics;      

	@BeforeClass
	public static void init() {
		loggingMetrics = new LoggingMetricsService("foo");
	}
    @Before
    public void setup() throws InvalidKeyException, IllegalBlockSizeException, BadPaddingException,
            NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeySpecException, InvalidParameterSpecException,
            InvalidAlgorithmParameterException, IOException, SignatureException, Exception {


    }

    private ResponseDefinitionBuilder jsonResponse( String s ) {
        return aResponse().withHeader( "Content-Type", "application/json" ).withBody( s );
    }

    @Test
    public void logOneMessageTest() throws
            NoSuchAlgorithmException, JsonProcessingException, ExecutionException, BadRequestException,
            SecurityConfigurationException, ResourceNotFoundException, ResourceNotLockedException {
        MetricsApi metricsApi = Mockito.mock( MetricsApi.class );

        loggingClient = new DefaultLoggingClient( metricsApi );
        
        String type = "api test";
        String message = "testing the api right now";

        String loggedID = loggingClient.uploadObject( new MetricsRequest(message, type) );
        
        System.out.println("LoggedID is " + loggedID);
        System.out.println("Message is " + loggingClient.getMetadata(loggedID).getLogMessage());
        
        Assert.assertTrue(message.equals(loggingClient.getMetadata(loggedID).getLogMessage()));
    }


}
