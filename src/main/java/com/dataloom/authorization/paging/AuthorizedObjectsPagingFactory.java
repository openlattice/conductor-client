package com.dataloom.authorization.paging;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openlattice.authorization.Principal;
import com.dataloom.mappers.ObjectMappers;
import com.datastax.driver.core.PagingState;
import com.fasterxml.jackson.core.JsonProcessingException;

public class AuthorizedObjectsPagingFactory {
    private static final Logger  logger  = LoggerFactory.getLogger( AuthorizedObjectsPagingFactory.class );

    private static final Encoder encoder = Base64.getUrlEncoder();
    private static final Decoder decoder = Base64.getUrlDecoder();

    private AuthorizedObjectsPagingFactory() {}

    public static AuthorizedObjectsPagingInfo createSafely( Principal principal, PagingState pagingState ) {
        return ( principal == null ) ? null : new AuthorizedObjectsPagingInfo( principal, pagingState );
    }

    public static String encode( AuthorizedObjectsPagingInfo info ) {
        if ( info == null ) {
            return null;
        }

        final byte[] bytes;
        try {
            bytes = ObjectMappers.getSmileMapper().writeValueAsBytes( info );
        } catch ( JsonProcessingException e ) {
            logger.error( "Error serializing AuthorizedObjectsPagingInfo: " + info );
            return null;
        }
        return new String( encoder.encode( bytes ), StandardCharsets.UTF_8 );
    }

    public static AuthorizedObjectsPagingInfo decode( String token ) {
        if ( token == null ) {
            return null;
        }

        final byte[] bytes = token.getBytes( StandardCharsets.UTF_8 );

        try {
            return ObjectMappers.getSmileMapper().readValue( decoder.decode( bytes ),
                    AuthorizedObjectsPagingInfo.class );
        } catch ( IOException e ) {
            logger.error( "Error deserializing AuthorizedObjectsPagingInfo." );
            return null;
        }
    }
}
