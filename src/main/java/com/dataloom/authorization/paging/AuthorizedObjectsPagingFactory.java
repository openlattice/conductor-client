package com.dataloom.authorization.paging;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.datastax.driver.core.PagingState;

public class AuthorizedObjectsPagingFactory {
    private static final Encoder encoder   = Base64.getEncoder();
    private static final Decoder decoder   = Base64.getDecoder();

    private static final String  delimiter = ":";

    private AuthorizedObjectsPagingFactory() {}

    public static AuthorizedObjectsPagingInfo createSafely( Principal principal, PagingState pagingState ){
        return ( principal == null ) ? null : new AuthorizedObjectsPagingInfo( principal, pagingState );
    }
    
    public static String encode( AuthorizedObjectsPagingInfo info ) {
        if ( info == null ) {
            return null;
        }

        String principalType = info.getPrincipal().getType().toString();
        String principalId = info.getPrincipal().getId();
        String pagingState = ( info.getPagingState() == null ) ? null : info.getPagingState().toString();

        String key = new StringBuilder().append( principalType ).append( delimiter )
                .append( principalId ).append( delimiter )
                .append( pagingState )
                .toString();

        final byte[] bytes = key.getBytes( StandardCharsets.UTF_8 );
        return new String( encoder.encode( bytes ), StandardCharsets.UTF_8 );
    }

    public static AuthorizedObjectsPagingInfo decode( String token ) {
        if( token == null ){
            return null;
        }
        
        final byte[] bytes = token.getBytes( StandardCharsets.UTF_8 );

        String key = new String( decoder.decode( bytes ), StandardCharsets.UTF_8 );
        String[] parts = key.split( delimiter );

        Principal principal = new Principal( PrincipalType.valueOf( parts[ 0 ] ), parts[ 1 ] );
        PagingState pagingState = ( parts.length == 3 ) ? PagingState.fromString( parts[ 2 ] ) : null;
        
        return new AuthorizedObjectsPagingInfo( principal, pagingState );
    }
}
