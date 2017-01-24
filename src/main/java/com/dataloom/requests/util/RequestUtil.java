package com.dataloom.requests.util;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.Principals;
import com.dataloom.requests.Request;
import com.dataloom.requests.RequestStatus;
import com.dataloom.requests.Status;
import com.datastax.driver.core.Row;
import com.kryptnostic.datastore.cassandra.RowAdapters;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.dataloom.authorization.util.AuthorizationUtils.*;

;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public final class RequestUtil {
    private RequestUtil() {
    }

    public static @Nonnull Status newStatus( @Nonnull Request request ) {
        return new Status(
                request.getAclKey(),
                Principals.getCurrentUser(),
                request.getPermissions(),
                RequestStatus.SUBMITTED );
    }

    public static AceKey aceKey( Status status ) {
        return new AceKey( status.getAclKey(), status.getPrincipal() );
    }

    public static Status status( Row row ) {
        return new Status(
                aclKey( row ),
                getPrincipalFromRow( row ),
                permissions( row ),
                RowAdapters.reqStatus( row ) );
    }

    public static Map<AceKey, Status> reqsAsStatusMap( Set<Request> requests ) {
        return requests
                .stream()
                .map( RequestUtil::newStatus )
                .collect( Collectors.toMap( RequestUtil::aceKey, Function.identity() ) );
    }

    public static Map<AceKey, Status> statusMap( Set<Status> requests ) {
        return requests
                .stream()
                .collect( Collectors.toMap( RequestUtil::aceKey, Function.identity() ) );
    }

    public static Status approve( Status s ) {
        return new Status( s.getAclKey(), s.getPrincipal(), s.getPermissions(), RequestStatus.APPROVED );
    }

    public static Status decline( Status s ) {
        return new Status( s.getAclKey(), s.getPrincipal(), s.getPermissions(), RequestStatus.DECLINED );
    }
}
