package com.dataloom.requests.util;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.Permission;
import com.dataloom.requests.PermissionsRequest;
import com.dataloom.requests.PermissionsRequestDetails;
import com.dataloom.requests.RequestStatus;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.reflect.TypeToken;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class PermissionsRequestUtils {
    private PermissionsRequestUtils() {}
    
    /**
     * Useful adapter for {@code Iterables#transform(Iterable, com.google.common.base.Function)} that allows lazy
     * evaluation of result set future. See the same function in AuthorizationUtils as well.
     * 
     * @param rsf The result set future to make a lazy evaluated iterator
     * @return The lazy evaluatable iterable
     */
    public static Iterable<Row> makeLazy( ResultSetFuture rsf ) {
        return getRowsAndFlatten( Stream.of( rsf ) )::iterator;
    }

    public static Stream<Row> getRowsAndFlatten( Stream<ResultSetFuture> stream ) {
        return stream.map( ResultSetFuture::getUninterruptibly ).flatMap( rs -> StreamSupport.stream( rs.spliterator(), false ) );
    }
    
    public static PermissionsRequest getPRFromRow( Row row ){
        final List<AclKeyPathFragment> aclRoot = RowAdapters.aclRoot( row );
        final String userId = row.getString( CommonColumns.PRINCIPAL_ID.cql() );
        Map<AclKeyPathFragment, EnumSet<Permission>> permissions = row.getMap( CommonColumns.ACL_CHILDREN_PERMISSIONS.cql(), TypeToken.of( AclKeyPathFragment.class ), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
        RequestStatus status = RowAdapters.status( row );
        return new PermissionsRequest( aclRoot, userId, new PermissionsRequestDetails( permissions, status )  );
    }

}
