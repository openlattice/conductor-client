package com.dataloom.authorization.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;
import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public final class AuthorizationUtils {
    private AuthorizationUtils() {}

    public static Principal getPrincipalFromRow( Row row ) {
        final PrincipalType principalType = row.get( CommonColumns.PRINCIPAL_TYPE.cql(), PrincipalType.class );
        final String principalId = row.getString( CommonColumns.PRINCIPAL_ID.cql() );
        checkState( StringUtils.isNotBlank( principalId ), "Principal id cannot be null." );
        checkNotNull( principalType, "Encountered null principal type" );
        return new Principal(
                principalType,
                principalId );
    }

    public static AceKey getAceKeyFromRow( Row row ) {
        Principal principal = getPrincipalFromRow( row );
        return new AceKey( getAclKeysFromRow( row ), principal );
    }

    public static List<UUID> getAclKeysFromRow( Row row ) {
        return row.getList( CommonColumns.ACL_KEYS.cql(), UUID.class );
    }

    public static EnumSet<Permission> permissions( Row row ) {
        return row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
    }

    /**
     * Useful adapter for {@code Iterables#transform(Iterable, com.google.common.base.Function)} that allows lazy
     * evaluation of result set future.
     * 
     * @param rsf The result set future to make a lazy evaluated iterator
     * @return The lazy evaluatable iteratable
     */
    public static Iterable<Row> makeLazy( ResultSetFuture rsf ) {
        Stream<ResultSet> srs = Arrays.asList( rsf ).stream().map( ResultSetFuture::getUninterruptibly );
        Stream<Row> rows = srs.flatMap( rs -> StreamSupport.stream( rs.spliterator(), false ) );
        return rows::iterator;
    }

    public static UUID getLastAclKeySafely( List<UUID> aclKeys ) {
        return aclKeys.isEmpty() ? null : aclKeys.get( aclKeys.size() - 1 );
    }

    public static Stream<Row> makeStream( Iterable<Row> rs ) {
        return StreamSupport.stream( rs.spliterator(), false );
    }

}
