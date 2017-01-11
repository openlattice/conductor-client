package com.dataloom.authorization.util;

import static com.google.common.base.Preconditions.checkState;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;
import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.SecurableObjectType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public final class AuthorizationUtils {
    private AuthorizationUtils() {}

    public static Principal getPrincipalFromRow( Row row ) {
        final String principalType = row.getString( CommonColumns.PRINCIPAL_TYPE.cql() );
        final String principalId = row.getString( CommonColumns.PRINCIPAL_ID.cql() );
        checkState( StringUtils.isNotBlank( principalId ), "Securable object id cannot be null." );
        checkState( StringUtils.isNotBlank( principalType ), "Encountered blank securable type" );
        return new Principal(
                PrincipalType.valueOf( principalType ),
                principalId );
    }

    public static AceKey getAceKeyFromRow( Row row ) {
        Principal principal = getPrincipalFromRow( row );
        return new AceKey( getAclKeysFromRow( row ), principal );
    }

    public static List<AclKeyPathFragment> getAclKeysFromRow( Row row ) {
        return row.getList( CommonColumns.ACL_KEYS.cql(), AclKeyPathFragment.class );
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

    public static SecurableObjectType extractObjectType( AceKey key ) {
        AclKeyPathFragment aclKey = getLastAclKeySafely( key.getKey() );
        // TODO: Do something better than return null.
        return aclKey == null ? null : aclKey.getType();
    }

    public static AclKeyPathFragment getLastAclKeySafely( List<AclKeyPathFragment> aclKeys ) {
        return aclKeys.isEmpty() ? aclKeys.get( aclKeys.size() - 1 ) : null;
    }

}
