package com.dataloom.authorization.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.UUID;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;
import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.AclKey;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public final class CassandraMappingUtils {
    private CassandraMappingUtils() {}

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
        return new AceKey( getSecurableObjectIdFromRow( row ), getSecurableObjectTypeFromRow( row ), principal );
    }

    public static AclKey getAclKeyFromRow( Row row ) {
        return new AclKey(
                getSecurableObjectTypeFromRow( row ),
                getSecurableObjectIdFromRow( row ) );
    }

    public static SecurableObjectType getSecurableObjectTypeFromRow( Row row ) {
        return row.get( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), SecurableObjectType.class );
        // checkState( StringUtils.isNotBlank( securableType ), "Encountered blank securable type" );
        // return SecurableObjectType.valueOf( securableType );
    }

    public static UUID getSecurableObjectIdFromRow( Row row ) {
        return checkNotNull( row.getUUID( CommonColumns.SECURABLE_OBJECTID.cql() ),
                "Securable object id cannot be null." );
    }

    /**
     * Useful adapter for {@code Iterables#transform(Iterable, com.google.common.base.Function)} that allows lazy
     * evaluation of result set future.
     * 
     * @param rsf The result set future to make a lazy evaluated iterator
     * @return The lazy evaluatable iteratable
     */
    public static Iterable<Row> makeLazy( ResultSetFuture rsf ) {
        return rsf.getUninterruptibly()::iterator;
    }

}
