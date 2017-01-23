package com.dataloom.auditing.util;

import com.dataloom.auditing.AuditMetric;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.datastax.driver.core.Row;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public final class AuditUtil {
    private AuditUtil() {
    }

    public static AuditMetric auditMetric( Row row ) {
        long count = row.getLong( CommonColumns.COUNT.cql() );
        return new AuditMetric( count, AuthorizationUtils.aclKey( row ) );
    }
}
