/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.kryptnostic.conductor.rpc.odata;

import com.dataloom.edm.internal.DatastoreConstants;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.cassandra.TableDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;

import static com.kryptnostic.datastore.cassandra.CommonColumns.*;

public enum Tables implements TableDef {
    ACL_KEYS,
    AUDIT_EVENTS,
    AUDIT_METRICS,
    DATA,
    ENTITY_EDGES,
    ENTITY_ID_LOOKUP,
    ENTITY_SETS,
    ENTITY_TYPES,
    LINKING_EDGES,
    NAMES,
    ORGANIZATIONS,
    PERMISSIONS,
    PERMISSIONS_REQUESTS_UNRESOLVED,
    PERMISSIONS_REQUESTS_RESOLVED,
    PROPERTY_TYPES,
    SCHEMAS,
    REQUESTS;

    private static final Logger                                 logger   = LoggerFactory
            .getLogger( Tables.class );
    private static final EnumMap<Tables, CassandraTableBuilder> cache    = new EnumMap<>( Tables.class );
    private static       String                                 keyspace = DatastoreConstants.KEYSPACE;

    static CassandraTableBuilder getTableDefinition( Tables table ) {
        CassandraTableBuilder ctb = cache.get( table );
        if ( ctb == null ) {
            ctb = createTableDefinition( table );
            cache.put( table, ctb );
        }
        return ctb;
    }

    static CassandraTableBuilder createTableDefinition( Tables table ) {
        switch ( table ) {
            case ACL_KEYS:
                return new CassandraTableBuilder( ACL_KEYS )
                        .ifNotExists()
                        .partitionKey( NAME )
                        .columns( SECURABLE_OBJECTID );
            case AUDIT_EVENTS:
                return new CassandraTableBuilder( AUDIT_EVENTS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( TIME_ID, PRINCIPAL_TYPE, PRINCIPAL_ID )
                        .columns( CommonColumns.PERMISSIONS, AUDIT_EVENT_DETAILS, BLOCK )
                        .sasi( PRINCIPAL_TYPE, PRINCIPAL_ID );
            case AUDIT_METRICS:
                return new CassandraTableBuilder( AUDIT_METRICS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( COUNT, ACL_KEY_VALUE )
                        .withDescendingOrder( COUNT );
            case ENTITY_EDGES:
                /*
                 * Implied in the table structure here is that a graph link must be written in the same sync job
                 * as the associated data.
                 */
                return new CassandraTableBuilder( ENTITY_EDGES )
                        .ifNotExists()
                        .partitionKey( CommonColumns.SOURCE )
                        .clusteringColumns( CommonColumns.DESTINATION )
                        .columns( CommonColumns.ENTITYID, CommonColumns.SYNCID )
                        .sasi( CommonColumns.SYNCID );
            case LINKING_EDGES:
                return new CassandraTableBuilder( LINKING_EDGES )
                        .ifNotExists()
                        .partitionKey( CommonColumns.GRAPH_ID, CommonColumns.SOURCE )
                        .clusteringColumns( CommonColumns.DESTINATION )
                        .columns( CommonColumns.EDGE_VALUE );
            case ENTITY_ID_LOOKUP:
                return new CassandraTableBuilder( ENTITY_ID_LOOKUP )
                        .ifNotExists()
                        .partitionKey( SYNCID, ENTITY_SET_ID )
                        .clusteringColumns( ENTITYID )
                        .secondaryIndex( ENTITY_SET_ID );
            case DATA:
                return new CassandraTableBuilder( DATA )
                        .ifNotExists()
                        .partitionKey( ENTITYID )
                        .clusteringColumns( PROPERTY_TYPE_ID, PROPERTY_VALUE )
                        .columns( SYNCID )
                        .sasi( SYNCID );
            case ENTITY_SETS:
                return new CassandraTableBuilder( ENTITY_SETS )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( NAME )
                        .columns( ENTITY_TYPE_ID,
                                TITLE,
                                DESCRIPTION )
                        .secondaryIndex( ENTITY_TYPE_ID, NAME );
            case ENTITY_TYPES:
                return new CassandraTableBuilder( ENTITY_TYPES )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( NAMESPACE, NAME )
                        .columns( TITLE,
                                DESCRIPTION,
                                KEY,
                                PROPERTIES,
                                CommonColumns.SCHEMAS )
                        .secondaryIndex( NAMESPACE, CommonColumns.SCHEMAS );
            case NAMES:
                return new CassandraTableBuilder( NAMES )
                        .ifNotExists()
                        .partitionKey( SECURABLE_OBJECTID )
                        .columns( NAME );
            case ORGANIZATIONS:
                return new CassandraTableBuilder( ORGANIZATIONS )
                        .ifNotExists()
                        .partitionKey( ID )
                        .columns( TITLE,
                                DESCRIPTION,
                                TRUSTED_ORGANIZATIONS,
                                ALLOWED_EMAIL_DOMAINS,
                                MEMBERS,
                                ROLES );
            case PROPERTY_TYPES:
                return new CassandraTableBuilder( PROPERTY_TYPES )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( NAMESPACE, NAME )
                        .columns( TITLE,
                                DESCRIPTION,
                                CommonColumns.SCHEMAS,
                                DATATYPE,
                                PII_FIELD )
                        .secondaryIndex( NAMESPACE, CommonColumns.SCHEMAS );
            case PERMISSIONS:
                // TODO: Once Cassandra fixes SASI + Collection column inde
                return new CassandraTableBuilder( PERMISSIONS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( PRINCIPAL_TYPE, PRINCIPAL_ID )
                        .columns( CommonColumns.PERMISSIONS )
                        .staticColumns( SECURABLE_OBJECT_TYPE )
                        .secondaryIndex( PRINCIPAL_TYPE,
                                PRINCIPAL_ID,
                                CommonColumns.PERMISSIONS,
                                SECURABLE_OBJECT_TYPE );
            case PERMISSIONS_REQUESTS_UNRESOLVED:
                return new CassandraTableBuilder( PERMISSIONS_REQUESTS_UNRESOLVED )
                        .ifNotExists()
                        .partitionKey( ACL_ROOT )
                        .clusteringColumns( PRINCIPAL_ID )
                        .columns( ACL_CHILDREN_PERMISSIONS, STATUS )
                        .sasi( STATUS );
            case PERMISSIONS_REQUESTS_RESOLVED:
                return new CassandraTableBuilder( PERMISSIONS_REQUESTS_RESOLVED )
                        .ifNotExists()
                        .partitionKey( PRINCIPAL_ID )
                        .clusteringColumns( REQUESTID )
                        .columns( ACL_ROOT, ACL_CHILDREN_PERMISSIONS, STATUS )
                        .fullCollectionIndex( ACL_ROOT )
                        .sasi( STATUS );
            case REQUESTS:
                return new CassandraTableBuilder( REQUESTS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( PRINCIPAL_TYPE, PRINCIPAL_ID )
                        .columns( CommonColumns.PERMISSIONS, STATUS )
                        .sasi( PRINCIPAL_TYPE,
                                PRINCIPAL_ID,
                                STATUS );
            case SCHEMAS:
                return new CassandraTableBuilder( SCHEMAS )
                        .ifNotExists()
                        .partitionKey( NAMESPACE )
                        .columns( NAME_SET );
            default:
                logger.error( "Missing table configuration {}, unable to start.", table.name() );
                throw new IllegalStateException( "Missing table configuration " + table.name() + ", unable to start." );
        }
    }

    public String getName() {
        return name();
    }

    public String getKeyspace() {
        return keyspace;
    }

    public CassandraTableBuilder getBuilder() {
        return getTableDefinition( this );
    }

    public TableDef asTableDef() {
        return this;
    }

}
