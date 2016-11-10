package com.kryptnostic.datastore.services;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.Principal;
import com.kryptnostic.datastore.PrincipalType;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.Queries;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;

public class CassandraTableManager {
    static enum TableType {
        entity_,
        index_,
        property_,
        schema_;
    }

    static enum Index {
        BY_TYPES,
        BY_SETS,
        BY_ROLES,
        BY_USERS
    }

    private final String                                              keyspace;
    private final Session                                             session;

    private final ConcurrentMap<FullQualifiedName, PreparedStatement> propertyTypeUpdateStatements;
    private final ConcurrentMap<FullQualifiedName, PreparedStatement> propertyIndexUpdateStatements;
    private final ConcurrentMap<FullQualifiedName, PreparedStatement> entityTypeInsertStatements;
    private final ConcurrentMap<FullQualifiedName, PreparedStatement> entityTypeUpdateStatements;
    private final ConcurrentMap<FullQualifiedName, PreparedStatement> entityIdToTypeUpdateStatements;

    private final ConcurrentMap<UUID, PreparedStatement>              schemaInsertStatements;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaUpsertStatements;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaSelectStatements;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaSelectAllStatements;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaSelectAllInNamespaceStatements;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaAddEntityTypes;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaRemoveEntityTypes;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaAddPropertyTypes;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaRemovePropertyTypes;

    private final PreparedStatement                                   getTypenameForEntityType;
    private final PreparedStatement                                   getTypenameForPropertyType;
    private final PreparedStatement                                   countProperty;
    private final PreparedStatement                                   countEntityTypes;
    private final PreparedStatement                                   countEntitySets;
    private final PreparedStatement                                   insertPropertyTypeLookup;
    private final PreparedStatement                                   updatePropertyTypeLookup;
    private final PreparedStatement                                   deletePropertyTypeLookup;
    private final PreparedStatement                                   getPropertyTypeForTypename;
    private final PreparedStatement                                   insertEntityTypeLookup;
    private final PreparedStatement                                   updateEntityTypeLookup;
    private final PreparedStatement                                   deleteEntityTypeLookup;
    private final PreparedStatement                                   getEntityTypeForTypename;
    private final PreparedStatement                                   getTypenameForEntityId;
    private final PreparedStatement                                   assignEntityToEntitySet;

    private final PreparedStatement                                   entityTypeAddSchema;
    private final PreparedStatement                                   entityTypeRemoveSchema;
    private final PreparedStatement                                   propertyTypeAddSchema;
    private final PreparedStatement                                   propertyTypeRemoveSchema;

    private final Map<PrincipalType, PreparedStatement>               addPermissionsForEntityType;
    private final Map<PrincipalType, PreparedStatement>               setPermissionsForEntityType;
    private final Map<PrincipalType, PreparedStatement>               getPermissionsForEntityType;
    private final Map<PrincipalType, PreparedStatement>               getPermissionsForEntityTypeByType;
    private final Map<PrincipalType, PreparedStatement>               deleteRowFromEntityTypesAclsTable;
    private final Map<PrincipalType, PreparedStatement>               addPermissionsForEntitySet;
    private final Map<PrincipalType, PreparedStatement>               setPermissionsForEntitySet;
    private final Map<PrincipalType, PreparedStatement>               getPermissionsForEntitySet;
    private final Map<PrincipalType, PreparedStatement>               getPermissionsForEntitySetBySet;
    private final Map<PrincipalType, PreparedStatement>               deleteRowFromEntitySetsAclsTable;

    private final Map<PrincipalType, PreparedStatement>               addPermissionsForPropertyTypeInEntityType;
    private final Map<PrincipalType, PreparedStatement>               setPermissionsForPropertyTypeInEntityType;
    private final Map<PrincipalType, PreparedStatement>               getPermissionsForPropertyTypeInEntityType;
    private final Map<PrincipalType, PreparedStatement>               deleteRowFromPropertyTypesInEntityTypesAclsTable;
    private final Map<PrincipalType, PreparedStatement>               addPermissionsForPropertyTypeInEntitySet;
    private final Map<PrincipalType, PreparedStatement>               setPermissionsForPropertyTypeInEntitySet;
    private final Map<PrincipalType, PreparedStatement>               getPermissionsForPropertyTypeInEntitySet;
    private final Map<PrincipalType, PreparedStatement>               deleteRowFromPropertyTypesInEntitySetsAclsTable;

    private final PreparedStatement                                   getOwnerForEntitySet;
    private final PreparedStatement                                   getEntitySetsUserOwns;
    private final PreparedStatement                                   updateOwnerForEntitySet;
    private final PreparedStatement                                   updateOwnerLookupForEntitySet;
    private final PreparedStatement                                   deleteFromEntitySetOwnerTable;
    private final PreparedStatement                                   deleteFromEntitySetOwnerLookupTable;

    private final Map<PrincipalType, PreparedStatement>               insertAclsRequest;
    private final Map<PrincipalType, PreparedStatement>               updateLookupForAclsRequest;
    private final Map<PrincipalType, PreparedStatement>               deleteAclsRequest;
    private final Map<PrincipalType, PreparedStatement>               deleteLookupForAclsRequest;
    private final Map<PrincipalType, PreparedStatement>               getAclsRequestsByUsername;
    private final Map<PrincipalType, PreparedStatement>               getAclsRequestsByUsernameAndEntitySet;
    private final Map<PrincipalType, PreparedStatement>               getAclsRequestsByEntitySet;
    private final Map<PrincipalType, PreparedStatement>               getAclsRequestById;

    public CassandraTableManager(
            String keyspace,
            Session session,
            MappingManager mm ) {
        this.session = session;
        this.keyspace = keyspace;

        this.propertyTypeUpdateStatements = Maps.newConcurrentMap();
        this.propertyIndexUpdateStatements = Maps.newConcurrentMap();
        this.entityTypeInsertStatements = Maps.newConcurrentMap();
        this.entityTypeUpdateStatements = Maps.newConcurrentMap();
        this.schemaInsertStatements = Maps.newConcurrentMap();
        this.schemaUpsertStatements = Maps.newConcurrentMap();
        this.schemaSelectStatements = Maps.newConcurrentMap();
        this.schemaSelectAllStatements = Maps.newConcurrentMap();
        this.schemaSelectAllInNamespaceStatements = Maps.newConcurrentMap();
        this.schemaAddEntityTypes = Maps.newConcurrentMap();
        this.schemaRemoveEntityTypes = Maps.newConcurrentMap();
        this.schemaAddPropertyTypes = Maps.newConcurrentMap();
        this.schemaRemovePropertyTypes = Maps.newConcurrentMap();
        this.entityIdToTypeUpdateStatements = Maps.newConcurrentMap();

        initCoreTables( keyspace, session );
        prepareSchemaQueries();

        this.getTypenameForEntityType = session.prepare( QueryBuilder
                .select()
                .from( keyspace, Tables.ENTITY_TYPES.getTableName() )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(),
                        QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(),
                        QueryBuilder.bindMarker() ) ) );

        this.getTypenameForPropertyType = session.prepare( QueryBuilder
                .select()
                .from( keyspace, Tables.PROPERTY_TYPES.getTableName() )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(),
                        QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(),
                        QueryBuilder.bindMarker() ) ) );

        this.countProperty = session.prepare( QueryBuilder
                .select().countAll()
                .from( keyspace, DatastoreConstants.PROPERTY_TYPES_TABLE )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(),
                        QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(),
                        QueryBuilder.bindMarker() ) ) );

        this.countEntityTypes = session.prepare( QueryBuilder
                .select().countAll()
                .from( keyspace, DatastoreConstants.ENTITY_TYPES_TABLE )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(),
                        QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(),
                        QueryBuilder.bindMarker() ) ) );

        this.countEntitySets = session.prepare( Queries.countEntitySets( keyspace ) );

        this.insertPropertyTypeLookup = session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.PROPERTY_TYPE_LOOKUP.getTableName() )
                        .value( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.FQN.cql(), QueryBuilder.bindMarker() ) );

        this.updatePropertyTypeLookup = session
                .prepare( ( QueryBuilder.update( keyspace, Tables.PROPERTY_TYPE_LOOKUP.getTableName() ) )
                        .with( QueryBuilder.set( CommonColumns.FQN.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.deletePropertyTypeLookup = session
                .prepare( QueryBuilder.delete().from( keyspace, Tables.PROPERTY_TYPE_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.getPropertyTypeForTypename = session
                .prepare( QueryBuilder.select().from( keyspace, Tables.PROPERTY_TYPE_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.insertEntityTypeLookup = session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.ENTITY_TYPE_LOOKUP.getTableName() )
                        .value( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.FQN.cql(), QueryBuilder.bindMarker() ) );

        this.updateEntityTypeLookup = session
                .prepare( ( QueryBuilder.update( keyspace, Tables.ENTITY_TYPE_LOOKUP.getTableName() ) )
                        .with( QueryBuilder.set( CommonColumns.FQN.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.deleteEntityTypeLookup = session
                .prepare( QueryBuilder.delete().from( keyspace, Tables.ENTITY_TYPE_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.getEntityTypeForTypename = session
                .prepare( QueryBuilder.select().from( keyspace, Tables.ENTITY_TYPE_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.getTypenameForEntityId = session
                .prepare( QueryBuilder.select().from( keyspace, Tables.ENTITY_ID_TO_TYPE.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) ) );

        this.assignEntityToEntitySet = session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.ENTITY_SET_MEMBERS.getTableName() )
                        .value( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.PARTITION_INDEX.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) );

        this.entityTypeAddSchema = session
                .prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES.getTableName() )
                        .with( QueryBuilder.addAll( CommonColumns.SCHEMAS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.entityTypeRemoveSchema = session
                .prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES.getTableName() )
                        .with( QueryBuilder.removeAll( CommonColumns.SCHEMAS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.propertyTypeAddSchema = session
                .prepare( QueryBuilder.update( keyspace, Tables.PROPERTY_TYPES.getTableName() )
                        .with( QueryBuilder.addAll( CommonColumns.SCHEMAS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.propertyTypeRemoveSchema = session
                .prepare( QueryBuilder.update( keyspace, Tables.PROPERTY_TYPES.getTableName() )
                        .with( QueryBuilder.removeAll( CommonColumns.SCHEMAS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) ) );

        /**
         * Permissions for Entity Type
         */
        this.addPermissionsForEntityType = new HashMap<>();

        addPermissionsForEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS.getTableName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        addPermissionsForEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES_USERS_ACLS.getTableName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.setPermissionsForEntityType = new HashMap<>();

        setPermissionsForEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS.getTableName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        setPermissionsForEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES_USERS_ACLS.getTableName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForEntityType = new HashMap<>();

        getPermissionsForEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_TYPES_USERS_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );
        
        this.getPermissionsForEntityTypeByType = new HashMap<>();

        getPermissionsForEntityTypeByType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForEntityTypeByType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_TYPES_USERS_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.deleteRowFromEntityTypesAclsTable = new HashMap<>();

        deleteRowFromEntityTypesAclsTable.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteRowFromEntityTypesAclsTable.put( PrincipalType.USER,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_TYPES_USERS_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        /**
         * Permissions for Entity Set
         */

        this.addPermissionsForEntitySet = new HashMap<>();

        addPermissionsForEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_SETS_ROLES_ACLS.getTableName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        addPermissionsForEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_SETS_USERS_ACLS.getTableName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.setPermissionsForEntitySet = new HashMap<>();

        setPermissionsForEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_SETS_ROLES_ACLS.getTableName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        setPermissionsForEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_SETS_USERS_ACLS.getTableName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForEntitySet = new HashMap<>();

        getPermissionsForEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_ROLES_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_USERS_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForEntitySetBySet = new HashMap<>();

        this.getPermissionsForEntitySetBySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_ROLES_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForEntitySetBySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_USERS_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );
        
        this.deleteRowFromEntitySetsAclsTable = new HashMap<>();

        deleteRowFromEntitySetsAclsTable.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_SETS_ROLES_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteRowFromEntitySetsAclsTable.put( PrincipalType.USER,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_SETS_USERS_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        /**
         * Entity Set Owner updates
         */

        this.getOwnerForEntitySet = session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_OWNER.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) );

        this.getEntitySetsUserOwns = session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_OWNER_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) ) );

        this.updateOwnerForEntitySet = session
                .prepare( QueryBuilder.update( keyspace, Tables.ENTITY_SETS_OWNER.getTableName() )
                        .with( QueryBuilder.set( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) );

        this.updateOwnerLookupForEntitySet = session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.ENTITY_SETS_OWNER_LOOKUP.getTableName() )
                        .value( CommonColumns.USER.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) );

        this.deleteFromEntitySetOwnerTable = session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_SETS_OWNER.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) );

        this.deleteFromEntitySetOwnerLookupTable = session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_SETS_OWNER_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) );

        /**
         * Permissions for Property Types In Entity Types
         */

        this.addPermissionsForPropertyTypeInEntityType = new HashMap<>();

        addPermissionsForPropertyTypeInEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS.getTableName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        addPermissionsForPropertyTypeInEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS.getTableName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.setPermissionsForPropertyTypeInEntityType = new HashMap<>();

        setPermissionsForPropertyTypeInEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS.getTableName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        setPermissionsForPropertyTypeInEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS.getTableName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForPropertyTypeInEntityType = new HashMap<>();

        getPermissionsForPropertyTypeInEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForPropertyTypeInEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.deleteRowFromPropertyTypesInEntityTypesAclsTable = new HashMap<>();

        deleteRowFromPropertyTypesInEntityTypesAclsTable.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteRowFromPropertyTypesInEntityTypesAclsTable.put( PrincipalType.USER,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        /**
         * Permissions for Property Types In Entity Sets
         */

        this.addPermissionsForPropertyTypeInEntitySet = new HashMap<>();

        addPermissionsForPropertyTypeInEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS.getTableName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        addPermissionsForPropertyTypeInEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS.getTableName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.setPermissionsForPropertyTypeInEntitySet = new HashMap<>();

        setPermissionsForPropertyTypeInEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS.getTableName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        setPermissionsForPropertyTypeInEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS.getTableName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForPropertyTypeInEntitySet = new HashMap<>();

        getPermissionsForPropertyTypeInEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForPropertyTypeInEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.deleteRowFromPropertyTypesInEntitySetsAclsTable = new HashMap<>();

        deleteRowFromPropertyTypesInEntitySetsAclsTable.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteRowFromPropertyTypesInEntitySetsAclsTable.put( PrincipalType.USER,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        /**
         * Acls Requests
         */
        this.insertAclsRequest = new HashMap<>();

        insertAclsRequest.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.ROLES_ACLS_REQUESTS.getTableName() )
                        .value( CommonColumns.USER.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) ) );

        insertAclsRequest.put( PrincipalType.USER, session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.USERS_ACLS_REQUESTS.getTableName() )
                        .value( CommonColumns.USER.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) ) );

        this.updateLookupForAclsRequest = new HashMap<>();

        updateLookupForAclsRequest.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.ROLES_ACLS_REQUESTS_LOOKUP.getTableName() )
                        .value( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.USER.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() ) ) );

        updateLookupForAclsRequest.put( PrincipalType.USER, session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.USERS_ACLS_REQUESTS_LOOKUP.getTableName() )
                        .value( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.USER.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() ) ) );

        this.deleteAclsRequest = new HashMap<>();

        deleteAclsRequest.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteAclsRequest.put( PrincipalType.USER, session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.deleteLookupForAclsRequest = new HashMap<>();

        deleteLookupForAclsRequest.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteLookupForAclsRequest.put( PrincipalType.USER, session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getAclsRequestsByUsername = new HashMap<>();

        getAclsRequestsByUsername.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) ) ) );

        getAclsRequestsByUsername.put( PrincipalType.USER, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) ) ) );
        
        this.getAclsRequestsByUsernameAndEntitySet = new HashMap<>();

        getAclsRequestsByUsernameAndEntitySet.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        getAclsRequestsByUsernameAndEntitySet.put( PrincipalType.USER, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );
        
        this.getAclsRequestsByEntitySet = new HashMap<>();

        getAclsRequestsByEntitySet.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        getAclsRequestsByEntitySet.put( PrincipalType.USER, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getAclsRequestById = new HashMap<>();

        getAclsRequestById.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() ) ) ) );

        getAclsRequestById.put( PrincipalType.USER, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() ) ) ) );
    }

    public String getKeyspace() {
        return keyspace;
    }

    public boolean createSchemaTableForAclId( UUID aclId ) {
        if ( createSchemasTableIfNotExists( keyspace, aclId, session ) ) {
            prepareSchemaQuery( aclId );
            return true;
        }
        return false;
    }

    public PreparedStatement getSchemaInsertStatement( UUID aclId ) {
        return schemaInsertStatements.get( aclId );
    }

    /**
     * Get prepared statement for loading schemas from the various tables based on aclId.
     *
     * @param aclId
     * @return
     */
    public PreparedStatement getSchemaStatement( UUID aclId ) {
        return schemaSelectStatements.get( aclId );
    }

    public PreparedStatement getSchemasInNamespaceStatement( UUID aclId ) {
        return schemaSelectAllInNamespaceStatements.get( aclId );
    }

    public PreparedStatement getAllSchemasStatement( UUID aclId ) {
        return schemaSelectAllStatements.get( aclId );
    }

    public PreparedStatement getSchemaUpsertStatement( UUID aclId ) {
        return schemaUpsertStatements.get( aclId );
    }

    public PreparedStatement getSchemaAddEntityTypeStatement( UUID aclId ) {
        return schemaAddEntityTypes.get( aclId );
    }

    public PreparedStatement getSchemaRemoveEntityTypeStatement( UUID aclId ) {
        return schemaRemoveEntityTypes.get( aclId );
    }

    public PreparedStatement getSchemaAddPropertyTypeStatement( UUID aclId ) {
        return schemaAddPropertyTypes.get( aclId );
    }

    public PreparedStatement getSchemaRemovePropertyTypeStatement( UUID aclId ) {
        return schemaRemovePropertyTypes.get( aclId );
    }

    public void registerSchema( Schema schema ) {
        Preconditions.checkArgument( schema.getEntityTypeFqns().size() == schema.getEntityTypes().size(),
                "Schema is out of sync." );
        schema.getEntityTypes().forEach( et -> {
            // TODO: Solve ID generation
            /*
             * While unlikely it's possible to have a UUID collision when creating an object. Two possible solutions:
             * (1) Use Hazelcast and perform a read prior to every write (2) Maintain a self-refreshing in-memory pool
             * of available UUIDs that shifts the reads to times when cassandra is under less stress. Option (2) with a
             * fall back to random UUID generation when pool is exhausted seems like an efficient bet.
             */
            putEntityTypeInsertStatement( et.getFullQualifiedName() );
            putEntityTypeUpdateStatement( et.getFullQualifiedName() );
            putEntityIdToTypeUpdateStatement( et.getFullQualifiedName() );
            et.getKey().forEach( fqn -> putPropertyIndexUpdateStatement( fqn ) );
        } );

        schema.getPropertyTypes().forEach( pt -> {
            putPropertyTypeUpdateStatement( pt.getFullQualifiedName() );
        } );
    }

    public void registerEntityTypesAndAssociatedPropertyTypes( EntityType entityType ) {
        putEntityTypeInsertStatement( entityType.getFullQualifiedName() );
        putEntityTypeUpdateStatement( entityType.getFullQualifiedName() );
        putEntityIdToTypeUpdateStatement( entityType.getFullQualifiedName() );
        entityType.getKey().forEach( fqn -> putPropertyIndexUpdateStatement( fqn ) );
        entityType.getProperties().forEach( fqn -> putPropertyTypeUpdateStatement( fqn ) );
    }

    public PreparedStatement getInsertEntityPreparedStatement( EntityType entityType ) {
        return getInsertEntityPreparedStatement( entityType.getFullQualifiedName() );
    }

    public PreparedStatement getInsertEntityPreparedStatement( FullQualifiedName fqn ) {
        return entityTypeInsertStatements.get( fqn );
    }

    public PreparedStatement getUpdateEntityPreparedStatement( EntityType entityType ) {
        return getUpdateEntityPreparedStatement( entityType.getFullQualifiedName() );
    }

    public PreparedStatement getUpdateEntityPreparedStatement( FullQualifiedName fqn ) {
        return entityTypeUpdateStatements.get( fqn );
    }

    public PreparedStatement getUpdateEntityIdTypenamePreparedStatement( FullQualifiedName fqn ) {
        return entityIdToTypeUpdateStatements.get( fqn );
    }

    public PreparedStatement getUpdatePropertyPreparedStatement( PropertyType propertyType ) {
        return getUpdatePropertyPreparedStatement( propertyType.getFullQualifiedName() );
    }

    public PreparedStatement getUpdatePropertyPreparedStatement( FullQualifiedName fqn ) {
        return propertyTypeUpdateStatements.get( fqn );
    }

    public PreparedStatement getUpdatePropertyIndexPreparedStatement( FullQualifiedName fqn ) {
        return this.propertyIndexUpdateStatements.get( fqn );
    }

    public PreparedStatement getCountPropertyStatement() {
        return countProperty;
    }

    public PreparedStatement getCountEntityTypesStatement() {
        return countEntityTypes;
    }

    public PreparedStatement getCountEntitySetsStatement() {
        return countEntitySets;
    }

    public void createEntityTypeTable( EntityType entityType, Map<FullQualifiedName, PropertyType> keyPropertyTypes ) {
        // Ensure that type name doesn't exist
        String entityTableQuery;

        do {
            entityTableQuery = Queries.createEntityTable( keyspace, getTablenameForEntityType( entityType ) );
        } while ( !Util.wasLightweightTransactionApplied( session.execute( entityTableQuery ) ) );

        entityType.getKey().forEach( fqn -> {
            // TODO: Use elasticsearch for maintaining index instead of maintaining in Cassandra.
            /*
             * This makes sure that index tables are created if they do not exist. Other entity types may already be
             * using this property type as a key.
             */
            PropertyType keyPropertyType = keyPropertyTypes.get( fqn );
            String typename = keyPropertyType.getTypename();
            Preconditions.checkArgument( StringUtils.isNotBlank( typename ),
                    "Typename for key property type cannot be null" );
            session.execute( Queries.createPropertyTableQuery( keyspace,
                    getTablenameForPropertyIndex( keyPropertyType ),
                    cc -> CassandraEdmMapping.getCassandraType( keyPropertyType.getDatatype() ) ) );

            putPropertyIndexUpdateStatement( fqn );
        } );
        putEntityTypeInsertStatement( entityType.getFullQualifiedName() );
        putEntityTypeUpdateStatement( entityType.getFullQualifiedName() );
        putEntityIdToTypeUpdateStatement( entityType.getFullQualifiedName() );
        // Loop until table creation succeeds.
    }

    public void createPropertyTypeTable( PropertyType propertyType ) {
        String propertyTableQuery;
        DataType valueType = CassandraEdmMapping.getCassandraType( propertyType.getDatatype() );
        do {
            propertyTableQuery = Queries.createPropertyTableQuery( keyspace,
                    getTablenameForPropertyValuesOfType( propertyType ),
                    cc -> valueType );
            // Loop until table creation succeeds.
        } while ( !Util.wasLightweightTransactionApplied( session.execute( propertyTableQuery ) ) );

        putPropertyTypeUpdateStatement( propertyType.getFullQualifiedName() );
    }

    public void deleteEntityTypeTable( String namespace, String entityName ) {
        // We should mark tables for deletion-- we lose historical information if we hard delete properties.
        /*
         * Use Accessor interface to look up objects and retrieve typename corresponding to table to delete.
         */
        throw new NotImplementedException( "Blame MTR" );// TODO
    }

    public void deletePropertyTypeTable( String namespace, String propertyName ) {
        throw new NotImplementedException( "Blame MTR" );// TODO
    }

    /**
     * Operations on Typename to (user-friendly) FullQualifiedName Lookup Tables
     */

    public void insertToPropertyTypeLookupTable( PropertyType propertyType ) {
        session.execute(
                insertPropertyTypeLookup.bind( propertyType.getTypename(), propertyType.getFullQualifiedName() ) );
    }

    public void updatePropertyTypeLookupTable( PropertyType propertyType ) {
        session.execute(
                updatePropertyTypeLookup.bind( propertyType.getTypename(), propertyType.getFullQualifiedName() ) );
        // TODO: reorder binding?
    }

    public void deleteFromPropertyTypeLookupTable( PropertyType propertyType ) {
        FullQualifiedName fqn = getPropertyTypeForTypename( propertyType.getTypename() );
        if ( fqn != null ) {
            session.execute(
                    deletePropertyTypeLookup.bind( propertyType.getTypename() ) );
        }
    }

    public void insertToEntityTypeLookupTable( EntityType entityType ) {
        session.execute(
                insertEntityTypeLookup.bind( entityType.getTypename(), entityType.getFullQualifiedName() ) );
    }

    public void updateEntityTypeLookupTable( EntityType entityType ) {
        session.execute(
                updateEntityTypeLookup.bind( entityType.getTypename(), entityType.getFullQualifiedName() ) );
        // TODO: reorder binding?
    }

    public void deleteFromEntityTypeLookupTable( EntityType entityType ) {
        FullQualifiedName fqn = getEntityTypeForTypename( entityType.getTypename() );
        if ( fqn != null ) {
            session.execute(
                    deleteEntityTypeLookup.bind( entityType.getTypename() ) );
        }
    }

    /**
     * Name getters for Entity Type
     */

    public String getTypenameForEntityType( EntityType entityType ) {
        return getTypenameForEntityType( entityType.getNamespace(), entityType.getName() );
    }

    public String getTypenameForEntityType( FullQualifiedName fullQualifiedName ) {
        return getTypenameForEntityType( fullQualifiedName.getNamespace(), fullQualifiedName.getName() );
    }

    public String getTypenameForEntityType( String namespace, String name ) {
        return Util.transformSafely( session.execute( this.getTypenameForEntityType.bind( namespace, name ) ).one(),
                r -> r.getString( CommonColumns.TYPENAME.cql() ) );
    }

    // this shall only be called the first time when entitySet is created
    public String getTypenameForEntitySet( EntitySet entitySet ) {
        return getTypenameForEntityType( entitySet.getType() );
    }

    public String getTablenameForEntityType( EntityType entityType ) {
        // `If type name is provided then just directly return the table name
        final String typename = entityType.getTypename();
        if ( StringUtils.isNotBlank( typename ) ) {
            return getTablenameForEntityTypeFromTypenameAndAclId( ACLs.EVERYONE_ACL, typename );
        }
        return getTablenameForEntityType( entityType.getFullQualifiedName() );
    }

    public String getTablenameForEntityType( FullQualifiedName fqn ) {
        return getTablenameForEntityTypeFromTypenameAndAclId( ACLs.EVERYONE_ACL, getTypenameForEntityType( fqn ) );
    }

    public String getTablenameForEntityTypeFromTypenameAndAclId( UUID aclId, String typename ) {
        return getTablename( TableType.entity_, aclId, typename );
    }

    public Boolean assignEntityToEntitySet( UUID entityId, String typename, String name ) {
        SecureRandom random = new SecureRandom();
        return Util.wasLightweightTransactionApplied(
                session.execute(
                        assignEntityToEntitySet.bind(
                                typename,
                                name,
                                Arrays.toString( random.generateSeed( 1 ) ),
                                entityId ) ) );
    }

    public Boolean assignEntityToEntitySet( UUID entityId, EntitySet es ) {
        String typename = getTypenameForEntitySet( es );
        SecureRandom random = new SecureRandom();
        return Util.wasLightweightTransactionApplied(
                session.execute(
                        assignEntityToEntitySet.bind(
                                typename,
                                es.getName(),
                                Arrays.toString( random.generateSeed( 1 ) ),
                                entityId ) ) );
    }

    public void entityTypeAddSchema( EntityType entityType, String schemaNamespace, String schemaName ) {
        entityTypeAddSchema( entityType.getNamespace(),
                entityType.getName(),
                new FullQualifiedName( schemaNamespace, schemaName ) );
    }

    public void entityTypeAddSchema( EntityType entityType, FullQualifiedName schemaFqn ) {
        entityTypeAddSchema( entityType.getNamespace(), entityType.getName(), schemaFqn );
    }

    public void entityTypeAddSchema( FullQualifiedName entityTypeFqn, String schemaNamespace, String schemaName ) {
        entityTypeAddSchema( entityTypeFqn.getNamespace(),
                entityTypeFqn.getName(),
                new FullQualifiedName( schemaNamespace, schemaName ) );
    }

    public void entityTypeAddSchema( String entityTypeNamespace, String entityTypeName, FullQualifiedName schemaFqn ) {
        session.execute(
                entityTypeAddSchema.bind(
                        ImmutableSet.of( schemaFqn ),
                        entityTypeNamespace,
                        entityTypeName ) );
    }

    public void entityTypeRemoveSchema( EntityType entityType, String schemaNamespace, String schemaName ) {
        entityTypeRemoveSchema( entityType, new FullQualifiedName( schemaNamespace, schemaName ) );
    }

    public void entityTypeRemoveSchema( EntityType entityType, FullQualifiedName schemaFqn ) {
        session.execute(
                entityTypeRemoveSchema.bind(
                        ImmutableSet.of( schemaFqn ),
                        entityType.getNamespace(),
                        entityType.getName() ) );
    }

    public String getTypenameForEntityId( UUID entityId ) {
        return Util.transformSafely( session.execute( this.getTypenameForEntityId.bind( entityId ) ).one(),
                r -> r.getString( CommonColumns.TYPENAME.cql() ) );
    }

    /*************************
     * Getters for Property Type
     *************************/

    public String getTypenameForPropertyType( PropertyType propertyType ) {
        return getTypenameForPropertyType( propertyType.getNamespace(), propertyType.getName() );
    }

    public String getTypenameForPropertyType( FullQualifiedName fullQualifiedName ) {
        return getTypenameForPropertyType( fullQualifiedName.getNamespace(), fullQualifiedName.getName() );
    }

    private String getTypenameForPropertyType( String namespace, String name ) {
        return Util.transformSafely( session.execute( this.getTypenameForPropertyType.bind( namespace, name ) ).one(),
                r -> r.getString( CommonColumns.TYPENAME.cql() ) );
    }

    public String getTablenameForPropertyValuesOfType( PropertyType propertyType ) {
        // If type name is provided then just directly return the table name
        final String typename = propertyType.getTypename();
        if ( StringUtils.isNotBlank( typename ) ) {
            return getTablenameForPropertyValuesFromTypenameAndAclId( ACLs.EVERYONE_ACL, typename );
        }
        return getTablenameForPropertyValuesOfType( propertyType.getFullQualifiedName() );
    }

    public String getTablenameForPropertyValuesOfType( FullQualifiedName propertyFqn ) {
        return getTablenameForPropertyValuesFromTypenameAndAclId( ACLs.EVERYONE_ACL,
                getTypenameForPropertyType( propertyFqn ) );
    }

    public String getTablenameForPropertyValuesFromTypenameAndAclId( UUID aclId, String typename ) {
        return getTablename( TableType.property_, aclId, typename );
    }

    public String getTablenameForPropertyIndex( PropertyType propertyType ) {
        final String typename = propertyType.getTypename();
        if ( StringUtils.isNotBlank( typename ) ) {
            return getTablenameForPropertyIndexFromTypenameAndAclId( ACLs.EVERYONE_ACL, typename );
        }
        return getTablenameForPropertyIndexOfType( propertyType.getFullQualifiedName() );
    }

    public String getTablenameForPropertyIndexOfType( FullQualifiedName propertyFqn ) {
        return getTablenameForPropertyIndexFromTypenameAndAclId( ACLs.EVERYONE_ACL,
                getTypenameForPropertyType( propertyFqn ) );
    }

    public String getTablenameForPropertyIndexFromTypenameAndAclId( UUID aclId, String typename ) {
        return getTablename( TableType.index_, aclId, typename );
    }

    public void propertyTypeAddSchema( FullQualifiedName propertyTypeFqn, String schemaNamespace, String schemaName ) {
        propertyTypeAddSchema( propertyTypeFqn.getNamespace(),
                propertyTypeFqn.getName(),
                new FullQualifiedName( schemaNamespace, schemaName ) );
    }

    public void propertyTypeAddSchema(
            String propertyTypeNamespace,
            String propertyTypeName,
            FullQualifiedName schemaFqn ) {
        session.execute(
                propertyTypeAddSchema.bind(
                        ImmutableSet.of( schemaFqn ),
                        propertyTypeNamespace,
                        propertyTypeName ) );
    }

    public void propertyTypeRemoveSchema( FullQualifiedName propertyType, String schemaNamespace, String schemaName ) {
        propertyTypeRemoveSchema( propertyType.getNamespace(),
                propertyType.getName(),
                new FullQualifiedName( schemaNamespace, schemaName ) );
    }

    public void propertyTypeRemoveSchema(
            String propertyTypeNamespace,
            String propertyTypeName,
            FullQualifiedName schemaFqn ) {
        session.execute(
                propertyTypeRemoveSchema.bind(
                        ImmutableSet.of( schemaFqn ),
                        propertyTypeNamespace,
                        propertyTypeName ) );
    }

    public Map<String, FullQualifiedName> getPropertyTypesForTypenames( Iterable<String> typenames ) {
        return Maps.toMap( typenames, this::getPropertyTypeForTypename );
    }

    public FullQualifiedName getPropertyTypeForTypename( String typename ) {
        return Util.transformSafely( session.execute( this.getPropertyTypeForTypename.bind( typename ) ).one(),
                r -> new FullQualifiedName( r.getString( CommonColumns.FQN.cql() ) ) );
    }

    public Map<String, FullQualifiedName> getEntityTypesForTypenames( Iterable<String> typenames ) {
        return Maps.toMap( typenames, this::getEntityTypeForTypename );
    }

    public FullQualifiedName getEntityTypeForTypename( String typename ) {
        return Util.transformSafely( session.execute( this.getEntityTypeForTypename.bind( typename ) ).one(),
                r -> new FullQualifiedName( r.getString( CommonColumns.FQN.cql() ) ) );
    }

    private void putEntityIdToTypeUpdateStatement( FullQualifiedName entityTypeFqn ) {
        entityIdToTypeUpdateStatements.put( entityTypeFqn,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.ENTITY_ID_TO_TYPE.getTableName() )
                        .with( QueryBuilder.set( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) ) ) );
    }

    private void putEntityTypeInsertStatement( FullQualifiedName entityTypeFqn ) {
        entityTypeInsertStatements.put( entityTypeFqn,
                session.prepare( QueryBuilder
                        .insertInto( keyspace, getTablenameForEntityType( entityTypeFqn ) )
                        .value( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.CLOCK.cql(),
                                QueryBuilder.fcall( "toTimestamp", QueryBuilder.now() ) )
                        .value( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITY_SETS.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.SYNCIDS.cql(), QueryBuilder.bindMarker() ) ) );
    }

    private void putEntityTypeUpdateStatement( FullQualifiedName entityTypeFqn ) {
        entityTypeUpdateStatements.put( entityTypeFqn,
                session.prepare( QueryBuilder
                        .update( keyspace, getTablenameForEntityType( entityTypeFqn ) )
                        .with( QueryBuilder.set( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.addAll( CommonColumns.ENTITY_SETS.cql(),
                                QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.appendAll( CommonColumns.SYNCIDS.cql(),
                                QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() ) ) ) );
    }

    private void putPropertyTypeUpdateStatement( FullQualifiedName propertyTypeFqn ) {
        // The preparation process re-orders the bind markers. Below they are set according to the order that they get
        // mapped to
        propertyTypeUpdateStatements.put( propertyTypeFqn,
                session.prepare( QueryBuilder
                        .update( keyspace, getTablenameForPropertyValuesOfType( propertyTypeFqn ) )
                        .with( QueryBuilder.appendAll( CommonColumns.SYNCIDS.cql(),
                                QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.VALUE.cql(), QueryBuilder.bindMarker() ) ) ) );

    }

    private void putPropertyIndexUpdateStatement( FullQualifiedName propertyTypeFqn ) {
        // The preparation process re-orders the bind markers. Below they are set according to the order that they get
        // mapped to
        propertyIndexUpdateStatements.put( propertyTypeFqn,
                session.prepare( QueryBuilder
                        .update( keyspace, getTablenameForPropertyIndexOfType( propertyTypeFqn ) )
                        .with( QueryBuilder.appendAll( CommonColumns.SYNCIDS.cql(),
                                QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.VALUE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) ) ) );
    }

    private void prepareSchemaQueries() {
        Set<UUID> aclIds = getAclsAppliedToSchemas();
        aclIds.forEach( this::prepareSchemaQuery );
    }

    private void prepareSchemaQuery( UUID aclId ) {
        String table = getTablenameForSchemaWithAclId( aclId );
        schemaSelectAllStatements.put( aclId, session.prepare( Queries.getAllSchemasQuery( keyspace, table ) ) );
        schemaSelectAllInNamespaceStatements.put( aclId,
                session.prepare( Queries.getAllSchemasInNamespaceQuery( keyspace, table ) ) );
        schemaSelectStatements.put( aclId, session.prepare( Queries.getSchemaQuery( keyspace, table ) ) );
        schemaInsertStatements.put( aclId, session.prepare( Queries.insertSchemaQueryIfNotExists( keyspace, table ) ) );
        schemaUpsertStatements.put( aclId, session.prepare( Queries.insertSchemaQuery( keyspace, table ) ) );
        schemaAddEntityTypes.putIfAbsent( aclId, session.prepare( Queries.addEntityTypesToSchema( keyspace, table ) ) );
        schemaRemoveEntityTypes.putIfAbsent( aclId,
                session.prepare( Queries.removeEntityTypesToSchema( keyspace, table ) ) );
        schemaAddPropertyTypes.putIfAbsent( aclId,
                session.prepare( Queries.addPropertyTypesToSchema( keyspace, table ) ) );
        schemaRemovePropertyTypes.putIfAbsent( aclId,
                session.prepare( Queries.removePropertyTypesFromSchema( keyspace, table ) ) );
    }

    private Set<UUID> getAclsAppliedToSchemas() {
        return ImmutableSet.of( ACLs.EVERYONE_ACL );
    }

    private void initCoreTables( String keyspace, Session session ) {
        createKeyspaceSparksIfNotExists( keyspace, session );
        createAclsTableIfNotExists( keyspace, session );
        createEntityTypesTableIfNotExists( keyspace, session );
        createPropertyTypesTableIfNotExists( keyspace, session );
        createEntitySetsTableIfNotExists( keyspace, session );
        createEntitySetMembersTableIfNotExists( keyspace, session );
        createPropertyTypeLookupTableIfNotExists( keyspace, session );
        createEntityTypeLookupTableIfNotExists( keyspace, session );
        createEntityIdTypenameTableIfNotExists( keyspace, session );
        // TODO: Remove this once everyone ACL is baked in.
        createSchemaTableForAclId( ACLs.EVERYONE_ACL );
    }

    public static String getTablenameForSchemaWithAclId( UUID aclId ) {
        return getTablename( TableType.schema_, aclId, "" );
    }

    public static String getTablename( TableType tableType, UUID aclId, String suffix ) {
        return tableType.name() + Util.toUnhyphenatedString( aclId ) + "_" + suffix;
    }

    public static String generateTypename() {
        return RandomStringUtils.randomAlphanumeric( 24 ).toLowerCase();
    }

    public static String getNameForDefaultEntitySet( String typename ) {
        return typename + "_" + typename;
    }

    /**************
     * Table Creators
     **************/

    private static void createKeyspaceSparksIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.CREATE_KEYSPACE );
    }

    private static void createAclsTableIfNotExists( String keyspace, Session session ) {
        createEntityTypesAclsTables( keyspace, session );
        createEntitySetsAclsTables( keyspace, session );
        createPropertyTypesInEntityTypesAclsTables( keyspace, session );
        createPropertyTypesInEntitySetsAclsTables( keyspace, session );

        session.execute( Queries.createSchemasAclsTableQuery( keyspace ) );

        session.execute( Queries.createRolesAclsRequestsTableQuery( keyspace ) );
        session.execute( Queries.indexEntitySetOnRolesAclsRequestsTableQuery( keyspace ) );
        session.execute( Queries.createRolesAclsRequestsLookupTableQuery( keyspace ) );

        session.execute( Queries.createUsersAclsRequestsTableQuery( keyspace ) );
        session.execute( Queries.indexEntitySetOnUsersAclsRequestsTableQuery( keyspace ) );
        session.execute( Queries.createUsersAclsRequestsLookupTableQuery( keyspace ) );
    }

    private static void createEntityTypesAclsTables( String keyspace, Session session ) {
        session.execute( Queries.createEntityTypesRolesAclsTableQuery( keyspace ) );
        session.execute( Queries.indexEntityTypeOnEntityTypesRolesAclsTableQuery( keyspace ) );
        session.execute( Queries.createEntityTypesUsersAclsTableQuery( keyspace ) );
        session.execute( Queries.indexEntityTypeOnEntityTypesUsersAclsTableQuery( keyspace ) );
    }

    private static void createEntitySetsAclsTables( String keyspace, Session session ) {
        session.execute( Queries.createEntitySetsRolesAclsTableQuery( keyspace ) );
        session.execute( Queries.indexEntitySetOnEntitySetsRolesAclsTableQuery( keyspace ) );
        session.execute( Queries.createEntitySetsUsersAclsTableQuery( keyspace ) );
        session.execute( Queries.indexEntitySetOnEntitySetsUsersAclsTableQuery( keyspace ) );

        session.execute( Queries.createEntitySetsOwnerTableQuery( keyspace ) );
        session.execute( Queries.createEntitySetsOwnerLookupTableQuery( keyspace ) );
    }

    private static void createPropertyTypesInEntityTypesAclsTables( String keyspace, Session session ) {
        session.execute( Queries.createPropertyTypesInEntityTypesRolesAclsTableQuery( keyspace ) );
        session.execute( Queries.indexEntityTypeOnPropertyTypesInEntityTypesRolesAclsTableQuery( keyspace ) );
        session.execute( Queries.createPropertyTypesInEntityTypesUsersAclsTableQuery( keyspace ) );
        session.execute( Queries.indexEntityTypeOnPropertyTypesInEntityTypesUsersAclsTableQuery( keyspace ) );
    }

    private static void createPropertyTypesInEntitySetsAclsTables( String keyspace, Session session ) {
        session.execute( Queries.createPropertyTypesInEntitySetsRolesAclsTableQuery( keyspace ) );
        session.execute( Queries.indexEntitySetOnPropertyTypesInEntitySetsRolesAclsTableQuery( keyspace ) );
        session.execute( Queries.createPropertyTypesInEntitySetsUsersAclsTableQuery( keyspace ) );
        session.execute( Queries.indexEntitySetOnPropertyTypesInEntitySetsUsersAclsTableQuery( keyspace ) );
    }

    private static boolean createSchemasTableIfNotExists( String keyspace, UUID aclId, Session session ) {
        return Util.wasLightweightTransactionApplied( session
                .execute( Queries.createSchemasTableQuery( keyspace, getTablenameForSchemaWithAclId( aclId ) ) ) );
    }

    private static void createEntitySetsTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateEntitySetsTableQuery( keyspace ) );
        session.execute( Queries.CREATE_INDEX_ON_NAME );
    }

    private static void createEntitySetMembersTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateEntitySetMembersTableQuery( keyspace ) );
    }

    private static void createEntityIdTypenameTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateEntityIdToTypenameTableQuery( keyspace ) );
    }

    private static void createEntityTypesTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateEntityTypesTableQuery( keyspace ) );
    }

    private void createPropertyTypesTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreatePropertyTypesTableQuery( keyspace ) );
    }

    private void createPropertyTypeLookupTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreatePropertyTypeLookupTableQuery( keyspace ) );
    }

    private void createEntityTypeLookupTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateEntityTypeLookupTableQuery( keyspace ) );
    }

    /**************
     * Acl Operations
     **************/

    public EnumSet<Permission> getRolePermissionsForEntityType( String role, FullQualifiedName entityTypeFqn ) {
        if ( role == Constants.ROLE_ADMIN ) {
            return EnumSet.allOf( Permission.class );
        } else {
            String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
            Row row = session.execute( this.getPermissionsForEntityType
                    .get( PrincipalType.ROLE )
                    .bind( role, entityTypeTypename ) )
                    .one();
            if ( row != null ) {
                return row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
            } else {
                // Property Type not found in Acl table; would mean no permission for now
                return EnumSet.noneOf( Permission.class );
            }
        }
    }

    public EnumSet<Permission> getUserPermissionsForEntityType( String user, FullQualifiedName entityTypeFqn ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        Row row = session.execute( this.getPermissionsForEntityType
                .get( PrincipalType.USER )
                .bind( user, entityTypeTypename ) )
                .one();
        if ( row != null ) {
            return row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
        } else {
            // Property Type not found in Acl table; would mean no permission for now
            return EnumSet.noneOf( Permission.class );
        }
    }

    public void addRolePermissionsForEntityType(
            String role,
            FullQualifiedName entityTypeFqn,
            Set<Permission> permissions ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        session.execute( this.addPermissionsForEntityType
                .get( PrincipalType.ROLE )
                .bind( permissions, role, entityTypeTypename ) );
    }

    public void addUserPermissionsForEntityType(
            String user,
            FullQualifiedName entityTypeFqn,
            Set<Permission> permissions ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        session.execute( this.addPermissionsForEntityType
                .get( PrincipalType.USER )
                .bind( permissions, user, entityTypeTypename ) );
    }

    public void setRolePermissionsForEntityType(
            String role,
            FullQualifiedName entityTypeFqn,
            Set<Permission> permissions ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        session.execute( this.setPermissionsForEntityType
                .get( PrincipalType.ROLE )
                .bind( permissions, role, entityTypeTypename ) );
    }

    public void setUserPermissionsForEntityType(
            String user,
            FullQualifiedName entityTypeFqn,
            Set<Permission> permissions ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        session.execute( this.setPermissionsForEntityType
                .get( PrincipalType.USER )
                .bind( permissions, user, entityTypeTypename ) );
    }

    public void deleteEntityTypeFromEntityTypesAclsTable( FullQualifiedName entityTypeFqn ) {
        // TODO rewrite this again
        
    }
    
    private void deleteEntityTypeFromEntityTypesAclsTable( PrincipalType type, FullQualifiedName entityTypeFqn ){
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        ResultSet rs = session.execute( this.getPermissionsForEntityTypeByType.get( type ).bind( entityTypeFqn ) );
    }

    public void deleteRoleAndTypeFromEntityTypesAclsTable( String role, FullQualifiedName entityTypeFqn ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        session.execute( deleteRowFromEntityTypesAclsTable.get( PrincipalType.ROLE )
                .bind( role, entityTypeTypename ) );
    }

    public void deleteUserAndTypeFromEntityTypesAclsTable( String user, FullQualifiedName entityTypeFqn ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        session.execute( deleteRowFromEntityTypesAclsTable.get( PrincipalType.USER )
                .bind( user, entityTypeTypename ) );
    }

    public EnumSet<Permission> getRolePermissionsForEntitySet( String role, String entitySetName ) {
        if ( role == Constants.ROLE_ADMIN ) {
            return EnumSet.allOf( Permission.class );
        } else {
            Row row = session.execute( this.getPermissionsForEntitySet
                    .get( PrincipalType.ROLE )
                    .bind( role, entitySetName ) )
                    .one();
            if ( row != null ) {
                return row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
            } else {
                // Property Type not found in Acl table; would mean no permission for now
                // TODO: change this, if you want default permission of a group
                return EnumSet.noneOf( Permission.class );
            }
        }
    }

    public EnumSet<Permission> getUserPermissionsForEntitySet( String user, String entitySetName ) {
        Row row = session.execute( this.getPermissionsForEntitySet
                .get( PrincipalType.USER )
                .bind( user, entitySetName ) )
                .one();
        if ( row != null ) {
            return row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
        } else {
            // Property Type not found in Acl table; would mean no permission for now
            // TODO: change this, if you want default permission of a group
            return EnumSet.noneOf( Permission.class );
        }
    }

    public void addRolePermissionsForEntitySet( String role, String entitySetName, Set<Permission> permissions ) {
        session.execute( this.addPermissionsForEntitySet
                .get( PrincipalType.ROLE )
                .bind( permissions, role, entitySetName ) );
    }

    public void addUserPermissionsForEntitySet( String user, String entitySetName, Set<Permission> permissions ) {
        session.execute( this.addPermissionsForEntitySet
                .get( PrincipalType.USER )
                .bind( permissions, user, entitySetName ) );
    }

    public void setRolePermissionsForEntitySet( String role, String entitySetName, Set<Permission> permissions ) {
        session.execute( this.setPermissionsForEntitySet
                .get( PrincipalType.ROLE )
                .bind( permissions, role, entitySetName ) );
    }

    public void setUserPermissionsForEntitySet( String user, String entitySetName, Set<Permission> permissions ) {
        session.execute( this.setPermissionsForEntitySet
                .get( PrincipalType.USER )
                .bind( permissions, user, entitySetName ) );
    }

    public void deleteEntitySetFromEntitySetsAclsTable( String entitySetName ) {
        // TODO rewrite this
    }

    public void deleteRoleAndSetFromEntitySetsAclsTable( String role, String entitySetName ) {
        session.execute( deleteRowFromEntitySetsAclsTable.get( PrincipalType.ROLE )
                .bind( role, entitySetName ) );
    }

    public void deleteUserAndSetFromEntitySetsAclsTable( String user, String entitySetName ) {
        session.execute( deleteRowFromEntitySetsAclsTable.get( PrincipalType.USER )
                .bind( user, entitySetName ) );
    }

    public ResultSet getRoleAclsForEntitySet( String entitySetName ) {
        return session.execute( getPermissionsForEntitySetBySet.get( PrincipalType.ROLE ).bind( entitySetName ) );
    }

    public ResultSet getUserAclsForEntitySet( String entitySetName ) {
        return session.execute( getPermissionsForEntitySetBySet.get( PrincipalType.USER ).bind( entitySetName ) );
    }

    public EnumSet<Permission> getRolePermissionsForPropertyTypeInEntityType(
            String role,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        if ( role == Constants.ROLE_ADMIN ) {
            return EnumSet.allOf( Permission.class );
        } else {
            String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
            String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );
            Row row = session.execute( this.getPermissionsForPropertyTypeInEntityType
                    .get( PrincipalType.ROLE )
                    .bind( role, entityTypeTypename, propertyTypeTypename ) )
                    .one();
            if ( row != null ) {
                return row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
            } else {
                // Property Type not found in Acl table; would mean no permission for now
                // TODO: change this, if you want default permission of a group
                return EnumSet.noneOf( Permission.class );
            }
        }
    }

    public EnumSet<Permission> getUserPermissionsForPropertyTypeInEntityType(
            String user,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );
        Row row = session.execute( this.getPermissionsForPropertyTypeInEntityType
                .get( PrincipalType.USER )
                .bind( user, entityTypeTypename, propertyTypeTypename ) )
                .one();
        if ( row != null ) {
            return row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
        } else {
            // Property Type not found in Acl table; would mean no permission for now
            // TODO: change this, if you want default permission of a group
            return EnumSet.noneOf( Permission.class );
        }
    }

    public void addRolePermissionsForPropertyTypeInEntityType(
            String role,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.addPermissionsForPropertyTypeInEntityType
                .get( PrincipalType.ROLE )
                .bind( permissions, role, entityTypeTypename, propertyTypeTypename ) );
    }

    public void addUserPermissionsForPropertyTypeInEntityType(
            String user,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.addPermissionsForPropertyTypeInEntityType
                .get( PrincipalType.USER )
                .bind( permissions, user, entityTypeTypename, propertyTypeTypename ) );
    }

    public void setRolePermissionsForPropertyTypeInEntityType(
            String role,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.setPermissionsForPropertyTypeInEntityType
                .get( PrincipalType.ROLE )
                .bind( permissions, role, entityTypeTypename, propertyTypeTypename ) );
    }

    public void setUserPermissionsForPropertyTypeInEntityType(
            String user,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.setPermissionsForPropertyTypeInEntityType
                .get( PrincipalType.USER )
                .bind( permissions, user, entityTypeTypename, propertyTypeTypename ) );
    }

    public void deleteRoleAndTypesFromPropertyTypesInEntityTypesAclsTable(
            String role,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.deleteRowFromPropertyTypesInEntityTypesAclsTable
                .get( PrincipalType.ROLE )
                .bind( role, entityTypeTypename, propertyTypeTypename ) );
    }

    public void deleteUserAndTypesFromPropertyTypesInEntityTypesAclsTable(
            String user,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.deleteRowFromPropertyTypesInEntityTypesAclsTable
                .get( PrincipalType.USER )
                .bind( user, entityTypeTypename, propertyTypeTypename ) );
    }

    public void deleteTypesFromPropertyTypesInEntityTypesAclsTable(
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );
        // TODO: rewrite this
    }

    public void deleteTypesFromPropertyTypesInEntityTypesAclsTable( FullQualifiedName entityTypeFqn ) {
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        // TODO: rewrite this
    }

    public EnumSet<Permission> getRolePermissionsForPropertyTypeInEntitySet(
            String role,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        if ( role == Constants.ROLE_ADMIN ) {
            return EnumSet.allOf( Permission.class );
        } else {
            String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );
            Row row = session.execute( this.getPermissionsForPropertyTypeInEntitySet
                    .get( PrincipalType.ROLE )
                    .bind( role, entitySetName, propertyTypeTypename ) )
                    .one();
            if ( row != null ) {
                return row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
            } else {
                // Property Type not found in Acl table; would mean no permission for now
                // TODO: change this, if you want default permission of a group
                return EnumSet.noneOf( Permission.class );
            }
        }
    }

    public EnumSet<Permission> getUserPermissionsForPropertyTypeInEntitySet(
            String user,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );
        Row row = session.execute( this.getPermissionsForPropertyTypeInEntitySet
                .get( PrincipalType.USER )
                .bind( user, entitySetName, propertyTypeTypename ) )
                .one();
        if ( row != null ) {
            return row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
        } else {
            // Property Type not found in Acl table; would mean no permission for now
            // TODO: change this, if you want default permission of a group
            return EnumSet.noneOf( Permission.class );
        }
    }

    public void addRolePermissionsForPropertyTypeInEntitySet(
            String role,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.addPermissionsForPropertyTypeInEntitySet
                .get( PrincipalType.ROLE )
                .bind( permissions, role, entitySetName, propertyTypeTypename ) );
    }

    public void addUserPermissionsForPropertyTypeInEntitySet(
            String user,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.addPermissionsForPropertyTypeInEntitySet
                .get( PrincipalType.USER )
                .bind( permissions, user, entitySetName, propertyTypeTypename ) );
    }

    public void setRolePermissionsForPropertyTypeInEntitySet(
            String role,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.setPermissionsForPropertyTypeInEntitySet
                .get( PrincipalType.ROLE )
                .bind( permissions, role, entitySetName, propertyTypeTypename ) );
    }

    public void setUserPermissionsForPropertyTypeInEntitySet(
            String user,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.setPermissionsForPropertyTypeInEntitySet
                .get( PrincipalType.USER )
                .bind( permissions, user, entitySetName, propertyTypeTypename ) );
    }

    public void deleteRoleAndSetFromPropertyTypesInEntitySetsAclsTable(
            String role,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.deleteRowFromPropertyTypesInEntitySetsAclsTable
                .get( PrincipalType.ROLE )
                .bind( role, entitySetName, propertyTypeTypename ) );
    }

    public void deleteUserAndSetFromPropertyTypesInEntitySetsAclsTable(
            String user,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        session.execute( this.deleteRowFromPropertyTypesInEntitySetsAclsTable
                .get( PrincipalType.USER )
                .bind( user, entitySetName, propertyTypeTypename ) );
    }

    public void deleteSetAndTypeFromPropertyTypesInEntitySetsAclsTable(
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );
        // TODO: rewrite this
        
    }

    public void deleteSetFromPropertyTypesInEntitySetsAclsTable( String entitySetName ) {
        // TODO: rewrite this
    }

    /**
     * Entity Set Owner methods
     */
    public String getOwnerForEntitySet( String entitySetName ) {
        
        return Util.transformSafely( session.execute( this.getOwnerForEntitySet.bind( entitySetName ) ).one(),
                r -> r.getString( CommonColumns.USER.cql() ) );
    }

    public boolean checkIfUserIsOwnerOfEntitySet( String username, String entitySetName ) {
        String owner = getOwnerForEntitySet( entitySetName );
        
        if( owner != null && !owner.isEmpty() ){
            return username.equals( owner );
        }
        return false;
    }

    public Iterable<String> getEntitySetsUserOwns( String username ) {
        ResultSet rs = session.execute( this.getEntitySetsUserOwns.bind( username ) );
        return Iterables.transform( rs, row -> row.getString( CommonColumns.ENTITY_SET.cql() ) );
    }

    public void addOwnerForEntitySet( String entitySetName, String username ) {
        session.execute( this.updateOwnerForEntitySet.bind( username, entitySetName ) );
        session.execute( this.updateOwnerLookupForEntitySet.bind( username, entitySetName ) );
    }

    public void deleteFromEntitySetOwnerAndLookupTable( String entitySetName ) {
        String owner = getOwnerForEntitySet( entitySetName );

        session.execute( this.deleteFromEntitySetOwnerTable.bind( entitySetName ) );
        session.execute( this.deleteFromEntitySetOwnerLookupTable.bind( owner, entitySetName ) );
    }

    /**
     * Acl Requests methods
     */

    public boolean checkIfUserIsOwnerOfPermissionsRequest( String username, UUID id ) {
        String owner = getUsernameFromRequestId( id );
        if( owner != null && !owner.isEmpty() ){
            return username.equals( owner );
        }
        return false;
    }
    
    public void addPermissionsRequestForPropertyTypeInEntitySet(
            String username,
            Principal principal,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            EnumSet<Permission> permissions ) {
        UUID requestId = UUID.randomUUID();
        Instant timestamp = Instant.now();
        switch ( principal.getType() ) {
            case ROLE:
                session.execute( this.insertAclsRequest.get( PrincipalType.ROLE ).bind( username,
                        entitySetName,
                        timestamp,
                        requestId,
                        principal.getName(),
                        propertyTypeFqn,
                        permissions ) );
                session.execute( this.updateLookupForAclsRequest.get( PrincipalType.ROLE ).bind( requestId,
                        username,
                        entitySetName,
                        timestamp ) );
                break;
            case USER:
                session.execute( this.insertAclsRequest.get( PrincipalType.USER ).bind( username,
                        entitySetName,
                        timestamp,
                        requestId,
                        principal.getName(),
                        propertyTypeFqn,
                        permissions ) );
                session.execute( this.updateLookupForAclsRequest.get( PrincipalType.USER ).bind( requestId,
                        username,
                        entitySetName,
                        timestamp ) );
                break;
            default:
                break;
        }
    }

    public void removePermissionsRequestForEntitySet( UUID id ) {
        PrincipalType type;
        String username;
        String entitySetName;
        Instant timestamp;

        // Retrieve Row info by request id
        Row rowRole = session.execute( this.getAclsRequestById
                .get( PrincipalType.ROLE )
                .bind( id ) ).one();

        if ( rowRole != null ) {
            type = PrincipalType.ROLE;
            username = rowRole.getString( CommonColumns.USER.cql() );
            entitySetName = rowRole.getString( CommonColumns.ENTITY_SET.cql() );
            timestamp = rowRole.get( CommonColumns.CLOCK.cql(), InstantCodec.instance );
        } else {
            Row rowUser = session.execute( this.getAclsRequestById
                    .get( PrincipalType.USER )
                    .bind( id ) ).one();

            if ( rowUser != null ) {
                type = PrincipalType.USER;
                username = rowUser.getString( CommonColumns.USER.cql() );
                entitySetName = rowUser.getString( CommonColumns.ENTITY_SET.cql() );
                timestamp = rowUser.get( CommonColumns.CLOCK.cql(), InstantCodec.instance );
            } else {
                // TODO write custom handler
                throw new ResourceNotFoundException( "Permissions Request not found." );
            }
        }

        // Actual removal
        session.execute( this.deleteAclsRequest.get( type ).bind( username, entitySetName, timestamp, id ) );
        session.execute( this.deleteLookupForAclsRequest.get( type ).bind( id ) );
    }

    public String getEntitySetNameFromRequestId( UUID id ){
        // Retrieve Row info by request id
        Row rowRole = session.execute( this.getAclsRequestById
                .get( PrincipalType.ROLE )
                .bind( id ) ).one();
        
        if ( rowRole != null ) {
            return rowRole.getString( CommonColumns.ENTITY_SET.cql() );
        } else {
            Row rowUser = session.execute( this.getAclsRequestById
                    .get( PrincipalType.USER )
                    .bind( id ) ).one();

            if ( rowUser != null ) {
                return rowUser.getString( CommonColumns.ENTITY_SET.cql() );
            } else {
                return null;
            }
        }
    }
    
    public String getUsernameFromRequestId( UUID id ){
        // Retrieve Row info by request id
        Row rowRole = session.execute( this.getAclsRequestById
                .get( PrincipalType.ROLE )
                .bind( id ) ).one();
        
        if ( rowRole != null ) {
            return rowRole.getString( CommonColumns.USER.cql() );
        } else {
            Row rowUser = session.execute( this.getAclsRequestById
                    .get( PrincipalType.USER )
                    .bind( id ) ).one();

            if ( rowUser != null ) {
                return rowUser.getString( CommonColumns.USER.cql() );
            } else {
                return null;
            }
        }
    }
    
    public Iterable<Row> getAllReceivedRequestsForPermissionsOfUsername( PrincipalType type, String username ) {
        return StreamSupport.stream( getEntitySetsUserOwns( username ).spliterator(), false )
                .map( entitySetName -> getAllReceivedRequestsForPermissionsOfEntitySet( type, entitySetName ) )
                .flatMap( iterRow -> StreamSupport.stream( iterRow.spliterator(), false ) )
                .collect( Collectors.toList() );
    }

    public Iterable<Row> getAllReceivedRequestsForPermissionsOfEntitySet( PrincipalType type, String entitySetName ) {
        return session
                .execute( getAclsRequestsByEntitySet.get( type ).bind( entitySetName ) );
    }
    
    public Iterable<Row> getAllSentRequestsForPermissions( PrincipalType type, String username ) {
        return session
                .execute( getAclsRequestsByUsername.get( type ).bind( username ) );
    }
    
    public Iterable<Row> getAllSentRequestsForPermissions( PrincipalType type, String username, String entitySetName ) {
        return session
                .execute( getAclsRequestsByUsernameAndEntitySet.get( type ).bind( username, entitySetName ) );
    }
}
