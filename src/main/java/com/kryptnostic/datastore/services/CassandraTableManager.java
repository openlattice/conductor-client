package com.kryptnostic.datastore.services;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.edm.internal.DatastoreConstants;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.internal.TypePK;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.Queries;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;
import com.kryptnostic.datastore.util.Util;

public class CassandraTableManager {
    private static final Logger logger = LoggerFactory.getLogger( CassandraTableManager.class );

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
    private final IMap<FullQualifiedName, PropertyType>               propertyTypes;
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
    // WARNING: getPermissionsByTypes (Entity type and property type) enabled ALLOW FILTERING.
    // EntityType is an secondary index. Needs filtering for correct property types after.
    private final Map<PrincipalType, PreparedStatement>               getPermissionsForPropertyTypeInEntityTypeByTypes;
    private final Map<PrincipalType, PreparedStatement>               getPermissionsForPropertyTypeInEntityTypeByEntityType;
    private final Map<PrincipalType, PreparedStatement>               deleteRowFromPropertyTypesInEntityTypesAclsTable;
    private final Map<PrincipalType, PreparedStatement>               addPermissionsForPropertyTypeInEntitySet;
    private final Map<PrincipalType, PreparedStatement>               setPermissionsForPropertyTypeInEntitySet;
    private final Map<PrincipalType, PreparedStatement>               getPermissionsForPropertyTypeInEntitySet;
    // WARNING: getPermissionsBySetAndType (Entity set and property type) enabled ALLOW FILTERING.
    // EntitySet is an secondary index. Needs filtering for correct property types after.
    private final Map<PrincipalType, PreparedStatement>               getPermissionsForPropertyTypeInEntitySetBySetAndType;
    private final Map<PrincipalType, PreparedStatement>               getPermissionsForPropertyTypeInEntitySetBySet;
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
    private final Map<PrincipalType, PreparedStatement>               getAclsRequestsByUserId;
    private final Map<PrincipalType, PreparedStatement>               getAclsRequestsByUserIdAndEntitySet;
    private final Map<PrincipalType, PreparedStatement>               getAclsRequestsByEntitySet;
    private final Map<PrincipalType, PreparedStatement>               getAclsRequestById;

    public CassandraTableManager(
            HazelcastInstance hazelcastInstance,
            String keyspace,
            Session session ) {
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
        this.propertyTypes = hazelcastInstance.getMap( HazelcastMap.PROPERTY_TYPES.name() );
        initCoreTables( keyspace, session );
        prepareSchemaQueries();

        this.getTypenameForEntityType = session.prepare( QueryBuilder
                .select()
                .from( keyspace, Tables.ENTITY_TYPES.getName() )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(),
                        QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(),
                        QueryBuilder.bindMarker() ) ) );

        this.getTypenameForPropertyType = session.prepare( QueryBuilder
                .select()
                .from( keyspace, Tables.PROPERTY_TYPES.getName() )
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
                .prepare( QueryBuilder.insertInto( keyspace, Tables.PROPERTY_TYPE_LOOKUP.getName() )
                        .value( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.FQN.cql(), QueryBuilder.bindMarker() ) );

        this.updatePropertyTypeLookup = session
                .prepare( ( QueryBuilder.update( keyspace, Tables.PROPERTY_TYPE_LOOKUP.getName() ) )
                        .with( QueryBuilder.set( CommonColumns.FQN.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.deletePropertyTypeLookup = session
                .prepare( QueryBuilder.delete().from( keyspace, Tables.PROPERTY_TYPE_LOOKUP.getName() )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.getPropertyTypeForTypename = session
                .prepare( QueryBuilder.select().from( keyspace, Tables.PROPERTY_TYPE_LOOKUP.getName() )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.insertEntityTypeLookup = session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.ENTITY_TYPE_LOOKUP.getName() )
                        .value( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.FQN.cql(), QueryBuilder.bindMarker() ) );

        this.updateEntityTypeLookup = session
                .prepare( ( QueryBuilder.update( keyspace, Tables.ENTITY_TYPE_LOOKUP.getName() ) )
                        .with( QueryBuilder.set( CommonColumns.FQN.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.deleteEntityTypeLookup = session
                .prepare( QueryBuilder.delete().from( keyspace, Tables.ENTITY_TYPE_LOOKUP.getName() )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.getEntityTypeForTypename = session
                .prepare( QueryBuilder.select().from( keyspace, Tables.ENTITY_TYPE_LOOKUP.getName() )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.getTypenameForEntityId = session
                .prepare( QueryBuilder.select().from( keyspace, Tables.ENTITY_ID_TO_TYPE.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) ) );

        this.assignEntityToEntitySet = session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.ENTITY_SET_MEMBERS.getName() )
                        .value( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.PARTITION_INDEX.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) );

        this.entityTypeAddSchema = session
                .prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES.getName() )
                        .with( QueryBuilder.addAll( CommonColumns.SCHEMAS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.entityTypeRemoveSchema = session
                .prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES.getName() )
                        .with( QueryBuilder.removeAll( CommonColumns.SCHEMAS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.propertyTypeAddSchema = session
                .prepare( QueryBuilder.update( keyspace, Tables.PROPERTY_TYPES.getName() )
                        .with( QueryBuilder.addAll( CommonColumns.SCHEMAS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.propertyTypeRemoveSchema = session
                .prepare( QueryBuilder.update( keyspace, Tables.PROPERTY_TYPES.getName() )
                        .with( QueryBuilder.removeAll( CommonColumns.SCHEMAS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) ) );

        /**
         * Permissions for Entity Type
         */
        this.addPermissionsForEntityType = new HashMap<>();

        addPermissionsForEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS.getName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        addPermissionsForEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES_USERS_ACLS.getName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.setPermissionsForEntityType = new HashMap<>();

        setPermissionsForEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS.getName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        setPermissionsForEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_TYPES_USERS_ACLS.getName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForEntityType = new HashMap<>();

        getPermissionsForEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_TYPES_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForEntityTypeByType = new HashMap<>();

        getPermissionsForEntityTypeByType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForEntityTypeByType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_TYPES_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.deleteRowFromEntityTypesAclsTable = new HashMap<>();

        deleteRowFromEntityTypesAclsTable.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteRowFromEntityTypesAclsTable.put( PrincipalType.USER,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_TYPES_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        /**
         * Permissions for Entity Set
         */

        this.addPermissionsForEntitySet = new HashMap<>();

        addPermissionsForEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_SETS_ROLES_ACLS.getName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        addPermissionsForEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_SETS_USERS_ACLS.getName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.setPermissionsForEntitySet = new HashMap<>();

        setPermissionsForEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_SETS_ROLES_ACLS.getName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        setPermissionsForEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder.update( keyspace, Tables.ENTITY_SETS_USERS_ACLS.getName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForEntitySet = new HashMap<>();

        getPermissionsForEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForEntitySetBySet = new HashMap<>();

        this.getPermissionsForEntitySetBySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForEntitySetBySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.deleteRowFromEntitySetsAclsTable = new HashMap<>();

        deleteRowFromEntitySetsAclsTable.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_SETS_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteRowFromEntitySetsAclsTable.put( PrincipalType.USER,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_SETS_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        /**
         * Entity Set Owner updates
         */

        this.getOwnerForEntitySet = session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_OWNER.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) );

        this.getEntitySetsUserOwns = session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ENTITY_SETS_OWNER_LOOKUP.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) ) );

        this.updateOwnerForEntitySet = session
                .prepare( QueryBuilder.update( keyspace, Tables.ENTITY_SETS_OWNER.getName() )
                        .with( QueryBuilder.set( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) );

        this.updateOwnerLookupForEntitySet = session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.ENTITY_SETS_OWNER_LOOKUP.getName() )
                        .value( CommonColumns.USER.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) );

        this.deleteFromEntitySetOwnerTable = session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_SETS_OWNER.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) );

        this.deleteFromEntitySetOwnerLookupTable = session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ENTITY_SETS_OWNER_LOOKUP.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) );

        /**
         * Permissions for Property Types In Entity Types
         */

        this.addPermissionsForPropertyTypeInEntityType = new HashMap<>();

        addPermissionsForPropertyTypeInEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS.getName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        addPermissionsForPropertyTypeInEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS.getName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.setPermissionsForPropertyTypeInEntityType = new HashMap<>();

        setPermissionsForPropertyTypeInEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS.getName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        setPermissionsForPropertyTypeInEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS.getName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForPropertyTypeInEntityType = new HashMap<>();

        getPermissionsForPropertyTypeInEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForPropertyTypeInEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForPropertyTypeInEntityTypeByTypes = new HashMap<>();

        getPermissionsForPropertyTypeInEntityTypeByTypes.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS.getName() )
                        .allowFiltering()
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForPropertyTypeInEntityTypeByTypes.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS.getName() )
                        .allowFiltering()
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForPropertyTypeInEntityTypeByEntityType = new HashMap<>();

        getPermissionsForPropertyTypeInEntityTypeByEntityType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForPropertyTypeInEntityTypeByEntityType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.deleteRowFromPropertyTypesInEntityTypesAclsTable = new HashMap<>();

        deleteRowFromPropertyTypesInEntityTypesAclsTable.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteRowFromPropertyTypesInEntityTypesAclsTable.put( PrincipalType.USER,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        /**
         * Permissions for Property Types In Entity Sets
         */

        this.addPermissionsForPropertyTypeInEntitySet = new HashMap<>();

        addPermissionsForPropertyTypeInEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS.getName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        addPermissionsForPropertyTypeInEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS.getName() )
                        .with( QueryBuilder.addAll( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.setPermissionsForPropertyTypeInEntitySet = new HashMap<>();

        setPermissionsForPropertyTypeInEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS.getName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        setPermissionsForPropertyTypeInEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS.getName() )
                        .with( QueryBuilder.set( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForPropertyTypeInEntitySet = new HashMap<>();

        getPermissionsForPropertyTypeInEntitySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForPropertyTypeInEntitySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForPropertyTypeInEntitySetBySetAndType = new HashMap<>();

        getPermissionsForPropertyTypeInEntitySetBySetAndType.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS.getName() )
                        .allowFiltering()
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForPropertyTypeInEntitySetBySetAndType.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS.getName() )
                        .allowFiltering()
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getPermissionsForPropertyTypeInEntitySetBySet = new HashMap<>();

        getPermissionsForPropertyTypeInEntitySetBySet.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        getPermissionsForPropertyTypeInEntitySetBySet.put( PrincipalType.USER,
                session.prepare( QueryBuilder.select()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.deleteRowFromPropertyTypesInEntitySetsAclsTable = new HashMap<>();

        deleteRowFromPropertyTypesInEntitySetsAclsTable.put( PrincipalType.ROLE,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ROLE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteRowFromPropertyTypesInEntitySetsAclsTable.put( PrincipalType.USER,
                session.prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) ) );

        /**
         * Acls Requests
         */
        this.insertAclsRequest = new HashMap<>();

        insertAclsRequest.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.ROLES_ACLS_REQUESTS.getName() )
                        .value( CommonColumns.USER.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) ) );

        insertAclsRequest.put( PrincipalType.USER, session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.USERS_ACLS_REQUESTS.getName() )
                        .value( CommonColumns.USER.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.USERID.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.PERMISSIONS.cql(), QueryBuilder.bindMarker() ) ) );

        this.updateLookupForAclsRequest = new HashMap<>();

        updateLookupForAclsRequest.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.ROLES_ACLS_REQUESTS_LOOKUP.getName() )
                        .value( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.USER.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() ) ) );

        updateLookupForAclsRequest.put( PrincipalType.USER, session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.USERS_ACLS_REQUESTS_LOOKUP.getName() )
                        .value( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.USER.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() ) ) );

        this.deleteAclsRequest = new HashMap<>();

        deleteAclsRequest.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteAclsRequest.put( PrincipalType.USER, session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.deleteLookupForAclsRequest = new HashMap<>();

        deleteLookupForAclsRequest.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS_LOOKUP.getName() )
                        .where( QueryBuilder.eq( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() ) ) ) );

        deleteLookupForAclsRequest.put( PrincipalType.USER, session
                .prepare( QueryBuilder.delete()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS_LOOKUP.getName() )
                        .where( QueryBuilder.eq( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getAclsRequestsByUserId = new HashMap<>();

        getAclsRequestsByUserId.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) ) ) );

        getAclsRequestsByUserId.put( PrincipalType.USER, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getAclsRequestsByUserIdAndEntitySet = new HashMap<>();

        getAclsRequestsByUserIdAndEntitySet.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        getAclsRequestsByUserIdAndEntitySet.put( PrincipalType.USER, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.USER.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getAclsRequestsByEntitySet = new HashMap<>();

        getAclsRequestsByEntitySet.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        getAclsRequestsByEntitySet.put( PrincipalType.USER, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET.cql(), QueryBuilder.bindMarker() ) ) ) );

        this.getAclsRequestById = new HashMap<>();

        getAclsRequestById.put( PrincipalType.ROLE, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.ROLES_ACLS_REQUESTS_LOOKUP.getName() )
                        .where( QueryBuilder.eq( CommonColumns.REQUESTID.cql(), QueryBuilder.bindMarker() ) ) ) );

        getAclsRequestById.put( PrincipalType.USER, session
                .prepare( QueryBuilder.select()
                        .from( keyspace, Tables.USERS_ACLS_REQUESTS_LOOKUP.getName() )
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

    public PreparedStatement getInsertEntityPreparedStatement( EntityType entityType ) {
        return getInsertEntityPreparedStatement( entityType.getType() );
    }

    public PreparedStatement getInsertEntityPreparedStatement( FullQualifiedName fqn ) {
        return entityTypeInsertStatements.get( fqn );
    }

    public PreparedStatement getUpdateEntityPreparedStatement( EntityType entityType ) {
        return getUpdateEntityPreparedStatement( entityType.getType() );
    }

    public PreparedStatement getUpdateEntityPreparedStatement( FullQualifiedName fqn ) {
        return entityTypeUpdateStatements.get( fqn );
    }

    public PreparedStatement getUpdateEntityIdTypenamePreparedStatement( FullQualifiedName fqn ) {
        return entityIdToTypeUpdateStatements.get( fqn );
    }

    public PreparedStatement getUpdatePropertyPreparedStatement( PropertyType propertyType ) {
        return getUpdatePropertyPreparedStatement( propertyType.getType() );
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

    public void createEntityTypeTable(
            EntityType entityType,
            Map<FullQualifiedName, PropertyType> keyPropertyTypes,
            Set<PropertyType> propertyTypes ) {
        // Ensure that type name doesn't exist
        String entityTableQuery;

        String maybeTablename = null;
        // maybeTablename = null;
        do {
            maybeTablename = getTablenameForEntityType( entityType );
            entityTableQuery = Queries.createEntityTable( keyspace,
                    maybeTablename,
                    keyPropertyTypes,
                    propertyTypes );
        } while ( !Util.wasLightweightTransactionApplied( session.execute( entityTableQuery ) ) );

        Preconditions.checkState( StringUtils.isNotBlank( maybeTablename ),
                "Tablename for creating entity type {} cannot be blank.",
                entityType );
        final String tablename = maybeTablename;
        propertyTypes.stream()
                .forEach( pt -> session
                        .execute( Queries.createEntityTableIndex( keyspace, tablename, pt.getType() ) ) );
        entityType.getKey().forEach( fqn -> {
            // TODO: Use elasticsearch for maintaining index instead of maintaining in Cassandra.
            /*
             * This makes sure that index tables are created if they do not exist. Other entity types may already be
             * using this property type as a key.
             */
            // PropertyType keyPropertyType = keyPropertyTypes.get( fqn );
            // String typename = keyPropertyType.getTypename();
            // Preconditions.checkArgument( StringUtils.isNotBlank( typename ),
            // "Typename for key property type cannot be null" );
            // session.execute( Queries.createPropertyTableQuery( keyspace,
            // getTablenameForPropertyIndex( keyPropertyType ),
            // cc -> CassandraEdmMapping.getCassandraType( keyPropertyType.getDatatype() ) ) );

            // putPropertyIndexUpdateStatement( fqn );
        } );
        // putEntityTypeInsertStatement( entityType.getFullQualifiedName() );
        // putEntityTypeUpdateStatement( entityType.getFullQualifiedName() );
        // putEntityIdToTypeUpdateStatement( entityType.getFullQualifiedName() );
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

        putPropertyTypeUpdateStatement( propertyType.getType() );
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
                insertPropertyTypeLookup.bind( propertyType.getTypename(), propertyType.getType() ) );
    }

    public void updatePropertyTypeLookupTable( PropertyType propertyType ) {
        session.execute(
                updatePropertyTypeLookup.bind(

    propertyType.getTypen    ame(), propertyType.getType() ) );
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
                insertEntityTypeLookup.bind( entityType.getTypename(), entityType.getType() ) );
    }

    /**
     * Name getters for Entity Type
     */

    public String getTablenameForEntityType( EntityType entityType ) {
        return getTablenameForEntityType( entityType.getType() );
    }

    public String getTablenameForEntityType( FullQualifiedName fqn ) {
        return getTablename( TableType.entity_, fqn );
    }

    public boolean assignEntityToEntitySet( UUID entityId, String typename, String name ) {
        SecureRandom random = new SecureRandom();
        return Util.wasLightweightTransactionApplied(
                session.execute(
                        assignEntityToEntitySet.bind(
                                typename,
                                name,
                                Arrays.toString( random.generateSeed( 1 ) ),
                                entityId ) ) );
    }

    public boolean assignEntityToEntitySet( UUID entityId, EntitySet es ) {
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

    public String getTypenameForEntityId( UUID entityId ) {
        return Util.transformSafely( session.execute( this.getTypenameForEntityId.bind( entityId ) ).one(),
                r -> r.getString( CommonColumns.TYPENAME.cql() ) );
    }

    /*************************
     * Getters for Property Type
     *************************/

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
                        .update( keyspace, Tables.ENTITY_ID_TO_TYPE.getName() )
                        .with( QueryBuilder.set( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) ) ) );
    }

    public static class PreparedStatementMapping {
        public PreparedStatement               stmt;
        public Map<FullQualifiedName, Integer> mapping;
    }

    // TODO: Cache these calls per user.
    public PreparedStatementMapping getInsertEntityPreparedStatement(
            FullQualifiedName entityTypeFqn,
            Collection<FullQualifiedName> writableProperties,
            Optional<String> entitySetName ) {
        PreparedStatementMapping psm = new PreparedStatementMapping();
        psm.mapping = Maps.newHashMapWithExpectedSize( writableProperties.size() );

        Insert query = QueryBuilder
                .insertInto( keyspace, getTablenameForEntityType( entityTypeFqn ) )
                .value( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() )
                .value( CommonColumns.CLOCK.cql(),
                        QueryBuilder.fcall( "toTimestamp", QueryBuilder.now() ) )
                .value( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() )
                .value( CommonColumns.ENTITY_SETS.cql(), QueryBuilder.bindMarker() )
                .value( CommonColumns.SYNCIDS.cql(), QueryBuilder.bindMarker() );

        int order = 4;
        for ( FullQualifiedName fqn : writableProperties ) {
            query = query.value( Queries.fqnToColumnName( fqn ),
                    QueryBuilder.bindMarker( Queries.fqnToColumnName( fqn ) + "_bm" ) );
            psm.mapping.put( fqn, order++ );
        }
        try {
            psm.stmt = session.prepare( query );
        } catch ( InvalidQueryException e ) {
            logger.error( "Invalid query exception: {}", query, e );
        }
        return psm;
    }

    private void putEntityTypeInsertStatement( FullQualifiedName entityTypeFqn ) {
        entityTypeInsertStatements.putIfAbsent( entityTypeFqn,
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

    private void prepareSchemaQueries() {
        Set<UUID> aclIds = getAclsAppliedToSchemas();
        aclIds.forEach( this::prepareSchemaQuery );
    }

    private void prepareSchemaQuery( UUID aclId ) {
        String table = Tables.SCHEMAS.getName();
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
        createEntitySetMembersTableIfNotExists( keyspace, session );
        createPropertyTypeLookupTableIfNotExists( keyspace, session );
        createEntityTypeLookupTableIfNotExists( keyspace, session );
        createEntityIdTypenameTableIfNotExists( keyspace, session );
        // TODO: Remove this once everyone ACL is baked in.
        createSchemaTableForAclId( ACLs.EVERYONE_ACL );
    }

    public static String getTablename( TableType tableType, TypePK type ) {
        return getTablename( tableType, type.getType() );
    }

    public static String getTablename( TableType tableType, FullQualifiedName fqn ) {
        return getTablename( tableType, fqn.getFullQualifiedNameAsString() );
    }

    public static String getTablename( TableType tableType, String fqn ) {
        return tableType.name() + fqn;
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
                .execute( Queries.createSchemasTableQuery( keyspace, Tables.SCHEMAS.getName() ) ) );
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
        // TODO Ho Chung: rewrite this again
        deleteEntityTypeFromEntityTypesAclsTable( PrincipalType.ROLE, entityTypeFqn );
        deleteEntityTypeFromEntityTypesAclsTable( PrincipalType.USER, entityTypeFqn );
    }

    private void deleteEntityTypeFromEntityTypesAclsTable( PrincipalType type, FullQualifiedName entityTypeFqn ) {
        String columnName = type.equals( PrincipalType.ROLE ) ? CommonColumns.ROLE.cql() : CommonColumns.USER.cql();
        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );

        ResultSet rs = session.execute( this.getPermissionsForEntityTypeByType.get( type ).bind( entityTypeFqn ) );
        // TODO Ho Chung: very severe concurrency issue. To be addressed after demo
        for ( Row row : rs ) {
            session.execute( this.deleteRowFromEntityTypesAclsTable.get( type ).bind( row.getString( columnName ),
                    entityTypeTypename ) );
        }
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
        // TODO Ho Chung: rewrite this
        deleteEntitySetFromEntitySetsAclsTable( PrincipalType.ROLE, entitySetName );
        deleteEntitySetFromEntitySetsAclsTable( PrincipalType.USER, entitySetName );
    }

    private void deleteEntitySetFromEntitySetsAclsTable( PrincipalType type, String entitySetName ) {
        String columnName = type.equals( PrincipalType.ROLE ) ? CommonColumns.ROLE.cql() : CommonColumns.USER.cql();

        ResultSet rs = session.execute( this.getPermissionsForEntitySetBySet.get( type ).bind( entitySetName ) );
        // TODO Ho Chung: very severe concurrency issue. To be addressed after demo
        for ( Row row : rs ) {
            session.execute( this.deleteRowFromEntitySetsAclsTable.get( type ).bind( row.getString( columnName ),
                    entitySetName ) );
        }
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

    public ResultSet getRoleAclsForPropertyTypeInEntitySetBySetAndType(
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );
        return session.execute( getPermissionsForPropertyTypeInEntitySetBySetAndType.get( PrincipalType.ROLE )
                .bind( entitySetName, propertyTypeTypename ) );
    }

    public ResultSet getUserAclsForPropertyTypeInEntitySetBySetAndType(
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );
        return session.execute( getPermissionsForPropertyTypeInEntitySetBySetAndType.get( PrincipalType.USER )
                .bind( entitySetName, propertyTypeTypename ) );
    }

    public EnumSet<Permission> getRolePermissionsForPropertyTypeInEntityType(
            String role,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
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
        // TODO: Ho Chung: rewrite this
        deleteTypesFromPropertyTypesInEntityTypesAclsTable( PrincipalType.ROLE, entityTypeFqn, propertyTypeFqn );
        deleteTypesFromPropertyTypesInEntityTypesAclsTable( PrincipalType.USER, entityTypeFqn, propertyTypeFqn );
    }

    private void deleteTypesFromPropertyTypesInEntityTypesAclsTable(
            PrincipalType type,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        String columnName = type.equals( PrincipalType.ROLE ) ? CommonColumns.ROLE.cql() : CommonColumns.USER.cql();

        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );
        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        ResultSet rs = session.execute( this.getPermissionsForPropertyTypeInEntityTypeByTypes.get( type )
                .bind( entityTypeTypename, propertyTypeTypename ) );
        // TODO Ho Chung: very severe concurrency issue. To be addressed after demo
        for ( Row row : rs ) {
            session.execute( this.deleteRowFromPropertyTypesInEntityTypesAclsTable.get( type )
                    .bind( row.getString( columnName ), entityTypeTypename, propertyTypeTypename ) );
        }
    }

    public void deleteEntityTypeFromPropertyTypesInEntityTypesAclsTable( FullQualifiedName entityTypeFqn ) {
        // TODO: rewrite this
        deleteEntityTypeFromPropertyTypesInEntityTypesAclsTable( PrincipalType.ROLE, entityTypeFqn );
        deleteEntityTypeFromPropertyTypesInEntityTypesAclsTable( PrincipalType.USER, entityTypeFqn );
    }

    private void deleteEntityTypeFromPropertyTypesInEntityTypesAclsTable(
            PrincipalType type,
            FullQualifiedName entityTypeFqn ) {
        String columnName = type.equals( PrincipalType.ROLE ) ? CommonColumns.ROLE.cql() : CommonColumns.USER.cql();

        String entityTypeTypename = getTypenameForEntityType( entityTypeFqn );

        ResultSet rs = session.execute(
                this.getPermissionsForPropertyTypeInEntityTypeByEntityType.get( type ).bind( entityTypeTypename ) );
        // TODO Ho Chung: very severe concurrency issue. To be addressed after demo
        for ( Row row : rs ) {
            session.execute(
                    this.deleteRowFromPropertyTypesInEntityTypesAclsTable.get( type ).bind( row.getString( columnName ),
                            entityTypeTypename,
                            row.getString( CommonColumns.PROPERTY_TYPE.cql() ) ) );
        }
    }

    public EnumSet<Permission> getRolePermissionsForPropertyTypeInEntitySet(
            String role,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
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
        // TODO: Ho Chung: rewrite this
        deleteSetAndTypeFromPropertyTypesInEntitySetsAclsTable( PrincipalType.ROLE, entitySetName, propertyTypeFqn );
        deleteSetAndTypeFromPropertyTypesInEntitySetsAclsTable( PrincipalType.USER, entitySetName, propertyTypeFqn );
    }

    private void deleteSetAndTypeFromPropertyTypesInEntitySetsAclsTable(
            PrincipalType type,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        String columnName = type.equals( PrincipalType.ROLE ) ? CommonColumns.ROLE.cql() : CommonColumns.USER.cql();

        String propertyTypeTypename = getTypenameForPropertyType( propertyTypeFqn );

        ResultSet rs = session.execute( this.getPermissionsForPropertyTypeInEntitySetBySetAndType.get( type )
                .bind( entitySetName, propertyTypeTypename ) );
        // TODO Ho Chung: very severe concurrency issue. To be addressed after demo
        for ( Row row : rs ) {
            session.execute( this.deleteRowFromPropertyTypesInEntitySetsAclsTable.get( type )
                    .bind( row.getString( columnName ), entitySetName, propertyTypeTypename ) );
        }
    }

    public void deleteSetFromPropertyTypesInEntitySetsAclsTable( String entitySetName ) {
        // TODO: Ho Chung: rewrite this
        deleteSetFromPropertyTypesInEntitySetsAclsTable( PrincipalType.ROLE, entitySetName );
        deleteSetFromPropertyTypesInEntitySetsAclsTable( PrincipalType.USER, entitySetName );
    }

    private void deleteSetFromPropertyTypesInEntitySetsAclsTable(
            PrincipalType type,
            String entitySetName ) {
        String columnName = type.equals( PrincipalType.ROLE ) ? CommonColumns.ROLE.cql() : CommonColumns.USER.cql();

        ResultSet rs = session
                .execute( this.getPermissionsForPropertyTypeInEntitySetBySet.get( type ).bind( entitySetName ) );
        // TODO Ho Chung: very severe concurrency issue. To be addressed after demo
        for ( Row row : rs ) {
            session.execute( this.deleteRowFromPropertyTypesInEntitySetsAclsTable.get( type ).bind(
                    row.getString( columnName ), entitySetName, row.getString( CommonColumns.PROPERTY_TYPE.cql() ) ) );
        }
    }

    /**
     * Entity Set Owner methods
     */
    public String getOwnerForEntitySet( String entitySetName ) {

        return Util.transformSafely( session.execute( this.getOwnerForEntitySet.bind( entitySetName ) ).one(),
                r -> r.getString( CommonColumns.USER.cql() ) );
    }

    public boolean checkIfUserIsOwnerOfEntitySet( String userId, String entitySetName ) {
        String ownerId = getOwnerForEntitySet( entitySetName );

        if ( ownerId != null && !ownerId.isEmpty() ) {
            return userId.equals( ownerId );
        }
        return false;
    }

    public Iterable<String> getEntitySetsUserOwns( String userId ) {
        ResultSet rs = session.execute( this.getEntitySetsUserOwns.bind( userId ) );
        return Iterables.transform( rs, row -> row.getString( CommonColumns.ENTITY_SET.cql() ) );
    }

    public void addOwnerForEntitySet( String entitySetName, String userId ) {
        session.execute( this.updateOwnerForEntitySet.bind( userId, entitySetName ) );
        session.execute( this.updateOwnerLookupForEntitySet.bind( userId, entitySetName ) );
    }

    public void deleteFromEntitySetOwnerAndLookupTable( String entitySetName ) {
        String owner = getOwnerForEntitySet( entitySetName );

        session.execute( this.deleteFromEntitySetOwnerTable.bind( entitySetName ) );
        session.execute( this.deleteFromEntitySetOwnerLookupTable.bind( owner, entitySetName ) );
    }

    /**
     * Acl Requests methods
     */

    public boolean checkIfUserIsOwnerOfPermissionsRequest( String userId, UUID id ) {
        String ownerId = getUserIdFromRequestId( id );
        if ( ownerId != null && !ownerId.isEmpty() ) {
            return userId.equals( ownerId );
        }
        return false;
    }

    public void addPermissionsRequestForPropertyTypeInEntitySet(
            String userId,
            Principal principal,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            EnumSet<Permission> permissions ) {
        UUID requestId = UUID.randomUUID();
        Instant timestamp = Instant.now();
        switch ( principal.getType() ) {
            case ROLE:
                session.execute( this.insertAclsRequest.get( PrincipalType.ROLE ).bind( userId,
                        entitySetName,
                        timestamp,
                        requestId,
                        principal.getId(),
                        propertyTypeFqn,
                        permissions ) );
                session.execute( this.updateLookupForAclsRequest.get( PrincipalType.ROLE ).bind( requestId,
                        userId,
                        entitySetName,
                        timestamp ) );
                break;
            case USER:
                session.execute( this.insertAclsRequest.get( PrincipalType.USER ).bind( userId,
                        entitySetName,
                        timestamp,
                        requestId,
                        principal.getId(),
                        propertyTypeFqn,
                        permissions ) );
                session.execute( this.updateLookupForAclsRequest.get( PrincipalType.USER ).bind( requestId,
                        userId,
                        entitySetName,
                        timestamp ) );
                break;
            default:
                break;
        }
    }

    public void removePermissionsRequestForEntitySet( UUID id ) {
        PrincipalType type;
        String userId;
        String entitySetName;
        Instant timestamp;

        // Retrieve Row info by request id
        Row rowRole = session.execute( this.getAclsRequestById
                .get( PrincipalType.ROLE )
                .bind( id ) ).one();

        if ( rowRole != null ) {
            type = PrincipalType.ROLE;
            userId = rowRole.getString( CommonColumns.USER.cql() );
            entitySetName = rowRole.getString( CommonColumns.ENTITY_SET.cql() );
            timestamp = rowRole.get( CommonColumns.CLOCK.cql(), InstantCodec.instance );
        } else {
            Row rowUser = session.execute( this.getAclsRequestById
                    .get( PrincipalType.USER )
                    .bind( id ) ).one();

            if ( rowUser != null ) {
                type = PrincipalType.USER;
                userId = rowUser.getString( CommonColumns.USER.cql() );
                entitySetName = rowUser.getString( CommonColumns.ENTITY_SET.cql() );
                timestamp = rowUser.get( CommonColumns.CLOCK.cql(), InstantCodec.instance );
            } else {
                // TODO write custom handler
                throw new ResourceNotFoundException( "Permissions Request not found." );
            }
        }

        // Actual removal
        session.execute( this.deleteAclsRequest.get( type ).bind( userId, entitySetName, timestamp, id ) );
        session.execute( this.deleteLookupForAclsRequest.get( type ).bind( id ) );
    }

    public void removePermissionsRequestForEntitySet( String entitySetName ) {
        // TODO Ho Chung: rewrite this
        removePermissionsRequestForEntitySet( PrincipalType.ROLE, entitySetName );
        removePermissionsRequestForEntitySet( PrincipalType.USER, entitySetName );
    }

    private void removePermissionsRequestForEntitySet( PrincipalType type, String entitySetName ) {

        ResultSet rs = session.execute( getAclsRequestsByEntitySet.get( type ).bind( entitySetName ) );
        // TODO Ho Chung: very severe concurrency issue. To be addressed after demo
        for ( Row row : rs ) {
            UUID requestId = row.getUUID( CommonColumns.REQUESTID.cql() );
            session.execute( this.deleteAclsRequest.get( type ).bind(
                    row.getString( CommonColumns.USER.cql() ),
                    entitySetName,
                    row.get( CommonColumns.CLOCK.cql(), InstantCodec.instance ),
                    requestId ) );
            session.execute( this.deleteLookupForAclsRequest.get( type ).bind( requestId ) );
        }
    }

    public String getEntitySetNameFromRequestId( UUID id ) {
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

    public String getUserIdFromRequestId( UUID id ) {
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

    public Iterable<Row> getAllReceivedRequestsForPermissionsOfUserId( PrincipalType type, String userId ) {
        return StreamSupport.stream( getEntitySetsUserOwns( userId ).spliterator(), false )
                .map( entitySetName -> getAllReceivedRequestsForPermissionsOfEntitySet( type, entitySetName ) )
                .flatMap( iterRow -> StreamSupport.stream( iterRow.spliterator(), false ) )
                .collect( Collectors.toList() );
    }

    public Iterable<Row> getAllReceivedRequestsForPermissionsOfEntitySet( PrincipalType type, String entitySetName ) {
        return session
                .execute( getAclsRequestsByEntitySet.get( type ).bind( entitySetName ) );
    }

    public Iterable<Row> getAllSentRequestsForPermissions( PrincipalType type, String userId ) {
        return session
                .execute( getAclsRequestsByUserId.get( type ).bind( userId ) );
    }

    public Iterable<Row> getAllSentRequestsForPermissions( PrincipalType type, String userId, String entitySetName ) {
        return session
                .execute( getAclsRequestsByUserIdAndEntitySet.get( type ).bind( userId, entitySetName ) );
    }

    public PropertyType getPropertyType( FullQualifiedName propertyType ) {
        return Preconditions.checkNotNull(
                propertyTypes.get( propertyType ),
                "Property type does not exist" );
    }

}
