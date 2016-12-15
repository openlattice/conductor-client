package com.kryptnostic.datastore.services;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AclKey;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.internal.DatastoreConstants;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.requests.GetSchemasRequest.TypeDetails;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.Queries;
import com.kryptnostic.datastore.util.Util;

public class EdmService implements EdmManager {
    private static final Logger                         logger = LoggerFactory.getLogger( EdmService.class );

    private final IMap<FullQualifiedName, PropertyType> propertyTypes;
    private final IMap<FullQualifiedName, EntityType>   entityTypes;
    private final IMap<String, EntitySet>               entitySets;

    private final IMap<AclKey, FullQualifiedName>       fqnsByAclKey;
    private final IMap<FullQualifiedName, String>       typenames;
    private final IMap<String, FullQualifiedName>       fqns;

    private final Session                               session;
    private final Mapper<EntitySet>                     entitySetMapper;
    private final Mapper<EntityType>                    entityTypeMapper;
    private final Mapper<PropertyType>                  propertyTypeMapper;

    private final CassandraEdmStore                     edmStore;
    private final CassandraTableManager                 tableManager;
    private final PermissionsService                    permissionsService;

    public EdmService(
            Session session,
            HazelcastInstance hazelcastInstance,
            MappingManager mappingManager,
            CassandraTableManager tableManager,
            PermissionsService permissionsService ) {
        this.session = session;
        this.edmStore = mappingManager.createAccessor( CassandraEdmStore.class );
        this.entitySetMapper = mappingManager.mapper( EntitySet.class );
        this.entityTypeMapper = mappingManager.mapper( EntityType.class );
        this.propertyTypeMapper = mappingManager.mapper( PropertyType.class );
        this.tableManager = tableManager;
        this.permissionsService = permissionsService;
        this.propertyTypes = hazelcastInstance.getMap( HazelcastMap.PROPERTY_TYPES.name() );
        this.entityTypes = hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() );
        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
        this.typenames = hazelcastInstance.getMap( HazelcastMap.TYPENAMES.name() );
        this.fqnsByAclKey = hazelcastInstance.getMap( HazelcastMap.FQN_ACL_KEY.name() );
        this.fqns = hazelcastInstance.getMap( HazelcastMap.FQNS.name() );
        List<EntityType> objectTypes = edmStore.getEntityTypes().all();
        // Temp comment out the following two lines to avoid "Schema is out of sync." crash
        // and NPE for the PreparedStatement. Need to engineer it.
        // schemas.forEach( schema -> logger.info( "Namespace loaded: {}", schema ) );
        // schemas.forEach( tableManager::registerSchema );
        objectTypes.forEach( objectType -> logger.info( "Object read: {}", objectType ) );
        objectTypes.forEach( tableManager::registerEntityTypesAndAssociatedPropertyTypes );
    }

    @Override
    public Iterable<Schema> getSchemas() {
        return getSchemas( EnumSet.allOf( TypeDetails.class ) );
    }

    @Override
    public Iterable<Schema> getSchemas( Set<TypeDetails> requestedDetails ) {
        PreparedStatement stmt = tableManager.getAllSchemasStatement( ACLs.EVERYONE_ACL );

        if ( stmt == null ) {
            return null;
        }

        final SchemaDetailsAdapter adapter = new SchemaDetailsAdapter(
                tableManager,
                entityTypeMapper,
                propertyTypeMapper,
                requestedDetails );

        Iterable<Schema> results = Iterables.transform( session.execute( stmt.bind() ), adapter );

        return Iterables.filter( results, Predicates.notNull() );
    }

    @Override
    public Iterable<Schema> getSchemasInNamespace( String namespace, Set<TypeDetails> requestedDetails ) {

        PreparedStatement stmt = tableManager.getSchemasInNamespaceStatement( ACLs.EVERYONE_ACL );

        if ( stmt == null ) {
            return null;
        }

        final SchemaDetailsAdapter adapter = new SchemaDetailsAdapter(
                tableManager,
                entityTypeMapper,
                propertyTypeMapper,
                requestedDetails );

        Iterable<Schema> results = Iterables.transform( session.execute( stmt.bind( namespace ) ), adapter );

        return Iterables.filter( results, Predicates.notNull() );
    }

    @Override
    public Schema getSchema( String namespace, String name, Set<TypeDetails> requestedDetails ) {

        PreparedStatement stmt = tableManager.getSchemaStatement( ACLs.EVERYONE_ACL );

        if ( stmt == null ) {
            return null;
        }

        final SchemaDetailsAdapter adapter = new SchemaDetailsAdapter(
                tableManager,
                entityTypeMapper,
                propertyTypeMapper,
                requestedDetails );

        Iterable<Schema> results = Iterables.transform( session.execute( stmt.bind( namespace, name ) ), adapter );

        return results.iterator().next();
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#updateObjectType(com.kryptnostic.types.ObjectType)
     */
    @Override
    public void upsertEntityType( Principal principal, EntityType entityType ) {
        // This call will fail if the typename has already been set for the entity.
        ensureValidEntityType( entityType );
        if ( checkEntityTypeExists( entityType.getType() ) ) {
            upsertEntityType( principal, entityType, true );
        } else {
            createEntityType( principal, entityType, true );
        }
    }

    private void upsertEntityType( Principal principal, EntityType entityType, boolean isValid ) {
        // This call will fail if the typename has already been set for the entity.
        if ( isValid ) {
            // Retrieve database record of entityType
            EntityType dbRecord = getEntityType( entityType.getType() );

            // Update properties
            Set<FullQualifiedName> currentPropertyTypes = dbRecord.getProperties();

            Set<FullQualifiedName> removablePropertyTypesInEntityType = Sets.difference( currentPropertyTypes,
                    entityType.getProperties() );
            removePropertyTypesFromEntityType( dbRecord, removablePropertyTypesInEntityType, true );

            Set<FullQualifiedName> newPropertyTypesInEntityType = Sets.difference( entityType.getProperties(),
                    currentPropertyTypes );
            addPropertyTypesToEntityType( entityType.getType().getNamespace(),
                    entityType.getType().getName(),
                    newPropertyTypesInEntityType );

            // Update Schema
            Set<FullQualifiedName> currentSchemas = dbRecord.getSchemas();

            Set<FullQualifiedName> removableSchemas = Sets.difference( currentSchemas, entityType.getSchemas() );
            removableSchemas.forEach( schema -> removeEntityTypesFromSchema( schema.getNamespace(),
                    schema.getName(),
                    ImmutableSet.of( entityType.getType() ) ) );

            Set<FullQualifiedName> newSchemas = Sets.difference( entityType.getSchemas(), currentSchemas );
            newSchemas.forEach( schema -> addEntityTypesToSchema( schema.getNamespace(),
                    schema.getName(),
                    ImmutableSet.of( entityType.getType() ) ) );

            // Persist
            entityTypeMapper.save( entityType );
        }
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createPropertyType(com.kryptnostic.types.PropertyType) update
     * propertyType (and return true upon success) if exists, return false otherwise
     */
    @Override
    public void upsertPropertyType( PropertyType propertyType ) {
        if ( checkPropertyTypeExists( propertyType.getType() ) ) {
            upsertPropertyType( propertyType, true );
        } else {
            createPropertyType( propertyType, true );
        }
    }

    private void upsertPropertyType( PropertyType propertyType, boolean isValid ) {
        if ( isValid ) {
            // TODO: Handle UUID mismatch / FQN change
            /*
             * Currently we aren't handling FQN changes properly. As FQN are used for lookup, if the FQN changes it
             * requires deleting the old property and creating a new property. This is problematic as it results in data
             * being deleted or having to be copied over manually from one column to another. To avoid this machinery
             * prone to failure, we should make UUIDs required for updating property types, entity types, and entity
             * sets.
             */

            // TODO: Ho Chung: are we getting rid of create and only keeping upsert?
            PropertyType dbRecord = propertyTypes.put( propertyType.getType(), propertyType );

            if ( dbRecord != null ) {
                // Update Schema
                Set<FullQualifiedName> currentSchemas = dbRecord.getSchemas();

                Set<FullQualifiedName> removableSchemas = Sets.difference( currentSchemas, propertyType.getSchemas() );
                removableSchemas.forEach( schema -> removePropertyTypesFromSchema( schema.getNamespace(),
                        schema.getName(),
                        ImmutableSet.of( propertyType.getType() ) ) );

                Set<FullQualifiedName> newSchemas = Sets.difference( propertyType.getSchemas(), currentSchemas );
                newSchemas.forEach( schema -> addPropertyTypesToSchema( schema.getNamespace(),
                        schema.getName(),
                        ImmutableSet.of( propertyType.getType() ) ) );
                // Set Property type
                propertyTypes.set( propertyType.getType(), propertyType );
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createNamespace(com.kryptnostic.types.Namespace)
     */
    @Override
    public void upsertSchema( Schema schema ) {
        ensureValidSchema( schema );

        UUID aclId = ( schema.getAclId() != null ) ? schema.getAclId() : ACLs.EVERYONE_ACL;
        if ( checkSchemaExists( schema.getNamespace(), schema.getName() ) ) {
            upsertSchema( schema, aclId, true );
        } else {
            createSchema( schema.getNamespace(),
                    schema.getName(),
                    aclId,
                    schema.getEntityTypeFqns(),
                    schema.getPropertyTypeFqns() );
        }
    }

    private void upsertSchema( Schema schema, UUID aclId, boolean isValid ) {
        if ( isValid ) {
            // Retrieve database record of entityType
            Schema dbRecord = getSchema( schema.getNamespace(),
                    schema.getNamespace(),
                    EnumSet.noneOf( TypeDetails.class ) );

            // Update entity types
            Set<FullQualifiedName> currentEntityTypes = dbRecord.getEntityTypeFqns();

            Set<FullQualifiedName> removableEntityTypes = Sets.difference( currentEntityTypes,
                    schema.getEntityTypeFqns() );
            removeEntityTypesFromSchema( schema.getNamespace(), schema.getName(), removableEntityTypes );

            Set<FullQualifiedName> newEntityTypes = Sets.difference( schema.getEntityTypeFqns(), currentEntityTypes );
            addEntityTypesToSchema( schema.getNamespace(), schema.getName(), newEntityTypes );

            // Update property types
            Set<FullQualifiedName> currentPropertyTypes = dbRecord.getPropertyTypeFqns();

            Set<FullQualifiedName> removablePropertyTypes = Sets.difference( currentPropertyTypes,
                    schema.getPropertyTypeFqns() );
            removePropertyTypesFromSchema( schema.getNamespace(), schema.getName(), removablePropertyTypes );

            Set<FullQualifiedName> newPropertyTypes = Sets.difference( schema.getPropertyTypeFqns(),
                    currentPropertyTypes );
            addPropertyTypesToSchema( schema.getNamespace(), schema.getName(), newPropertyTypes );

            // Persist
            session.execute( tableManager.getSchemaUpsertStatement( aclId ).bind( schema.getNamespace(),
                    schema.getName(),
                    schema.getEntityTypeFqns(),
                    schema.getPropertyTypeFqns() ) );
        }
    }

    @Override
    public void createPropertyType( PropertyType propertyType ) {
        ensurePropertyTypeDoesNotExist( propertyType.getType() );
        createPropertyType( propertyType, true );
    }

    private void createPropertyType( PropertyType propertyType, boolean isValid ) {
        propertyType.getSchemas()
                .forEach(
                        schemaFqn -> addPropertyTypesToSchema( schemaFqn.getNamespace(),
                                schemaFqn.getName(),
                                ImmutableSet.of( propertyType.getType() ) ) );

        tableManager.insertToPropertyTypeLookupTable( propertyType );
    }

    @Override
    public void createSchema(
            String namespace,
            String name,
            UUID aclId,
            Set<FullQualifiedName> entityTypes,
            Set<FullQualifiedName> propertyTypes ) {
        ensureSchemaDoesNotExist( namespace, name );
        createSchema( namespace, name, aclId, entityTypes, propertyTypes, true );
    }

    private void createSchema(
            String namespace,
            String name,
            UUID aclId,
            Set<FullQualifiedName> entityTypes,
            Set<FullQualifiedName> propertyTypes,
            boolean isValid ) {
        if ( isValid ) {
            boolean created = false;
            tableManager.createSchemaTableForAclId( aclId );

            entityTypes.stream()
                    .forEach( entityTypeFqn -> tableManager.entityTypeAddSchema( entityTypeFqn, namespace, name ) );

            propertyTypes.stream()
                    .forEach(
                            propertyTypeFqn -> tableManager.propertyTypeAddSchema( propertyTypeFqn, namespace, name ) );

            created = Util.wasLightweightTransactionApplied(
                    session.execute(
                            tableManager.getSchemaInsertStatement( aclId )

                                    .bind( namespace, name, entityTypes, propertyTypes ) ) );
            if ( !created ) {
                throw new IllegalStateException( "Failed to create schema." );
            }
        }
    }

    @Override
    public void createSchema( String namespace, String name, UUID aclId, Set<FullQualifiedName> entityTypes ) {
        ensureSchemaDoesNotExist( namespace, name );
        createSchema( namespace, name, aclId, entityTypes, true );
    }

    private void createSchema(
            String namespace,
            String name,
            UUID aclId,
            Set<FullQualifiedName> entityTypes,
            boolean isValid ) {
        if ( isValid ) {
            Set<FullQualifiedName> propertyTypes = entityTypes.stream()
                    .map( entityTypeFqn -> entityTypeMapper.get( entityTypeFqn.getNamespace(),
                            entityTypeFqn.getName() ) )
                    .map( entityType -> entityType.getProperties() )
                    .reduce( ( left, right ) -> {
                        left.addAll( right );
                        return left;
                    } ).get();
            createSchema( namespace, name, aclId, entityTypes, propertyTypes, true );
        }
    }

    @Override
    public void deleteEntityType( FullQualifiedName entityTypeFqn ) {
        // This step validates existence of Entity Type.
        EntityType entityType = getEntityType( entityTypeFqn );

        entityType.getSchemas().forEach(
                schemaFqn -> removeEntityTypesFromSchema( schemaFqn.getNamespace(),
                        schemaFqn.getName(),
                        ImmutableSet.of( entityTypeFqn ) ) );
        // TODO: remove property types from schema using reference counting

        // Remove all entity sets of the type
        getEntitySetsForEntityType( entityTypeFqn ).forEach( entitySet -> {
            deleteEntitySet( entitySet.getName() );
        } );
        permissionsService.removePermissionsForEntityType( entityTypeFqn );
        permissionsService.removePermissionsForPropertyTypeInEntityType( entityTypeFqn );

        // Previous functions may need lookup to work - must delete lookup last
        tableManager.deleteFromEntityTypeLookupTable( entityType );
        entityTypeMapper.delete( entityType );
    }

    @Override
    public void deletePropertyType( FullQualifiedName propertyTypeFqn ) {
        // This step validates existence of Property Type.
        PropertyType propertyType = getPropertyType( propertyTypeFqn );

        propertyType.getSchemas().forEach( schemaFqn -> removePropertyTypesFromSchema( schemaFqn.getNamespace(),
                schemaFqn.getName(),
                ImmutableSet.of( propertyTypeFqn ) ) );
        getEntityTypes().forEach( entityType -> {
            removePropertyTypesFromEntityType( entityType, ImmutableSet.of( propertyTypeFqn ) );
        } );

        tableManager.deleteFromPropertyTypeLookupTable( propertyType );

        propertyTypeMapper.delete( propertyType );
    }

    @Override
    public void deleteSchema( Schema namespaces ) {
        // TODO: Implement delete schema
    }

    @Override
    public void addEntityTypesToSchema( String namespace, String name, Set<FullQualifiedName> entityTypes ) {
        ensureSchemaExists( namespace, name );
        ensureEntityTypesExist( entityTypes );

        Set<FullQualifiedName> propertyTypes = new HashSet<>();

        entityTypes.stream()
                .map( entityTypeFqn -> getEntityType( entityTypeFqn ) )
                .forEach( entityType -> {
                    // Get all properties for each entity type
                    propertyTypes.addAll( entityType.getProperties() );
                    // Update Schema column for each Entity Type
                    tableManager.entityTypeAddSchema( entityType, namespace, name );
                } );

        addPropertyTypesToSchema( namespace, name, propertyTypes );

        for ( UUID aclId : AclContextService.getCurrentContextAclIds() ) {
            session.executeAsync(
                    tableManager.getSchemaAddEntityTypeStatement( aclId )
                            .bind( entityTypes, propertyTypes, namespace, name ) );
        }
    }

    @Override
    public void removeEntityTypesFromSchema( String namespace, String name, Set<FullQualifiedName> entityTypes ) {
        ensureSchemaExists( namespace, name );
        ensureEntityTypesExist( entityTypes );

        // TODO: propertyTypes not removed From Schema table when Entity Types are removed. Need reference counting on
        // propertyTypes to do so.
        Set<FullQualifiedName> propertyTypes = new HashSet<>();

        entityTypes.stream()
                .map( entityTypeFqn -> getEntityType( entityTypeFqn ) )
                .forEach( entityType -> {
                    // Get all properties for each entity type
                    propertyTypes.addAll( entityType.getProperties() );
                    // Update Schema column for each Entity Type
                    tableManager.entityTypeRemoveSchema( entityType, namespace, name );
                } );

        // removePropertyTypesFromSchema( namespace, name, propertyTypes );
        propertyTypes.stream()
                .forEach(
                        propertyTypeFqn -> tableManager.propertyTypeRemoveSchema( propertyTypeFqn, namespace, name ) );

        for ( UUID aclId : AclContextService.getCurrentContextAclIds() ) {
            session.executeAsync(
                    tableManager.getSchemaRemoveEntityTypeStatement( aclId ).bind( entityTypes, namespace, name ) );
        }
    }

    @Override
    public void createEntityType(
            Principal principal,
            EntityType entityType ) {
        ensureValidEntityType( entityType );
        ensureEntityTypeDoesNotExist( entityType.getType() );
        createEntityType( principal, entityType, true );
    }

    private void createEntityType( Principal principal, EntityType entityType, boolean isValid ) {
        /**
         * Refactored by Ho Chung, so that upsertEntityType won't do duplicate checks. checkedValid means that isValid
         * is true if entity type is checked valid, and checked not already exist.
         */
        if ( isValid ) {
            // Only create entity table if insert transaction succeeded.
            final EntityType existing = entityTypes.putIfAbsent( entityType.getType(), entityType );
            if ( existing == null ) {
                Set<PropertyType> properties = ImmutableSet
                        .copyOf( propertyTypes.getAll( entityType.getProperties() ).values() );
                tableManager.createEntityTypeTable( entityType,
                        Maps.asMap( entityType.getKey(),
                                fqn -> getPropertyType( fqn ) ),
                        properties );
                entityType.getSchemas().forEach( schema -> addEntityTypesToSchema( schema.getNamespace(),
                        schema.getName(),
                        ImmutableSet.of( entityType.getType() ) ) );
                tableManager.insertToEntityTypeLookupTable( entityType );
            } else {
                // Retrieve properties known to user
                Set<FullQualifiedName> currentPropertyTypes = existing.getProperties();
                // Remove the removable property types in database properly; this step takes care of removal of
                // permissions
                Set<FullQualifiedName> removablePropertyTypesInEntityType = Sets.difference( currentPropertyTypes,
                        entityType.getProperties() );
                removePropertyTypesFromEntityType( existing, removablePropertyTypesInEntityType, true );
                // Add the new property types in
                Set<FullQualifiedName> newPropertyTypesInEntityType = Sets.difference( entityType.getProperties(),
                        currentPropertyTypes );
                addPropertyTypesToEntityType( entityType.getType().getNamespace(),
                        entityType.getType().getName(),
                        newPropertyTypesInEntityType );
            }
        }
    }

    @Override
    public void upsertEntitySet( EntitySet entitySet ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( entitySet.getTypename() ),
                "Cannot store an entity set with a blank typename" );
        entitySets.set( entitySet.getName(), entitySet );
        if ( checkEntitySetExists( entitySet.getName() ) ) {
            ensureEntityTypeExists( entitySet.getType() );

            String typename = tableManager.getTypenameForEntityType( entitySet.getType() );
            if ( checkEntitySetExists( typename, entitySet.getName() ) ) {
                // This means that entity type is not changed.
                entitySet.setTypename( typename );
                entitySetMapper.save( entitySet );
            } else {
                throw new IllegalArgumentException( "Cannot change entity type associated to the entity set." );
            }
        } else {
            createEntitySet( Principals.getCurrentUser(), entitySet );
        }
    }

    @Override
    public void deleteEntitySet( EntitySet entitySet ) {
        deleteEntitySet( entitySet.getName() );
    }

    @Override
    public void deleteEntitySet( String entitySetName ) {
        EntitySet entitySet = getEntitySet( entitySetName );

        permissionsService.removePermissionsForEntitySet( entitySetName );
        permissionsService.removePermissionsForPropertyTypeInEntitySet( entitySetName );
        permissionsService.removePermissionsRequestForEntitySet( entitySetName );

        tableManager.deleteFromEntitySetOwnerAndLookupTable( entitySetName );
        // TODO Remove entities in the entity set
        entitySetMapper.delete( entitySet );
    }

    @Override
    public void assignEntityToEntitySet( UUID entityId, String name ) {
        String typename = tableManager.getTypenameForEntityId( entityId );
        ensureEntityTypeExists( typename );
        ensureEntitySetExists( typename, name );

        tableManager.assignEntityToEntitySet( entityId, typename, name );
    }

    @Override
    public void assignEntityToEntitySet( UUID entityId, EntitySet es ) {
        assignEntityToEntitySet( entityId, es.getName() );
    }

    @Override
    public void createEntitySet( Principal principal, FullQualifiedName type, String name, String title ) {
        createEntitySet( principal, new EntitySet( Optional.absent(), type, name, title ) );
    }

    @Override
    public void createEntitySet( EntitySet entitySet ) {
        ensureEntityTypeExists( entitySet.getType() );
        String typename = tableManager.getTypenameForEntityType( entitySet.getType() );
        System.out.println( "typename upon entity set creation: " + typename );
        entitySet.setTypename( typename );
        if ( entitySets.putIfAbsent( entitySet.getName(), entitySet ) != null ) {} else {
            throw new IllegalStateException( "Entity set of the same name already exists." );

        }
    }

    @Override
    public void createEntitySet( Principal principal, EntitySet entitySet ) {
        Principals.ensureUser( principal );
        createEntitySet( entitySet );
        tableManager.addOwnerForEntitySet( entitySet.getName(), principal.getName() );

        EntityType entityType = entityTypes.get( entitySet.getType() );
        permissionsService.addPermissionsForEntitySet( principal,
                entitySet.getName(),
                EnumSet.allOf( Permission.class ) );
        entityType.getProperties()
                .forEach( propertyTypeFqn -> permissionsService.addPermissionsForPropertyTypeInEntitySet(
                        principal,
                        entitySet.getName(),
                        propertyTypeFqn,
                        EnumSet.allOf( Permission.class ) ) );

        tableManager.addOwnerForEntitySet( entitySet.getName(), principal.getId() );

        permissionsService.addPermissionsForEntitySet( principal,
                entitySet.getName(),
                EnumSet.allOf( Permission.class ) );
        entityType.getProperties()
                .forEach( propertyTypeFqn -> permissionsService.addPermissionsForPropertyTypeInEntitySet(
                        principal,
                        entitySet.getName(),
                        propertyTypeFqn,
                        EnumSet.allOf( Permission.class ) ) );
    }

    @Override
    public EntityType getEntityType( FullQualifiedName entityTypeFqn ) {
        return Preconditions.checkNotNull(
                entityTypes.get( entityTypeFqn ),
                "Entity type does not exist" );
    }

    public Iterable<EntityType> getEntityTypes() {
        return edmStore.getEntityTypes().all();
    }

    @Override
    public EntityType getEntityType( String namespace, String name ) {
        return getEntityType( new FullQualifiedName( namespace, name ) );
    }

    @Override
    public EntitySet getEntitySet( String name ) {
        EntitySet entitySet = Preconditions.checkNotNull( edmStore.getEntitySet( name ), "Entity Set does not exist." );
        return EdmDetailsAdapter.setEntitySetTypename( tableManager, entitySet );
    }

    private Iterable<EntitySet> getEntitySetsForEntityType( FullQualifiedName type ) {
        // Returns ALL entity sets of an entity type, viewable or not.
        // Used for deletion of all entity sets when deleting an entity type.
        String typename = getEntityType( type ).getTypename();
        return edmStore.getEntitySetsForEntityType( typename ).all().stream()
                .map( entitySet -> EdmDetailsAdapter.setEntitySetTypename( tableManager, entitySet ) )
                .collect( Collectors.toList() );
    }

    @Override
    public Iterable<EntitySet> getEntitySets() {
        return edmStore.getEntitySets().all().stream()
                .map( entitySet -> EdmDetailsAdapter.setEntitySetTypename( tableManager, entitySet ) )
                .collect( Collectors.toList() );
    }

    @Override
    public Iterable<EntitySet> getEntitySetsUserOwns( String userId ) {
        return StreamSupport.stream( getEntitySetNamesUserOwns( userId ).spliterator(), false )
                .map( entitySetName -> getEntitySet( entitySetName ) )
                .collect( Collectors.toList() );
    }

    @Override
    public Iterable<String> getEntitySetNamesUserOwns( String userId ) {
        return tableManager.getEntitySetsUserOwns( userId );
    }

    @Override
    public PropertyType getPropertyType( FullQualifiedName propertyType ) {
        return Preconditions.checkNotNull(
                propertyTypeMapper.get( propertyType.getNamespace(), propertyType.getName() ),
                "Property type does not exist." );
    }

    @Override
    public Iterable<PropertyType> getPropertyTypesInNamespace( String namespace ) {
        return edmStore.getPropertyTypesInNamespace( namespace ).all();
    }

    @Override
    public Iterable<PropertyType> getPropertyTypes() {
        return edmStore.getPropertyTypes().all();
    }

    @Override
    public FullQualifiedName getPropertyTypeFullQualifiedName( String typename ) {
        FullQualifiedName fqn = tableManager.getPropertyTypeForTypename( typename );
        return Preconditions.checkNotNull( fqn );
    }

    @Override
    public FullQualifiedName getEntityTypeFullQualifiedName( String typename ) {
        FullQualifiedName fqn = tableManager.getEntityTypeForTypename( typename );
        return Preconditions.checkNotNull( fqn );
    }

    @Override
    public EntityDataModel getEntityDataModel() {
        Iterable<Schema> schemas = getSchemas();
        Iterable<EntityType> entityTypes = getEntityTypes();
        Iterable<PropertyType> propertyTypes = getPropertyTypes();
        Iterable<EntitySet> entitySets = getEntitySets();
        final Set<String> namespaces = Sets.newHashSet();

        entityTypes.forEach( entityType -> namespaces.add( entityType.getNamespace() ) );
        propertyTypes.forEach( propertyType -> namespaces.add( propertyType.getNamespace() ) );

        return new EntityDataModel(
                namespaces,
                schemas,
                entityTypes,
                propertyTypes,
                entitySets );
    }

    @Override
    public void addPropertyTypesToEntityType( String namespace, String name, Set<FullQualifiedName> properties ) {

        EntityType entityType = getEntityType( namespace, name );
        ensurePropertyTypesExist( properties );

        Set<FullQualifiedName> newProperties = ImmutableSet
                .copyOf( Sets.difference( properties, entityType.getProperties() ) );

        if ( newProperties == null || newProperties.size() == 0 ) {
            return;
        }

        entityType.addProperties( newProperties );
        edmStore.updateExistingEntityType(
                entityType.getNamespace(),
                entityType.getName(),
                entityType.getKey(),
                entityType.getProperties() );

        String tableName = tableManager.getTablenameForEntityType( new FullQualifiedName( namespace, name ) );

        String propertiesWithType = newProperties.stream()
                .map( fqn -> tableManager.getTypenameForPropertyType( fqn ) + " "
                        + CassandraEdmMapping
                                .getCassandraTypeName( propertyTypes.get( fqn ).getDatatype() ) )
                .collect( Collectors.joining( "," ) );

        session.execute( Queries.addPropertyColumnsToEntityTable(
                DatastoreConstants.KEYSPACE,
                tableName,
                propertiesWithType ) );

        newProperties.forEach( propertyTypeFqn -> session.execute( Queries.createEntityTableIndex(
                DatastoreConstants.KEYSPACE,
                tableName,
                propertyTypeFqn ) ) );

        Set<FullQualifiedName> schemas = entityType.getSchemas();
        schemas.forEach( schemaFqn -> {
            addPropertyTypesToSchema( schemaFqn.getNamespace(), schemaFqn.getName(), newProperties );
        } );
    }

    @Override
    public void removePropertyTypesFromEntityType( String namespace, String name, Set<FullQualifiedName> properties ) {
        EntityType entityType = getEntityType( namespace, name );
        removePropertyTypesFromEntityType( entityType, properties );
        // TODO: remove property types from Schema should be done via reference counting
    }

    @Override
    public void removePropertyTypesFromEntityType( EntityType entityType, Set<FullQualifiedName> properties ) {
        Preconditions.checkArgument( Sets.intersection( entityType.getKey(), properties ).isEmpty(),
                "Cannot remove key property types from entity type." );
        ensurePropertyTypesExist( properties );
        removePropertyTypesFromEntityType( entityType, properties, true );
    }

    private void removePropertyTypesFromEntityType(
            EntityType entityType,
            Set<FullQualifiedName> properties,
            boolean isValid ) {
        /**
         * Refactored by Ho Chung, to avoid duplicate checks. isValid means that entityType is checked to be valid, and
         * property types are checked to exist.
         */
        if ( isValid ) {
            
            Set<FullQualifiedName> removableProperties = ImmutableSet.copyOf( Sets.intersection( properties, entityType.getProperties() ) );

            if( removableProperties == null || removableProperties.size() == 0 ){
                return;
            }

            entityType.removeProperties( removableProperties );
            // Acl
            removableProperties
                    .forEach( propertyTypeFqn -> permissionsService.removePermissionsForPropertyTypeInEntityType(
                            entityType.getType(), propertyTypeFqn ) );
            // TODO: Remove properties from Schema, once reference counting is implemented.

            edmStore.updateExistingEntityType(
                    entityType.getNamespace(),
                    entityType.getName(),
                    entityType.getKey(),
                    entityType.getProperties() );

            //Alter entity type table
            String tableName = tableManager.getTablenameForEntityType( entityType );
            String propertyColumnNames = removableProperties.stream().map( fqn -> Queries.fqnToColumnName( fqn ) )
                    .collect( Collectors.joining( "," ) );

            removableProperties.forEach( propertyTypeFqn -> session.execute( Queries.dropEntityTableIndex(
                    DatastoreConstants.KEYSPACE,
                    tableName,
                    propertyTypeFqn ) ) );

            session.execute( Queries.dropPropertyColumnsFromEntityTable(
                    DatastoreConstants.KEYSPACE,
                    tableName,
                    propertyColumnNames ) );
        }
    }

    @Override
    public void addPropertyTypesToSchema( String namespace, String name, Set<FullQualifiedName> properties ) {
        ensureSchemaExists( namespace, name );
        ensurePropertyTypesExist( properties );

        properties.stream()
                .forEach(
                        propertyTypeFqn -> tableManager.propertyTypeAddSchema( propertyTypeFqn, namespace, name ) );

        for ( UUID aclId : AclContextService.getCurrentContextAclIds() ) {
            session.executeAsync(
                    tableManager.getSchemaAddPropertyTypeStatement( aclId ).bind( properties, namespace, name ) );
        }
    }

    @Override
    public void removePropertyTypesFromSchema( String namespace, String name, Set<FullQualifiedName> properties ) {
        ensureSchemaExists( namespace, name );
        ensurePropertyTypesExist( properties );

        properties.stream()
                .forEach( propertyTypeFqn -> tableManager.propertyTypeRemoveSchema( propertyTypeFqn,
                        namespace,
                        name ) );

        for ( UUID aclId : AclContextService.getCurrentContextAclIds() ) {
            session.executeAsync(
                    tableManager.getSchemaRemovePropertyTypeStatement( aclId ).bind( properties,
                            namespace,
                            name ) );
        }
    }

    /*******************
     * Validation methods
     *******************/
    //TODO: Ho Chung will update these after other methods are finalized.
    @Override
    public void ensureValidEntityType( EntityType entityType ) {
        ensurePropertyTypesExist( entityType.getProperties() );
        entityType.getSchemas().forEach( schema -> ensureSchemaExists( schema.getNamespace(), schema.getName() ) );
    }

    @Override
    public void ensureValidSchema( Schema schema ) {
        ensureEntityTypesExist( schema.getEntityTypeFqns() );
        ensurePropertyTypesExist( schema.getPropertyTypeFqns() );
    }

    @Override
    public void ensureSchemaExists( String namespace, String name ) {
        Preconditions.checkArgument( checkSchemaExists( namespace, name ),
                "Schema " + namespace + "." + name + " does not exist." );
    }

    @Override
    public void ensureEntityTypesExist( Set<FullQualifiedName> entityTypes ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypes ), "Not all entity types exist." );
    }

    @Override
    public void ensureEntityTypeExists( FullQualifiedName entityTypeFqn ) {
        Preconditions.checkArgument( checkEntityTypeExists( entityTypeFqn ),
                "Entity type " + entityTypeFqn + " does not exist." );
    }

    @Override
    public void ensureEntityTypeExists( String typename ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( typename ), "Entity type does not exist." );
    }

    @Override
    public void ensurePropertyTypesExist( Set<FullQualifiedName> propertyTypes ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypes ), "Not all property types exist." );
    }

    @Override
    public void ensurePropertyTypeExists( FullQualifiedName propertyTypeFqn ) {
        Preconditions.checkArgument( checkPropertyTypeExists( propertyTypeFqn ),
                "Property type " + propertyTypeFqn + " does not exist." );
    }

    @Override
    public void ensureEntitySetExists( String typename, String entitySetName ) {
        Preconditions.checkArgument( checkEntitySetExists( typename, entitySetName ),
                "Entity set " + entitySetName + " does not exist." );
    }

    @Override
    public void ensureEntityTypeDoesNotExist( FullQualifiedName entityTypeFqn ) {
        Preconditions.checkArgument( !checkEntityTypeExists( entityTypeFqn ),
                "Entity type of the same name already exists." );
    }

    @Override
    public void ensurePropertyTypeDoesNotExist( FullQualifiedName propertyTypeFqn ) {
        Preconditions.checkArgument( !checkPropertyTypeExists( propertyTypeFqn ),
                "Property type of the same name already exists." );
    }

    @Override
    public void ensureSchemaDoesNotExist( String namespace, String name ) {
        Preconditions.checkArgument( !checkSchemaExists( namespace, name ), "Schema of the same name already exists." );
    }

    @Override
    public void ensureEntitySetDoesNotExist( String typename, String entitySetName ) {
        Preconditions.checkArgument( !checkEntitySetExists( typename, entitySetName ),
                "Entity Set of the same name already exists." );
    }

    /************************************
     * Helper methods to check existence
     ************************************/
    @Override
    public boolean checkEntityTypesExist( Set<FullQualifiedName> entityTypes ) {
        Stream<ResultSetFuture> futures = entityTypes.parallelStream()
                .map( entityType -> session
                        .executeAsync(
                                tableManager.getCountEntityTypesStatement().bind( entityType.getNamespace(),
                                        entityType.getName() ) ) );
        // Cause Java 8
        try {
            return Futures.allAsList( (Iterable<ResultSetFuture>) futures::iterator ).get().stream()
                    .map( rs -> rs.one().getLong( "count" ) )
                    .noneMatch( count -> count == 0 );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to verify all entity types exist." );
            return false;
        }
    }

    @Override
    public boolean checkPropertyTypesExist( Set<FullQualifiedName> properties ) {
        return properties.stream().allMatch( propertyTypes::containsKey );
    }

    @Override
    public boolean checkPropertyTypeExists( FullQualifiedName propertyTypeFqn ) {
        // Ho Chung: this check is easier for single property type
        String typename = tableManager.getTypenameForPropertyType( propertyTypeFqn );
        return StringUtils.isNotBlank( typename );
    }

    @Override
    public boolean checkEntityTypeExists( FullQualifiedName entityTypeFqn ) {
        String typename = tableManager.getTypenameForEntityType( entityTypeFqn );
        return StringUtils.isNotBlank( typename );
    }

    @Override
    public boolean checkEntitySetExists( String name ) {
        EntitySet entitySet = edmStore.getEntitySet( name );
        if ( entitySet != null ) {
            return true;
        }
        return false;
    }

    private boolean checkEntitySetExists( String typename, String name ) {
        return Util
                .isCountNonZero( session.execute( tableManager.getCountEntitySetsStatement().bind( typename, name ) ) );
    }

    @Override
    public boolean checkSchemaExists( String namespace, String name ) {
        return checkSchemaExists( new FullQualifiedName( namespace, name ) );
    }

    private boolean checkSchemaExists( FullQualifiedName schema ) {
        UUID aclId = ACLs.EVERYONE_ACL;
        return ( session.execute(
                tableManager.getSchemaStatement( aclId ).bind( schema.getNamespace(), schema.getName() ) )
                .one() != null );
    }

    @Override
    public void createEntitySet( String typename, String name, String title ) {
        // TODO Auto-generated method stub

    }

    @Override
    public void createEntityType( EntityType objectType ) {
        // TODO Auto-generated method stub

    }

    @Override
    public Collection<PropertyType> getPropertyTypes( Set<FullQualifiedName> properties ) {
        return propertyTypes.getAll( properties ).values();
    }
}
