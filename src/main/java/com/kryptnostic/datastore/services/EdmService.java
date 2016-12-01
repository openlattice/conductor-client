package com.kryptnostic.datastore.services;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
        Preconditions.checkArgument( StringUtils.isNotBlank( namespace ), "Schema namespace cannot be blank." );

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
        Preconditions.checkArgument( StringUtils.isNotBlank( namespace ), "Schema namespace cannot be blank." );
        Preconditions.checkArgument( StringUtils.isNotBlank( name ), "Schema name cannot be blank." );

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
     * @see com.kryptnostic.types.services.EdmManager#createNamespace(com.kryptnostic.types.Namespace)
     */
    @Override
    public void upsertSchema( Schema schema ) {
        session.execute( tableManager.getSchemaUpsertStatement( schema.getAclId() ).bind( schema.getNamespace(),
                schema.getName(),
                schema.getEntityTypeFqns(),
                schema.getPropertyTypeFqns() ) );
    }

    @Override
    public void createPropertyType( PropertyType propertyType ) {
        ensureValidPropertyType( propertyType );
        /*
         * We retrieve or create the typename for the property. If the property already exists then lightweight
         * transaction will fail and return value will be correctly set.
         */
        /**
         * Refactored by Ho Chung, so that upsertPropertyType won't do duplicate checks. isValid is true if property
         * type is checked valid, and checked not already exist.
         */
        propertyType.setTypename( Queries.fqnToColumnName( propertyType.getFullQualifiedName() ) );

        if ( propertyTypes.putIfAbsent( propertyType.getFullQualifiedName(), propertyType ) == null ) {
            propertyType.getSchemas()
                    .forEach(
                            schemaFqn -> addPropertyTypesToSchema( schemaFqn.getNamespace(),
                                    schemaFqn.getName(),
                                    ImmutableSet.of( propertyType.getFullQualifiedName() ) ) );

            tableManager.insertToPropertyTypeLookupTable( propertyType );
        } else {
            throw new IllegalStateException( "Failed to create property type." );
        }
    }

    @Override
    public void createSchema(
            String namespace,
            String name,
            UUID aclId,
            Set<FullQualifiedName> entityTypes,
            Set<FullQualifiedName> propertyTypes ) {
        boolean created = false;
        tableManager.createSchemaTableForAclId( aclId );

        entityTypes.stream()
                .forEach( entityTypeFqn -> tableManager.entityTypeAddSchema( entityTypeFqn, namespace, name ) );

        propertyTypes.stream()
                .forEach( propertyTypeFqn -> tableManager.propertyTypeAddSchema( propertyTypeFqn, namespace, name ) );

        created = Util.wasLightweightTransactionApplied(
                session.execute(
                        tableManager.getSchemaInsertStatement( aclId )

                                .bind( namespace, name, entityTypes, propertyTypes ) ) );
        if ( !created ) {
            throw new IllegalStateException( "Failed to create schema." );
        }
    }

    @Override
    public void createSchema( String namespace, String name, UUID aclId, Set<FullQualifiedName> entityTypes ) {
        Set<FullQualifiedName> propertyTypes = entityTypes.stream()
                .map( entityTypeFqn -> entityTypeMapper.get( entityTypeFqn.getNamespace(), entityTypeFqn.getName() ) )
                .map( entityType -> entityType.getProperties() )
                .reduce( ( left, right ) -> {
                    left.addAll( right );
                    return left;
                } ).get();
        createSchema( namespace, name, aclId, entityTypes, propertyTypes );
    }

    @Override
    public void deleteEntityType( FullQualifiedName entityTypeFqn ) {

        EntityType entityType = getEntityType( entityTypeFqn );

        try {
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
        } catch ( Exception e ) {
            throw new IllegalStateException( "Deletion of Entity Type failed." );
        }
    }

    @Override
    public void deletePropertyType( FullQualifiedName propertyTypeFqn ) {
        PropertyType propertyType = getPropertyType( propertyTypeFqn );

        try {
            propertyType.getSchemas().forEach( schemaFqn -> removePropertyTypesFromSchema( schemaFqn.getNamespace(),
                    schemaFqn.getName(),
                    ImmutableSet.of( propertyTypeFqn ) ) );
            getEntityTypes().forEach( entityType -> {
                removePropertyTypesFromEntityType( entityType, ImmutableSet.of( propertyTypeFqn ) );
            } );

            tableManager.deleteFromPropertyTypeLookupTable( propertyType );

            propertyTypeMapper.delete( propertyType );
        } catch ( Exception e ) {
            throw new IllegalStateException( "Deletion of Entity Type failed." );
        }
    }

    @Override
    public void deleteSchema( Schema namespaces ) {
        // TODO: Implement delete schema
    }

    @Override
    public void addEntityTypesToSchema( String namespace, String name, Set<FullQualifiedName> entityTypes ) {
        Preconditions.checkNotNull( getSchema( namespace, name, ImmutableSet.of() ), "Schema does not exist." );
        for ( FullQualifiedName fqn : entityTypes ) {
            Preconditions.checkNotNull( checkEntityTypeExists( fqn ), "Entity Type " + fqn + " does not exist." );
        }
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
        Preconditions.checkNotNull( checkSchemaExists( namespace, name ), "Schema does not exist." );
        for ( FullQualifiedName fqn : entityTypes ) {
            Preconditions.checkNotNull( checkEntityTypeExists( fqn ), "Entity Type " + fqn + " does not exist." );
        }
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
        // Make sure entity type is valid
        ensureValidEntityType( entityType );
        /**
         * Refactored by Ho Chung, so that upsertEntityType won't do duplicate checks. checkedValid means that isValid
         * is true if entity type is checked valid, and checked not already exist.
         */
        // Generate the typename for this type
        String typename = Queries.fqnToColumnName( entityType.getFullQualifiedName() );// CassandraTableManager.generateTypename();
        entityType.setTypename( typename );

        // Only create entity table if insert transaction succeeded.
        final EntityType existing = entityTypes.putIfAbsent( entityType.getFullQualifiedName(), entityType );
        if ( existing == null ) {
            tableManager.createEntityTypeTable( entityType,
                    Maps.asMap( entityType.getKey(),
                            fqn -> getPropertyType( fqn ) ) );
            entityType.getSchemas().forEach( schema -> addEntityTypesToSchema( schema.getNamespace(),
                    schema.getName(),
                    ImmutableSet.of( entityType.getFullQualifiedName() ) ) );
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
            addPropertyTypesToEntityType( entityType.getNamespace(),
                    entityType.getName(),
                    newPropertyTypesInEntityType );
        }
    }

    @Override
    public void upsertEntitySet( EntitySet entitySet ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( entitySet.getTypename() ),
                "Cannot store an entity set with a blank typename" );
        entitySets.set( entitySet.getName(), entitySet );
        // entitySetMapper.save( entitySet );
        // TODO: Figure out a better way to response HttpStatus code or cleanup cassandra after unit test
        // else {
        // throw new InternalError();
        // }
    }

    @Override
    public void deleteEntitySet( EntitySet entitySet ) {
        deleteEntitySet( entitySet.getName() );
    }

    @Override
    public void deleteEntitySet( String entitySetName ) {
        EntitySet entitySet = getEntitySet( entitySetName );

        try {
            // Acls removal
            permissionsService.removePermissionsForEntitySet( entitySetName );
            permissionsService.removePermissionsForPropertyTypeInEntitySet( entitySetName );

            entitySetMapper.delete( entitySet );
        } catch ( Exception e ) {
            throw new IllegalStateException( "Deletion of Entity Set failed." );
        }
    }

    @Override
    public void assignEntityToEntitySet( UUID entityId, String name ) {
        String typename = tableManager.getTypenameForEntityId( entityId );
        Preconditions.checkArgument( StringUtils.isNotBlank( typename ), "Entity type not found." );
        Preconditions.checkArgument( checkEntitySetExists( typename, name ), "Entity set does not exist." );

        boolean assigned = tableManager.assignEntityToEntitySet( entityId, typename, name );
        if ( !assigned ) {
            throw new IllegalStateException( "Failed to assign entity to entity set." );
        }
    }

    @Override
    public void assignEntityToEntitySet( UUID entityId, EntitySet es ) {
        assignEntityToEntitySet( entityId, es.getName() );
    }

    @Override
    public void createEntitySet( FullQualifiedName type, String name, String title ) {
        String typename = tableManager.getTypenameForEntityType( type );
        createEntitySet( typename, name, title );
    }

    @Override
    public void createEntitySet( String typename, String name, String title ) {
        if ( entitySets.putIfAbsent( name,
                new EntitySet().setName( name ).setTitle( title ).setTypename( typename ) ) != null ) {
            throw new IllegalStateException( "Entity set already exists." );
        }
    }

    @Override
    public void createEntitySet( EntitySet entitySet ) {
        Preconditions.checkArgument( StringUtils.isBlank( entitySet.getTypename() ),
                "Entity Set Typename should not be provided." );

        String typename = tableManager.getTypenameForEntityType( entitySet.getType() );
        System.out.println( "typename upon entity set creation: " + typename );
        entitySet.setTypename( typename );

        createEntitySet( typename, entitySet.getName(), entitySet.getTitle() );
    }

    @Override
    public void createEntitySet( Principal principal, EntitySet entitySet ) {
        try {
            createEntitySet( entitySet );
            Principals.ensureUser( principal );
            tableManager.addOwnerForEntitySet( entitySet.getName(), principal.getName() );

            EntityType entityType = entityTypeMapper.get( entitySet.getType().getNamespace(),
                    entitySet.getType().getName() );
            permissionsService.addPermissionsForEntitySet( principal,
                    entitySet.getName(),
                    EnumSet.allOf( Permission.class ) );
            entityType.getProperties()
                    .forEach( propertyTypeFqn -> permissionsService.addPermissionsForPropertyTypeInEntitySet(
                            principal,
                            entitySet.getName(),
                            propertyTypeFqn,
                            EnumSet.allOf( Permission.class ) ) );

        } catch ( Exception e ) {
            throw new IllegalStateException( "Entity Set not created." );
        }
    }

    @Override
    public EntityType getEntityType( FullQualifiedName entityTypeFqn ) {

        return Preconditions.checkNotNull(
                entityTypeMapper.get( entityTypeFqn.getNamespace(), entityTypeFqn.getName() ),
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
        EntitySet entitySet = Preconditions.checkNotNull( edmStore.getEntitySet( name ), "Entity Set does not exist" );
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
    public Iterable<EntitySet> getEntitySetsUserOwns( String username ) {
        return StreamSupport.stream( getEntitySetNamesUserOwns( username ).spliterator(), false )
                .map( entitySetName -> getEntitySet( entitySetName ) )
                .collect( Collectors.toList() );
    }

    @Override
    public Iterable<String> getEntitySetNamesUserOwns( String username ) {
        return tableManager.getEntitySetsUserOwns( username );
    }

    @Override
    public PropertyType getPropertyType( FullQualifiedName propertyType ) {
        return Preconditions.checkNotNull(
                propertyTypeMapper.get( propertyType.getNamespace(), propertyType.getName() ),
                "Property type does not exist" );
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
        Preconditions.checkArgument( checkPropertyTypesExist( properties ), "Some properties do not exist." );

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

        String propertiesWithType = newProperties.stream()
                .map( fqn -> tableManager.getTypenameForPropertyType( fqn ) + " "
                        + CassandraEdmMapping
                                .getCassandraTypeName( propertyTypes.get( fqn ).getDatatype() ) )
                .collect( Collectors.joining( "," ) );

        session.execute( Queries.addPropertyColumnsToEntityTable(
                DatastoreConstants.KEYSPACE,
                tableManager.getTablenameForEntityType( new FullQualifiedName( namespace, name ) ),
                propertiesWithType ) );

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
        Preconditions.checkArgument( checkPropertyTypesExist( properties ), "Some properties do not exist." );
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

            if ( properties != null && entityType.getProperties() != null
                    && entityType.getProperties().containsAll( properties ) ) {
                entityType.removeProperties( properties );
                // Acl
                properties
                        .forEach( propertyTypeFqn -> permissionsService.removePermissionsForPropertyTypeInEntityType(
                                entityType.getFullQualifiedName(), propertyTypeFqn ) );
            } else {
                throw new IllegalArgumentException( "Not all properties are included in the EntityType" );
            }
            // TODO: Remove properties from Schema, once reference counting is implemented.

            edmStore.updateExistingEntityType(
                    entityType.getNamespace(),
                    entityType.getName(),
                    entityType.getKey(),
                    entityType.getProperties() );

            String propertyColumnNames = properties.stream().map( fqn -> Queries.fqnToColumnName( fqn ) )
                    .collect( Collectors.joining( "," ) );

            session.execute( Queries.dropPropertyColumnsFromEntityTable(
                    DatastoreConstants.KEYSPACE,
                    tableManager.getTablenameForEntityType( entityType ),
                    propertyColumnNames ) );
        }
    }

    @Override
    public void addPropertyTypesToSchema( String namespace, String name, Set<FullQualifiedName> properties ) {
        Preconditions.checkArgument( checkPropertyTypesExist( properties ), "Some properties do not exist." );
        Preconditions.checkArgument( checkSchemaExists( namespace, name ), "Schema does not exist." );

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
        Preconditions.checkArgument( checkPropertyTypesExist( properties ), "Some properties do not exist." );
        Preconditions.checkArgument( checkSchemaExists( namespace, name ), "Schema does not exist." );

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

    /**************
     * Validation
     **************/
    private void ensureValidEntityType( EntityType entityType ) {
        try {
            Preconditions.checkNotNull( entityType.getNamespace(), "Namespace for Entity Type is missing" );
            Preconditions.checkNotNull( entityType.getName(), "Name of Entity Type is missing" );
            Preconditions.checkNotNull( entityType.getProperties(), "Property for Entity Type is missing" );
            Preconditions.checkNotNull( entityType.getKey(), "Key for Entity Type is missing" );
            Preconditions.checkArgument( checkPropertyTypesExist( entityType.getProperties() )
                    && entityType.getProperties().containsAll( entityType.getKey() ), "Invalid Entity Type provided" );
        } catch ( Exception e ) {
            throw new IllegalArgumentException( e.getMessage() );
        }
    }

    private void ensureValidPropertyType( PropertyType propertyType ) {
        try {
            Preconditions.checkNotNull( propertyType.getNamespace(), "Namespace for Property Type is missing" );
            Preconditions.checkNotNull( propertyType.getName(), "Name of Property Type is missing" );
            Preconditions.checkNotNull( propertyType.getDatatype(), "Datatype of Property Type is missing" );
            Preconditions.checkNotNull( propertyType.getMultiplicity(), "Multiplicity of Property Type is missing" );
        } catch ( Exception e ) {
            throw new IllegalArgumentException( e.getMessage() );
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
}
