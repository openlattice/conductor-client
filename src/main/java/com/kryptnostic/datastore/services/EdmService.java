package com.kryptnostic.datastore.services;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.dataloom.edm.internal.*;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.exceptions.BadRequestException;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.requests.GetSchemasRequest.TypeDetails;
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
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.datastore.cassandra.Queries;
import com.kryptnostic.datastore.util.Util;

public class EdmService implements EdmManager {
    private static final Logger         logger = LoggerFactory.getLogger( EdmService.class );

    private final Session               session;
    private final Mapper<EntitySet>     entitySetMapper;
    private final Mapper<EntityType>    entityTypeMapper;
    private final Mapper<PropertyType>  propertyTypeMapper;

    private final CassandraEdmStore     edmStore;
    private final CassandraTableManager tableManager;
    private final PermissionsService    permissionsService;

    public EdmService(
            Session session,
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
     * @see com.kryptnostic.types.services.EdmManager#updateObjectType(com.kryptnostic.types.ObjectType)
     */
    @Override
    public void upsertEntityType( Optional<String> username, EntityType entityType ) {
        // This call will fail if the typename has already been set for the entity.
        ensureValidEntityType( entityType );
        if ( checkEntityTypeExists( entityType.getFullQualifiedName() ) ) {
            // Retrieve database record of entityType
            EntityType dbRecord = getEntityType( entityType.getFullQualifiedName() );
            
            // Update properties
            Set<FullQualifiedName> currentPropertyTypes = dbRecord.getProperties();
            
            Set<FullQualifiedName> removablePropertyTypesInEntityType = Sets.difference( currentPropertyTypes,
                    entityType.getProperties() );
            removePropertyTypesFromEntityType( dbRecord, removablePropertyTypesInEntityType, true );
            
            Set<FullQualifiedName> newPropertyTypesInEntityType = Sets.difference( entityType.getProperties(),
                    currentPropertyTypes );
            addPropertyTypesToEntityType( entityType.getNamespace(),
                    entityType.getName(),
                    newPropertyTypesInEntityType );
            
            // Update Schema
            Set<FullQualifiedName> currentSchemas = dbRecord.getSchemas();
            
            Set<FullQualifiedName> removableSchemas = Sets.difference( currentSchemas, entityType.getSchemas() );
            removableSchemas.forEach( schema -> removeEntityTypesFromSchema( schema.getNamespace(), schema.getName(), ImmutableSet.of( entityType.getFullQualifiedName() ) ) );
            
            Set<FullQualifiedName> newSchemas = Sets.difference( entityType.getSchemas(), currentSchemas );
            newSchemas.forEach( schema -> addEntityTypesToSchema( schema.getNamespace(), schema.getName(), ImmutableSet.of( entityType.getFullQualifiedName() ) ) );
            
            // Persist
            entityTypeMapper.save( entityType );
        } else {
            createEntityType( username, entityType, true );
        }
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createPropertyType(com.kryptnostic.types.PropertyType) update
     * propertyType (and return true upon success) if exists, return false otherwise
     */
    @Override
    public void upsertPropertyType( PropertyType propertyType ) {
        ensureValidPropertyType( propertyType );
        if ( checkPropertyTypeExists( propertyType.getFullQualifiedName() ) ) {
            // Retrieve database record of property type
            PropertyType dbRecord = getPropertyType( propertyType.getFullQualifiedName() );
            
            // Update Schema
            Set<FullQualifiedName> currentSchemas = dbRecord.getSchemas();
            
            Set<FullQualifiedName> removableSchemas = Sets.difference( currentSchemas, propertyType.getSchemas() );
            removableSchemas.forEach( schema -> removeEntityTypesFromSchema( schema.getNamespace(), schema.getName(), ImmutableSet.of( propertyType.getFullQualifiedName() ) ) );
            
            Set<FullQualifiedName> newSchemas = Sets.difference( propertyType.getSchemas(), currentSchemas );
            newSchemas.forEach( schema -> addEntityTypesToSchema( schema.getNamespace(), schema.getName(), ImmutableSet.of( propertyType.getFullQualifiedName() ) ) ); 
            
            propertyTypeMapper.save( propertyType );
            //TODO: Need to alter table?
        } else {
            createPropertyType( propertyType, true );
        }
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createNamespace(com.kryptnostic.types.Namespace)
     */
    @Override
    public void upsertSchema( Schema schema ) {
        // Retrieve database record of entityType
        Schema dbRecord = getSchema( schema.getNamespace(),
                schema.getNamespace(),
                EnumSet.noneOf( TypeDetails.class ) );

        // Update entity types
        Set<FullQualifiedName> currentEntityTypes = dbRecord.getEntityTypeFqns();

        Set<FullQualifiedName> removableEntityTypes = Sets.difference( currentEntityTypes, schema.getEntityTypeFqns() );
        removeEntityTypesFromSchema( schema.getNamespace(), schema.getName(), removableEntityTypes );

        Set<FullQualifiedName> newEntityTypes = Sets.difference( schema.getEntityTypeFqns(), currentEntityTypes );
        addEntityTypesToSchema( schema.getNamespace(), schema.getName(), newEntityTypes );

        // Update property types
        Set<FullQualifiedName> currentPropertyTypes = dbRecord.getPropertyTypeFqns();

        Set<FullQualifiedName> removablePropertyTypes = Sets.difference( currentPropertyTypes,
                schema.getPropertyTypeFqns() );
        removePropertyTypesFromSchema( schema.getNamespace(), schema.getName(), removablePropertyTypes );

        Set<FullQualifiedName> newPropertyTypes = Sets.difference( schema.getPropertyTypeFqns(), currentPropertyTypes );
        addPropertyTypesToSchema( schema.getNamespace(), schema.getName(), newPropertyTypes );

        // Persist
        session.execute( tableManager.getSchemaUpsertStatement( schema.getAclId() ).bind( schema.getNamespace(),
                schema.getName(),
                schema.getEntityTypeFqns(),
                schema.getPropertyTypeFqns() ) );
    }

    @Override
    public void createPropertyType( PropertyType propertyType ) {
        ensureValidPropertyType( propertyType );
        Preconditions.checkArgument( !checkPropertyTypeExists( propertyType.getFullQualifiedName() ),
                "Property Type of same name exists." );
        createPropertyType( propertyType, true );
    }

    private void createPropertyType( PropertyType propertyType, boolean isValid ) {
        /*
         * We retrieve or create the typename for the property. If the property already exists then lightweight
         * transaction will fail and return value will be correctly set.
         */
        /**
         * Refactored by Ho Chung, so that upsertPropertyType won't do duplicate checks. isValid is true if property
         * type is checked valid, and checked not already exist.
         */
        if ( isValid ) {
            boolean propertyCreated = false;
            propertyType.setTypename( Queries.fqnToColumnName( propertyType.getFullQualifiedName() ) );
            propertyCreated = Util.wasLightweightTransactionApplied(
                    edmStore.createPropertyTypeIfNotExists( propertyType.getNamespace(),
                            propertyType.getName(),
                            propertyType.getTypename(),
                            propertyType.getDatatype(),
                            propertyType.getMultiplicity(),
                            propertyType.getSchemas() ) );

            if ( propertyCreated ) {
                propertyType.getSchemas()
                        .forEach(
                                schemaFqn -> addPropertyTypesToSchema( schemaFqn.getNamespace(),
                                        schemaFqn.getName(),
                                        ImmutableSet.of( propertyType.getFullQualifiedName() ) ) );

                // tableManager.createPropertyTypeTable( propertyType );
                tableManager.insertToPropertyTypeLookupTable( propertyType );
            } else {
                throw new IllegalStateException( "Failed to create property type." );
            }
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
            tableManager.deleteEntityTypeTable( entityType.getNamespace(), entityType.getName() );
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
            EntityType entityType ) {
        // Make sure entity type is valid
        ensureValidEntityType( entityType );
        Preconditions.checkArgument( !checkEntityTypeExists( entityType.getFullQualifiedName() ),
                "Entity type of same name already exists." );
        createEntityType( Optional.absent(), entityType, true );
    }

    @Override
    public void createEntityType(
            Optional<String> username,
            EntityType entityType ) {
        // Make sure entity type is valid
        ensureValidEntityType( entityType );
        Preconditions.checkArgument( !checkEntityTypeExists( entityType.getFullQualifiedName() ),
                "Entity type of same name already exists." );

        createEntityType( username, entityType, true );
    }

    private boolean createEntityType( Optional<String> username, EntityType entityType, boolean isValid ) {
        /**
         * Refactored by Ho Chung, so that upsertEntityType won't do duplicate checks. checkedValid means that isValid
         * is true if entity type is checked valid, and checked not already exist.
         */
        if ( isValid ) {
            boolean entityCreated = false;
            // Generate the typename for this type
            String typename = Queries.fqnToColumnName( entityType.getFullQualifiedName() );// CassandraTableManager.generateTypename();
            entityType.setTypename( typename );

            entityCreated = Util.wasLightweightTransactionApplied(
                    edmStore.createEntityTypeIfNotExists( entityType.getNamespace(),
                            entityType.getName(),
                            entityType.getTypename(),
                            entityType.getKey(),
                            entityType.getProperties(),
                            entityType.getSchemas() ) );

            entityType.getSchemas()
                    .forEach(
                            schemaFqn -> addEntityTypesToSchema( schemaFqn.getNamespace(),
                                    schemaFqn.getName(),
                                    ImmutableSet.of( entityType.getFullQualifiedName() ) ) );

            // Only create entity table if insert transaction succeeded.
            if ( entityCreated ) {
                tableManager.createEntityTypeTable( entityType,
                        Maps.asMap( entityType.getKey(),
                                fqn -> getPropertyType( fqn ) ) );
                tableManager.insertToEntityTypeLookupTable( entityType );
            }
            return entityCreated;
        }
        return false;
    }

    @Override
    public void upsertEntitySet( EntitySet entitySet ) {
        entitySetMapper.save( entitySet );
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
            permissionsService.removePermissionsRequestForEntitySet( entitySetName );
            
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
        Preconditions.checkArgument( !checkEntitySetExists( typename, name ), "Entity set already exists." );

        boolean isCreated = Util
                .wasLightweightTransactionApplied( edmStore.createEntitySetIfNotExists( typename, name, title ) );
        if ( !isCreated ) {
            throw new IllegalStateException( "Failed to create entity set." );
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
    public void createEntitySet( Optional<String> username, EntitySet entitySet ) {
        try {
            createEntitySet( entitySet );

            if ( username.isPresent() ) {
                tableManager.addOwnerForEntitySet( entitySet.getName(), username.get() );

                EntityType entityType = entityTypeMapper.get( entitySet.getType().getNamespace(),
                        entitySet.getType().getName() );
                permissionsService.addPermissionsForEntitySet( new Principal( PrincipalType.USER, username.get() ),
                        entitySet.getName(),
                        EnumSet.allOf( Permission.class ) );
                entityType.getProperties()
                        .forEach( propertyTypeFqn -> permissionsService.addPermissionsForPropertyTypeInEntitySet(
                                new Principal( PrincipalType.USER, username.get() ),
                                entitySet.getName(),
                                propertyTypeFqn,
                                EnumSet.allOf( Permission.class ) ) );

            }
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

        Set<FullQualifiedName> newProperties = ImmutableSet.copyOf( Sets.difference( properties, entityType.getProperties() ) );

        if( newProperties == null || newProperties.size() == 0 ){
            return;
        }

        entityType.addProperties( newProperties );
        edmStore.updateExistingEntityType(
                entityType.getNamespace(),
                entityType.getName(),
                entityType.getKey(),
                entityType.getProperties() );

        String propertiesWithType = newProperties.stream().map( fqn ->
                tableManager.getTypenameForPropertyType( fqn ) + " " + CassandraEdmMapping.getCassandraTypeName( tableManager.getPropertyType( fqn ).getDatatype() )
        ).collect( Collectors.joining(","));

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

            if ( properties != null && entityType.getProperties() != null && entityType.getProperties().containsAll( properties ) ) {
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

            String propertyColumnNames = properties.stream().map( fqn ->
                    Queries.fqnToColumnName( fqn )
            ).collect( Collectors.joining(",") );

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
        Stream<ResultSetFuture> futures = properties.parallelStream()
                .map( prop -> session
                        .executeAsync(
                                tableManager.getCountPropertyStatement().bind( prop.getNamespace(),
                                        prop.getName() ) ) );
        // Cause Java 8
        try {
            return Futures.allAsList( (Iterable<ResultSetFuture>) futures::iterator ).get().stream()
                    .map( rs -> rs.one().getLong( "count" ) )
                    .noneMatch( count -> count == 0 );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to verify all properties exist." );
            return false;
        }
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
