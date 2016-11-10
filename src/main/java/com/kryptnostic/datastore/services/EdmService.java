package com.kryptnostic.datastore.services;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpServerErrorException;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.Principal;
import com.kryptnostic.datastore.PrincipalType;
import com.kryptnostic.datastore.exceptions.BadRequestException;
import com.kryptnostic.datastore.services.requests.GetSchemasRequest.TypeDetails;
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
        List<Schema> schemas = ImmutableList.copyOf( getSchemas() );
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

        return Iterables.filter( Iterables.concat( results ), Predicates.notNull() );
    }

    @Override
    public Iterable<Schema> getSchemasInNamespace( String namespace, Set<TypeDetails> requestedDetails ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( namespace ), "Namespace cannot be blank." );

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

        return Iterables.filter( Iterables.concat( results ), Predicates.notNull() );
    }

    @Override
    public Iterable<Schema> getSchema( String namespace, String name, Set<TypeDetails> requestedDetails ) {
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

        return Iterables.filter( Iterables.concat( results ), Predicates.notNull() );
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
            // Retrieve properties known to user
            Set<FullQualifiedName> currentPropertyTypes = dbRecord.getProperties();
            // Remove the removable property types in database properly; this step takes care of removal of
            // permissions
            Set<FullQualifiedName> removablePropertyTypesInEntityType = Sets.difference( currentPropertyTypes,
                    entityType.getProperties() );
            removePropertyTypesFromEntityType( dbRecord, removablePropertyTypesInEntityType, true );
            // Add the new property types in
            Set<FullQualifiedName> newPropertyTypesInEntityType = Sets.difference( entityType.getProperties(),
                    currentPropertyTypes );
            addPropertyTypesToEntityType( entityType.getNamespace(),
                    entityType.getName(),
                    newPropertyTypesInEntityType );
        } else {
            createEntityType( username, entityType, true, true );
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
            Util.wasLightweightTransactionApplied(
                    edmStore.updatePropertyTypeIfExists(
                            propertyType.getDatatype(),
                            propertyType.getMultiplicity(),
                            propertyType.getSchemas(),
                            propertyType.getNamespace(),
                            propertyType.getName() ) );
        } else {
            createPropertyType( propertyType, true, true );
        }
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
        createPropertyType( propertyType,
                true,
                !checkPropertyTypeExists( propertyType.getFullQualifiedName() ) );
    }

    private void createPropertyType( PropertyType propertyType, boolean checkedValid, boolean doesNotExist ) {
        /*
         * We retrieve or create the typename for the property. If the property already exists then lightweight
         * transaction will fail and return value will be correctly set.
         */
        /**
         * Refactored by Ho Chung, so that upsertPropertyType won't do duplicate checks. checkedValid means that
         * ensureValidPropertyType is run; error throwing should be done there.
         */
        if ( checkedValid && doesNotExist ) {
            boolean propertyCreated = false;
            propertyType.setTypename( CassandraTableManager.generateTypename() );
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

                tableManager.createPropertyTypeTable( propertyType );
                tableManager.insertToPropertyTypeLookupTable( propertyType );
            } else {
                propertyCreated = true;
            }

            if ( !propertyCreated ) {
                throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR );
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
        // TODO: Figure out a better way to response HttpStatus code or cleanup cassandra after unit test
        // if ( !created ) {
        // throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR );
        // }
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
        EntityType entityType = entityTypeMapper.get( entityTypeFqn.getNamespace(), entityTypeFqn.getName() );

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
        PropertyType propertyType = propertyTypeMapper
                .get( propertyTypeFqn.getNamespace(), propertyTypeFqn.getName() );

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
        Set<FullQualifiedName> propertyTypes = new HashSet<>();

        entityTypes.stream()
                .map( entityTypeFqn -> entityTypeMapper.get( entityTypeFqn.getNamespace(), entityTypeFqn.getName() ) )
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
        // TODO: propertyTypes not removed From Schema table when Entity Types are removed. Need reference counting on
        // propertyTypes to do so.
        Set<FullQualifiedName> propertyTypes = new HashSet<>();

        entityTypes.stream()
                .map( entityTypeFqn -> entityTypeMapper.get( entityTypeFqn.getNamespace(), entityTypeFqn.getName() ) )
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

        createEntityType( Optional.absent(),
                entityType,
                true,
                !checkEntityTypeExists( entityType.getFullQualifiedName() ) );

    }
    
    @Override
    public void createEntityType(
            Optional<String> username,
            EntityType entityType ) {
        // Make sure entity type is valid
        ensureValidEntityType( entityType );

        createEntityType( username,
                entityType,
                true,
                !checkEntityTypeExists( entityType.getFullQualifiedName() ) );

    }

    private boolean createEntityType( Optional<String> username, EntityType entityType, boolean checkedValid, boolean doesNotExist ) {
        /**
         * Refactored by Ho Chung, so that upsertEntityType won't do duplicate checks. checkedValid means that
         * ensureValidEntityType is run; error throwing should be done there.
         */
        if ( checkedValid && doesNotExist ) {
            boolean entityCreated = false;
            // Generate the typename for this type
            String typename = CassandraTableManager.generateTypename();
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

                if( username.isPresent() ){
                    createDefaultEntitySet( username.get(), entityType );
                } else {
                    createDefaultEntitySet( entityType );
                }
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
        EntitySet entitySet = EdmDetailsAdapter.setEntitySetTypename( tableManager, edmStore.getEntitySet( entitySetName ) );
        // Acls removal
        permissionsService.removePermissionsForEntitySet( entitySetName );
        permissionsService.removePermissionsForPropertyTypeInEntitySet( entitySetName );

        entitySetMapper.delete( entitySet );
    }

    @Override
    public void assignEntityToEntitySet( UUID entityId, String name ) {
        boolean assigned = false;
        String typename = tableManager.getTypenameForEntityId( entityId );
        if ( StringUtils.isBlank( typename ) ) {
            throw new BadRequestException( "Entity type not found." );
        }
        if ( !isExistingEntitySet( typename, name ) ) {
            throw new BadRequestException( "Entity set does not exist." );
        }
        assigned = tableManager.assignEntityToEntitySet( entityId, typename, name );
        if ( !assigned ) {
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR );
        }
        tableManager.assignEntityToEntitySet( entityId, typename, name );
        // return tableManager.assignEntityToEntitySet( entityId, typename, name );
        // return false;
    }

    @Override
    public void assignEntityToEntitySet( UUID entityId, EntitySet es ) {
        assignEntityToEntitySet( entityId, es.getName() );
    }

    private void createDefaultEntitySet( EntityType entityType ) {
        String typename = tableManager.getTypenameForEntityType( entityType.getFullQualifiedName() );
        String name = tableManager.getNameForDefaultEntitySet( typename );
        String title = "Default Entity Set for the entity type with typename " + typename;
        createEntitySet( typename, name, title );
    }
    
    private void createDefaultEntitySet( String username, EntityType entityType ) {
        String typename = tableManager.getTypenameForEntityType( entityType.getFullQualifiedName() );
        String name = tableManager.getNameForDefaultEntitySet( typename );
        String title = "Default Entity Set for the entity type with typename " + typename;
        createEntitySet( username, typename, name, title );
    }


    @Override
    public boolean createEntitySet( FullQualifiedName type, String name, String title ) {
        String typename = tableManager.getTypenameForEntityType( type );
        return createEntitySet( typename, name, title );
    }
    
    @Override
    public boolean createEntitySet( String typename, String name, String title ) {
        // TODO: clean up unit tests to follow the pattern and figure out a better to return this HttpStatus
        // if ( isExistingEntitySet( typename, name ) ) {
        // throw new BadRequestException( "Entity set already exists." );
        // }
        return Util.wasLightweightTransactionApplied( edmStore.createEntitySetIfNotExists( typename, name, title ) );
    }

    private boolean createEntitySet( String username, String typename, String name, String title ) {
        boolean entitySetCreated = createEntitySet( typename, name, title );
        
        if( entitySetCreated ){
            tableManager.addOwnerForEntitySet( name, username );
        }
        
        return entitySetCreated;
    }
    
    @Override
    public boolean createEntitySet( EntitySet entitySet ) {
        if ( StringUtils.isNotBlank( entitySet.getTypename() ) ) {
            throw new BadRequestException( "Typename is not provided." );
        }
        String typename = tableManager.getTypenameForEntityType( entitySet.getType() );
        System.out.println( "typename upon entity set creation: " + typename );
        entitySet.setTypename( typename );
        return createEntitySet( typename, entitySet.getName(), entitySet.getTitle() );
    }

    
    @Override
    public boolean createEntitySet( Optional<String> username, EntitySet entitySet){
        boolean entitySetCreated = createEntitySet( entitySet );
        if( entitySetCreated && username.isPresent()){
            tableManager.addOwnerForEntitySet( entitySet.getName(), username.get() );
            
            EntityType entityType = entityTypeMapper.get( entitySet.getType().getNamespace(), entitySet.getType().getName() );
            permissionsService.addPermissionsForEntitySet( new Principal( PrincipalType.USER, username.get()), entitySet.getName(), EnumSet.allOf( Permission.class ) );
            entityType.getProperties().forEach( propertyTypeFqn -> 
                permissionsService.addPermissionsForPropertyTypeInEntitySet( new Principal( PrincipalType.USER, username.get()), entitySet.getName(), propertyTypeFqn, EnumSet.allOf( Permission.class ) )
                    );
            
       }
        return entitySetCreated;
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
        return EdmDetailsAdapter.setEntitySetTypename( tableManager, edmStore.getEntitySet( name ) );
    }

    private Iterable<EntitySet> getEntitySetsForEntityType( FullQualifiedName type ) {
        // Returns ALL entity sets of an entity type, viewable or not.
        // Used for deletion of all entity sets when deleting an entity type.
        String typename = tableManager.getTypenameForEntityType( type.getNamespace(), type.getName() );
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
    public Iterable<String> getEntitySetNamesUserOwns( String username ){
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
        if ( fqn != null ) {
            return fqn;
        }
        return null;
    }

    @Override
    public FullQualifiedName getEntityTypeFullQualifiedName( String typename ) {
        FullQualifiedName fqn = tableManager.getEntityTypeForTypename( typename );
        if ( fqn != null ) {
            return fqn;
        }
        return null;
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
    public boolean isExistingEntitySet( String name ) {
        EntitySet entitySet = edmStore.getEntitySet( name );
        if ( entitySet != null ) {
            return true;
        }
        return false;
    }

    private boolean isExistingEntitySet( String typename, String name ) {
        return Util
                .isCountNonZero( session.execute( tableManager.getCountEntitySetsStatement().bind( typename, name ) ) );
    }

    @Override
    public void addPropertyTypesToEntityType( String namespace, String name, Set<FullQualifiedName> properties ) {
        EntityType entityType;

        try {
            entityType = getEntityType( namespace, name );
            Preconditions.checkArgument( checkPropertyTypesExist( properties ), "Some properties do not exist." );
        } catch ( IllegalArgumentException e ) {
            throw new BadRequestException( "Illegal Arguments are provided." );
        }

        Set<FullQualifiedName> newProperties = Sets.difference( properties, entityType.getProperties() );
        entityType.addProperties( newProperties );
        edmStore.updateExistingEntityType(
                entityType.getNamespace(),
                entityType.getName(),
                entityType.getKey(),
                entityType.getProperties() );

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
        try {
            Preconditions.checkArgument( checkPropertyTypesExist( properties ), "Some properties do not exist." );

            removePropertyTypesFromEntityType( entityType, properties, true );
        } catch ( IllegalArgumentException e ) {
            throw new BadRequestException( "Illegal Arguments are provided." );
        }
    }

    private void removePropertyTypesFromEntityType(
            EntityType entityType,
            Set<FullQualifiedName> properties,
            boolean checkedValid ) {
        /**
         * Refactored by Ho Chung, to avoid duplicate checks. checkedValid means that entityType is checked to be valid,
         * and property types are checked to exist; error throwing should be done there.
         */
        if ( checkedValid ) {

            if ( properties != null && entityType.getProperties() != null ) {
                entityType.removeProperties( properties );
                // Acl
                properties
                        .forEach( propertyTypeFqn -> permissionsService.removePermissionsForPropertyTypeInEntityType(
                                entityType.getFullQualifiedName(), propertyTypeFqn ) );
            }
            // TODO: Remove properties from Schema, once reference counting is implemented.

            edmStore.updateExistingEntityType(
                    entityType.getNamespace(),
                    entityType.getName(),
                    entityType.getKey(),
                    entityType.getProperties() );

        }
    }

    @Override
    public void addPropertyTypesToSchema( String namespace, String name, Set<FullQualifiedName> properties ) {
        try {
            Preconditions.checkArgument( checkPropertyTypesExist( properties ), "Some properties do not exist." );
            Preconditions.checkArgument( schemaExists( namespace, name ), "Schema does not exist." );

            properties.stream()
                    .forEach(
                            propertyTypeFqn -> tableManager.propertyTypeAddSchema( propertyTypeFqn, namespace, name ) );

            for ( UUID aclId : AclContextService.getCurrentContextAclIds() ) {
                session.executeAsync(
                        tableManager.getSchemaAddPropertyTypeStatement( aclId ).bind( properties, namespace, name ) );
            }
        } catch ( IllegalArgumentException e ) {
            throw new BadRequestException( "Illegal Arguments are provided." );
        }
    }

    @Override
    public void removePropertyTypesFromSchema( String namespace, String name, Set<FullQualifiedName> properties ) {
        try {
            Preconditions.checkArgument( checkPropertyTypesExist( properties ), "Some properties do not exist." );
            Preconditions.checkArgument( schemaExists( namespace, name ), "Schema does not exist." );

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
        } catch ( IllegalArgumentException e ) {
            throw new BadRequestException( "Illegal Arguments are provided." );
        }
    }

    /**************
     * Validation
     **************/
    private void ensureValidEntityType( EntityType entityType ) {
        Preconditions.checkNotNull( entityType.getNamespace(), "Namespace for Entity Type is missing" );
        Preconditions.checkNotNull( entityType.getName(), "Name of Entity Type is missing" );
        Preconditions.checkNotNull( entityType.getProperties(), "Property for Entity Type is missing" );
        Preconditions.checkNotNull( entityType.getKey(), "Key for Entity Type is missing" );
        Preconditions.checkArgument( checkPropertyTypesExist( entityType.getProperties() )
                && entityType.getProperties().containsAll( entityType.getKey() ), "Invalid Entity Type provided" );
    }

    private void ensureValidPropertyType( PropertyType propertyType ) {
        Preconditions.checkNotNull( propertyType.getNamespace(), "Namespace for Property Type is missing" );
        Preconditions.checkNotNull( propertyType.getName(), "Name of Property Type is missing" );
        Preconditions.checkNotNull( propertyType.getDatatype(), "Datatype of Property Type is missing" );
        Preconditions.checkNotNull( propertyType.getMultiplicity(), "Multiplicity of Property Type is missing" );
    }

    private boolean checkPropertyTypesExist( Set<FullQualifiedName> properties ) {
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

    private boolean checkPropertyTypeExists( FullQualifiedName propertyTypeFqn ) {
        // Ho Chung: this check is easier for single property type
        String typename = tableManager.getTypenameForPropertyType( propertyTypeFqn );
        return StringUtils.isNotBlank( typename );
    }

    private boolean checkEntityTypeExists( FullQualifiedName entityTypeFqn ) {
        String typename = tableManager.getTypenameForEntityType( entityTypeFqn );
        return StringUtils.isNotBlank( typename );
    }

    private boolean schemaExists( String namespace, String name ) {
        return schemaExists( new FullQualifiedName( namespace, name ) );
    }

    private boolean schemaExists( FullQualifiedName schema ) {
        UUID aclId = ACLs.EVERYONE_ACL;
        return ( session.execute(
                tableManager.getSchemaStatement( aclId ).bind( schema.getNamespace(), schema.getName() ) )
                .one() != null );
    }
}
