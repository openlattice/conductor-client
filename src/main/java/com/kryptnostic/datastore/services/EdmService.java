package com.kryptnostic.datastore.services;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
import com.dataloom.edm.exceptions.AclKeyConflictException;
import com.dataloom.edm.exceptions.TypeExistsException;
import com.dataloom.edm.internal.AbstractSchemaAssociatedSecurableType;
import com.dataloom.edm.internal.DatastoreConstants;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.properties.CassandraEntityTypeManager;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
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
    private static final Logger                   logger = LoggerFactory.getLogger( EdmService.class );
    private final IMap<UUID, PropertyType>        propertyTypes;
    private final IMap<UUID, EntityType>          entityTypes;
    private final IMap<String, EntitySet>         entitySets;
    private final IMap<FullQualifiedName, AclKey> aclKeys;
    private final IMap<AclKey, FullQualifiedName> fqns;

    private final Session                         session;

    private final CassandraEdmStore               edmStore;
    private final CassandraEntitySetManager       entitySetManager;
    private final CassandraEntityTypeManager      entityTypeManager;
    private final HazelcastSchemaManager          schemaManager;
    private final CassandraTableManager           tableManager;
    private final PermissionsService              permissionsService;

    public EdmService(
            String keyspace,
            Session session,
            HazelcastInstance hazelcastInstance,
            CassandraTableManager tableManager,
            CassandraEntitySetManager entitySetManager,
            CassandraEntityTypeManager entityTypeManager,
            HazelcastSchemaManager cassandraSchemaManager,
            PermissionsService permissionsService ) {
        this.session = session;
        this.tableManager = tableManager;
        this.entitySetManager = entitySetManager;
        this.entityTypeManager = entityTypeManager;
        this.permissionsService = permissionsService;
        this.propertyTypes = hazelcastInstance.getMap( HazelcastMap.PROPERTY_TYPES.name() );
        this.entityTypes = hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() );
        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
        this.fqns = hazelcastInstance.getMap( HazelcastMap.FQNS.name() );
        this.aclKeys = hazelcastInstance.getMap( HazelcastMap.ACL_KEYS.name() );

        // Temp comment out the following two lines to avoid "Schema is out of sync." crash
        // and NPE for the PreparedStatement. Need to engineer it.
        // schemas.forEach( schema -> logger.info( "Namespace loaded: {}", schema ) );
        // schemas.forEach( tableManager::registerSchema );
        entityTypes.values().forEach( entityType -> logger.debug( "Object type read: {}", entityType ) );
        propertyTypes.values().forEach( propertyType -> logger.debug( "Property type read: {}", propertyType ) );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createPropertyType(com.kryptnostic.types.PropertyType) update
     * propertyType (and return true upon success) if exists, return false otherwise
     */
    @Override
    public void createPropertyTypeIfNotExists( PropertyType propertyType ) {
        ensureValidPropertyType( propertyType );
        reserveAclKeyAndValidateType( propertyType );

        // Create property type if it doesn't exist.
        PropertyType dbRecord = propertyTypes.putIfAbsent( propertyType.getId(), propertyType );

        if ( dbRecord != null ) {
            // Update Schema
            Set<FullQualifiedName> currentSchemas = dbRecord.getSchemas();
            Set<FullQualifiedName> removableSchemas = Sets.difference( currentSchemas, propertyType.getSchemas() );
            removableSchemas.forEach( schemaManager.entityTypesSchemaRemover( propertyType.getId() ) );
            Set<FullQualifiedName> newSchemas = Sets.difference( propertyType.getSchemas(), currentSchemas );
            newSchemas.forEach( schemaManager.propertyTypesSchemaAdder( propertyType.getId() ) );
            // Set Property type
            propertyTypes.set( propertyType.getId(), propertyType );
        }
    }

    @Override
    public void deleteEntityType( FullQualifiedName entityTypeFqn ) {
        /*
         * Entity types should only be deleted if there are now entity sets of that type in the system.
         */
        EntityType entityType = getEntityType( entityTypeFqn );
        if ( Iterables.isEmpty( entitySetManager.getAllEntitySetsForType( entityTypeFqn ) ) ) {
            AclKey entityTypeKey = aclKeys.get( entityTypeFqn );
            entityTypes.delete( entityTypeKey.getId() );
        }
    }

    @Override
    public void deletePropertyType( FullQualifiedName propertyTypeFqn ) {
        Set<EntityType> entityTypes = entityTypeManager
                .getEntityTypesContainingPropertyTypes( ImmutableSet.of( propertyTypeFqn ) );
        if ( entityTypes.stream()
                .allMatch( et -> Iterables.isEmpty( entitySetManager.getAllEntitySetsForType( et.getType() ) ) ) ) {
            AclKey propertyKey = aclKeys.get( propertyTypeFqn );
            propertyTypes.delete( propertyKey );
        }
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
            // Retrieve database record of entityType

        } else {
            createEntityType( entityType );
        }
    }

    public void createEntityType( EntityType entityType ) {
        /*
         * This is really create or replace and should be noted as such.
         */
        ensureValidEntityType( entityType );
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

            // Update Schema
            final Set<FullQualifiedName> currentSchemas = existing.getSchemas();
            final Set<FullQualifiedName> removableSchemas = Sets.difference( currentSchemas, entityType.getSchemas() );
            final Set<FullQualifiedName> entityTypeSingleton = ImmutableSet.of( existing.getType() );

            removableSchemas
                    .forEach( schema -> schemaManager.removeEntityTypesFromSchema( entityTypeSingleton, schema ) );

            Set<FullQualifiedName> newSchemas = Sets.difference( entityType.getSchemas(), currentSchemas );
            newSchemas.forEach( schema -> schemaManager.addEntityTypesToSchema( entityTypeSingleton, schema ) );
        }
    }

    @Override
    public void upsertEntitySet( EntitySet entitySet ) {
        entitySets.set( entitySet.getName(), entitySet );
        if ( checkEntitySetExists( entitySet.getName() ) ) {
            entitySetMapper.save( entitySet );
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
    public void createEntitySet( Principal principal, FullQualifiedName type, String name, String title ) {
        createEntitySet( principal, new EntitySet( Optional.absent(), type, name, title ) );
    }

    private void createEntitySet( EntitySet entitySet ) {
        Preconditions.checkNotNull( entitySet.getType(), "Entity set type cannot be null" );
        if ( entitySets.putIfAbsent( entitySet.getName(), entitySet ) != null ) {
            throw new IllegalStateException( "Entity set already exists." );
        }
    }

    @Override
    public void createEntitySet( Principal principal, EntitySet entitySet ) {
        try {
            Principals.ensureUser( principal );

            createEntitySet( entitySet );

            tableManager.addOwnerForEntitySet( entitySet.getName(), principal.getId() );

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

        } catch ( Exception e ) {
            throw new IllegalStateException( "Entity Set not created." );
        }
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
        EntitySet entitySet = Preconditions.checkNotNull( edmStore.getEntitySet( name ), "Entity Set does not exist" );
    }

    private Iterable<EntitySet> getEntitySetsForEntityType( FullQualifiedName type ) {
        return entitySetManager.getAllEntitySetsForType( type );
    }

    @Override
    public Iterable<EntitySet> getEntitySets() {
        return entitySetManager.getAllEntitySets();
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
        return Preconditions.checkNotNull( propertyTypes.get( propertyType ), "Property type does not exist" );
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
                                entityType.getType(), propertyTypeFqn ) );
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

            if ( !properties.isEmpty() ) {
                session.execute( Queries.dropPropertyColumnsFromEntityTable(
                        DatastoreConstants.KEYSPACE,
                        tableManager.getTablenameForEntityType( entityType ),
                        propertyColumnNames ) );
            }
        }
    }

    /**************
     * Validation
     **************/
    private void ensureValidEntityType( EntityType entityType ) {
        try {
            Preconditions.checkArgument( StringUtils.isNotBlank( entityType.getType().getNamespace() ),
                    "Namespace for Entity Type is missing" );
            Preconditions.checkArgument( StringUtils.isNotBlank( entityType.getType().getName() ),
                    "Name of Entity Type is missing" );
            Preconditions.checkArgument( CollectionUtils.isNotEmpty( entityType.getProperties() ),
                    "Property for Entity Type is missing" );
            Preconditions.checkArgument( CollectionUtils.isNotEmpty( entityType.getKey() ),
                    "Key for Entity Type is missing" );
            Preconditions.checkArgument( checkPropertyTypesExist( entityType.getProperties() )
                    && entityType.getProperties().containsAll( entityType.getKey() ), "Invalid Entity Type provided" );
        } catch ( Exception e ) {
            throw new IllegalArgumentException( e.getMessage() );
        }
    }

    private void ensureValidPropertyType( PropertyType propertyType ) {
        try {
            Preconditions.checkArgument( StringUtils.isNotBlank( propertyType.getType().getNamespace() ),
                    "Namespace for Property Type is missing" );
            Preconditions.checkArgument( StringUtils.isNotBlank( propertyType.getType().getName() ),
                    "Name of Property Type is missing" );
            Preconditions.checkArgument( propertyType.getDatatype() != null, "Datatype of Property Type is missing" );
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
        return propertyTypes.containsKey( propertyTypeFqn );
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
    public Collection<PropertyType> getPropertyTypes( Set<FullQualifiedName> properties ) {
        Map<FullQualifiedName, AclKey> propertyAclKeys = aclKeys.getAll( properties );
        Set<UUID> propertyIds = propertyAclKeys.values().stream().map( AclKey::getId ).collect( Collectors.toSet() );
        return propertyTypes.getAll( propertyIds ).values();
    }

    /**
     * This function reserves a UUID for a SecurableObject based on AclKey. It throws unchecked exception
     * {@link TypeExistsException} if the type already exists or {@link AclKeyConflictException} if a different AclKey
     * is already associated with the type.
     * 
     * @param type The type for which to reserve an FQN and UUID.
     */
    public void reserveAclKeyAndValidateType( AbstractSchemaAssociatedSecurableType type ) {
        /*
         * Template this call and make wrappers that directly insert into type maps making fqns redundant.
         */
        final FullQualifiedName fqn = fqns.putIfAbsent( type.getAclKey(), type.getType() );
        final boolean fqnMatches = type.getType().equals( fqn );

        if ( fqn == null || fqnMatches ) {
            /*
             * AclKey <-> Type association exists and is correct. Safe to try and register AclKey for type.
             */
            final AclKey existingAclKey = aclKeys.putIfAbsent( type.getType(), type.getAclKey() );

            /*
             * Even if aclKey matches, letting two threads go through type creation creates potential problems when
             * entity types and entity sets are created using property types that have not quiesced. Easier for now to
             * just let one thread win and simplifies code path a lot.
             */

            if ( existingAclKey != null ) {
                if ( fqn == null ) {
                    // We need to remove UUID reservation
                    fqns.delete( type.getAclKey() );
                }
                throw new TypeExistsException( "Type " + type.toString() + "already exists." );
            }

            /*
             * AclKey <-> Type association exists and is correct. Type <-> AclKey association exists and is correct.
             * Only a single thread should ever reach here.
             */
        } else {
            throw new AclKeyConflictException( "AclKey is already associated with different FQN." );
        }
    }
}
