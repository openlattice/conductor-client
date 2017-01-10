package com.kryptnostic.datastore.services;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.properties.CassandraTypeManager;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.dataloom.edm.types.processors.AddPropertyTypesToEntityTypeProcessor;
import com.dataloom.edm.types.processors.RemovePropertyTypesFromEntityTypeProcessor;
import com.dataloom.edm.util.EdmUtil;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.hazelcast.HazelcastUtils;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.Lambdas;
import com.kryptnostic.datastore.util.Util;

public class EdmService implements EdmManager {
    private static final Logger                     logger = LoggerFactory.getLogger( EdmService.class );
    private final IMap<UUID, PropertyType>          propertyTypes;
    private final IMap<UUID, EntityType>            entityTypes;
    private final IMap<UUID, EntitySet>             entitySets;
    private final IMap<FullQualifiedName, AclKeyPathFragment>   aclKeys;
    private final IMap<AclKeyPathFragment, FullQualifiedName>   fqns;

    private final HazelcastAclKeyReservationService aclKeyReservations;
    private final AuthorizationManager              authorizations;
    private final CassandraEntitySetManager         entitySetManager;
    private final CassandraTypeManager              entityTypeManager;
    private final HazelcastSchemaManager            schemaManager;
    
    private DurableExecutorService executor;

    public EdmService(
            String keyspace,
            Session session,
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService aclKeyReservations,
            AuthorizationManager authorizations,
            CassandraEntitySetManager entitySetManager,
            CassandraTypeManager entityTypeManager,
            HazelcastSchemaManager schemaManager ) {
        this.authorizations = authorizations;
        this.entitySetManager = entitySetManager;
        this.entityTypeManager = entityTypeManager;
        this.schemaManager = schemaManager;
        this.propertyTypes = hazelcastInstance.getMap( HazelcastMap.PROPERTY_TYPES.name() );
        this.entityTypes = hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() );
        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
        this.fqns = hazelcastInstance.getMap( HazelcastMap.FQNS.name() );
        this.aclKeys = hazelcastInstance.getMap( HazelcastMap.ACL_KEYS.name() );
        this.aclKeyReservations = aclKeyReservations;
        this.executor = hazelcastInstance.getDurableExecutorService( "default" );
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
        aclKeyReservations.reserveAclKeyAndValidateType( propertyType );

        /*
         * Create property type if it doesn't exist. The reserveAclKeyAndValidateType call should ensure that
         */
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
        } else {
            logger.error(
                    "Inconsistency encountered in database. Verify that existing property types have all their acl keys reserved." );
        }
    }

    @Override
    public void deleteEntityType( UUID entityTypeFqn ) {
        /*
         * Entity types should only be deleted if there are no entity sets of that type in the system.
         */
        if ( Iterables.isEmpty( entitySetManager.getAllEntitySetsForType( entityTypeFqn ) ) ) {
            AclKeyPathFragment entityTypeKey = aclKeys.get( entityTypeFqn );
            entityTypes.delete( entityTypeKey.getId() );
        }
    }

    @Override
    public void deletePropertyType( UUID propertyTypeId ) {
        Set<EntityType> entityTypes = entityTypeManager
                .getEntityTypesContainingPropertyTypes( ImmutableSet.of( propertyTypeId ) );
        if ( entityTypes.stream()
                .allMatch( et -> Iterables.isEmpty( entitySetManager.getAllEntitySetsForType( et.getId() ) ) ) ) {
            propertyTypes.delete( propertyTypeId );
        }
    }

    public void createEntityType( EntityType entityType ) {
        /*
         * This is really create or replace and should be noted as such.
         */
        ensureValidEntityType( entityType );
        aclKeyReservations.reserveAclKeyAndValidateType( entityType );
        // Only create entity table if insert transaction succeeded.
        final EntityType existing = entityTypes.putIfAbsent( entityType.getId(), entityType );
        if ( existing == null ) {
            /*
             * As long as schemas are registered with upsertSchema, the schema query service should pick up the schemas
             * directly from the entity types and property types tables. Longer term, we should be more explicit about
             * the magic schema registration that happens when an entity type or property type is written since the
             * services are loosely coupled in a way that makes it easy to break accidentally.
             */
            schemaManager.upsertSchemas( entityType.getSchemas() );
        } else {
            /*
             * Only allow updates if entity type is not already in use.
             */
            if ( Iterables.isEmpty( entitySetManager.getAllEntitySetsForType( entityType.getAclKeyPathFragment().getId() ) ) ) {
                // Retrieve properties known to user
                Set<UUID> currentPropertyTypes = existing.getProperties();
                // Remove the removable property types in database properly; this step takes care of removal of
                // permissions
                Set<UUID> removablePropertyTypesInEntityType = Sets.difference( currentPropertyTypes,
                        entityType.getProperties() );
                removePropertyTypesFromEntityType( existing.getId(), removablePropertyTypesInEntityType );
                // Add the new property types in
                Set<UUID> newPropertyTypesInEntityType = Sets.difference( entityType.getProperties(),
                        currentPropertyTypes );
                addPropertyTypesToEntityType( entityType.getId(), newPropertyTypesInEntityType );

                // Update Schema
                final Set<FullQualifiedName> currentSchemas = existing.getSchemas();
                final Set<FullQualifiedName> removableSchemas = Sets.difference( currentSchemas,
                        entityType.getSchemas() );
                final Set<UUID> entityTypeSingleton = getEntityTypeUuids( ImmutableSet.of( existing.getType() ) );

                removableSchemas
                        .forEach( schema -> schemaManager.removeEntityTypesFromSchema( entityTypeSingleton, schema ) );

                Set<FullQualifiedName> newSchemas = Sets.difference( entityType.getSchemas(), currentSchemas );
                newSchemas.forEach( schema -> schemaManager.addEntityTypesToSchema( entityTypeSingleton, schema ) );
            }
        }
    }

    @Override
    public void deleteEntitySet( UUID entitySetId ) {
        /*
         * A side-effect of entity set inheriting from the base securable type class is that it's actual type is only
         * stored as a FQN and not a UUID. At some point, we should fix this so that the UUID of it's underlying entity
         * type is actually stored. BACKEND-612
         */
        final EntitySet entitySet = Util.getSafely( entitySets, entitySetId );
        final EntityType entityType = getEntityType( entitySet.getType() );

        /*
         * We cleanup permissions first as this will make entity set unavailable, even if delete fails.
         */
        final AclKeyPathFragment entitySetAclKey = new AclKeyPathFragment( SecurableObjectType.EntitySet, entitySetId );
        authorizations.deletePermissions( ImmutableList.of( entitySetAclKey ) );
        entityType.getProperties().stream()
                .map( propertyTypeId -> ImmutableList.of( entitySetAclKey,
                        new AclKeyPathFragment( SecurableObjectType.PropertyTypeInEntitySet, propertyTypeId ) ) )
                .forEach( authorizations::deletePermissions );

        Util.deleteSafely( entitySets, entitySetId );
    }

    @Override
    public void assignEntityToEntitySet( String entityId, String name ) {
        entitySetManager.assignEntityToEntitySet( entityId, name );
    }

    @Override
    public void assignEntityToEntitySet( String entityId, EntitySet es ) {
        assignEntityToEntitySet( entityId, es.getName() );
    }

    private void createEntitySet( EntitySet entitySet ) {
        checkNotNull( entitySet.getType(), "Entity set type cannot be null" );
        checkState( entitySetManager.getEntitySet( entitySet.getName() ) == null, "Entity set already exists." );
        if ( entitySets.putIfAbsent( entitySet.getId(), entitySet ) != null ) {
            throw new IllegalStateException( "Entity set already exists." );
        }
    }

    @Override
    public void createEntitySet( Principal principal, EntitySet entitySet ) {
        try {
            Principals.ensureUser( principal );

            createEntitySet( entitySet );

            EntityType entityType = entityTypes.get( entitySet.getType() );
            authorizations.addPermission( ImmutableList.of( entitySet.getAclKeyPathFragment() ),
                    principal,
                    EnumSet.allOf( Permission.class ) );
            entityType.getProperties().stream()
                    .map( propertyTypeId -> new AclKeyPathFragment( SecurableObjectType.PropertyTypeInEntitySet, propertyTypeId ) )
                    .forEach( propertyTypeAclKey -> authorizations.addPermission(
                            ImmutableList.of( entitySet.getAclKeyPathFragment(), propertyTypeAclKey ),
                            principal,
                            EnumSet.allOf( Permission.class ) ) );
            executor.submit( ConductorCall
            		.wrap( Lambdas.submitEntitySetToElasticsearch(
            				entitySet,
            				Lists.newArrayList( propertyTypes.getAll( entityType.getProperties() ).values() ),
            				principal ) ) );
        } catch ( Exception e ) {
            throw new IllegalStateException( "Entity Set not created." );
        }
    }

    @Override
    public Set<AclKeyPathFragment> getAclKeys( Set<FullQualifiedName> fqns ) {
        return ImmutableSet.copyOf( aclKeys.getAll( fqns ).values() );
    }

    @Override
    public Set<UUID> getEntityTypeUuids( Set<FullQualifiedName> fqns ) {
        return aclKeys.getAll( fqns ).values().stream()
                .filter( EdmUtil::isNonNullEntityTypeAclKey )
                .map( AclKeyPathFragment::getId )
                .collect( Collectors.toSet() );
    }

    @Override
    public Set<UUID> getPropertyTypeUuids( Set<FullQualifiedName> fqns ) {
        return aclKeys.getAll( fqns ).values().stream()
                .filter( EdmUtil::isNonNullPropertyTypeAclKey )
                .map( AclKeyPathFragment::getId )
                .collect( Collectors.toSet() );
    }

    @Override
    public EntityType getEntityType( FullQualifiedName typeFqn ) {
        AclKeyPathFragment entityTypeAclKey = getTypeAclKey( typeFqn );
        Preconditions.checkNotNull( entityTypeAclKey,
                "Entity type %s does not exist.",
                typeFqn.getFullQualifiedNameAsString() );
        return getEntityType( entityTypeAclKey.getId() );
    }

    @Override
    public EntityType getEntityType( UUID entityTypeFqn ) {
        AclKeyPathFragment aclKey = aclKeys.get( entityTypeFqn );
        return Preconditions.checkNotNull(
                HazelcastUtils.typedGet( entityTypes, aclKey.getId() ),
                "Entity type does not exist" );

    }

    public Iterable<EntityType> getEntityTypes() {
        return entityTypeManager.getEntityTypes();
    }

    @Override
    public EntityType getEntityType( String namespace, String name ) {
        return getEntityType( new FullQualifiedName( namespace, name ) );
    }

    @Override
    public EntitySet getEntitySet( UUID entitySetId ) {
        return Util.getSafely( entitySets, entitySetId );
    }

    @Override
    public EntitySet getEntitySet( String entitySetName ) {
        return entitySetManager.getEntitySet( entitySetName );
    }

    @Override
    public Iterable<EntitySet> getEntitySets() {
        return entitySetManager.getAllEntitySets();
    }

    @Override
    public Iterable<EntitySet> getEntitySetsOwnedByPrincipal( Principal principal ) {
        Iterable<AclKeyPathFragment> aclKeys = authorizations.getAuthorizedObjectsOfType( principal,
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.OWNER ) );
        return StreamSupport.stream( aclKeys.spliterator(), false )
                .map( AclKeyPathFragment::getId )
                .map( Util.getSafeMapper( entitySets ) )
                .collect( Collectors.toList() );
    }

    @Override
    public PropertyType getPropertyType( FullQualifiedName propertyType ) {
        return Preconditions.checkNotNull( Util.getSafely( propertyTypes, aclKeys.get( propertyType ).getId() ),
                "Property type does not exist" );
    }

    @Override
    public Iterable<PropertyType> getPropertyTypesInNamespace( String namespace ) {
        return entityTypeManager.getPropertyTypesInNamespace( namespace );
    }

    @Override
    public Iterable<PropertyType> getPropertyTypes() {
        return entityTypeManager.getPropertyTypes();
    }

    @Override
    public EntityDataModel getEntityDataModel() {
        Iterable<Schema> schemas = schemaManager.getAllSchemas();
        Iterable<EntityType> entityTypes = getEntityTypes();
        Iterable<PropertyType> propertyTypes = getPropertyTypes();
        Iterable<EntitySet> entitySets = getEntitySets();
        final Set<String> namespaces = Sets.newHashSet();

        entityTypes.forEach( entityType -> namespaces.add( entityType.getType().getNamespace() ) );
        propertyTypes.forEach( propertyType -> namespaces.add( propertyType.getType().getNamespace() ) );

        return new EntityDataModel(
                namespaces,
                schemas,
                entityTypes,
                propertyTypes,
                entitySets );
    }

    @Override
    public void addPropertyTypesToEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exist." );
        entityTypes.executeOnKey( entityTypeId, new AddPropertyTypesToEntityTypeProcessor( propertyTypeIds ) );
    }

    @Override
    public void removePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exist." );
        entityTypes.executeOnKey( entityTypeId, new RemovePropertyTypesFromEntityTypeProcessor( propertyTypeIds ) );
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
    public boolean checkPropertyTypesExist( Set<UUID> properties ) {
        return properties.stream().allMatch( propertyTypes::containsKey );
    }

    @Override
    public boolean checkPropertyTypeExists( UUID propertyTypeId ) {
        return propertyTypes.containsKey( propertyTypeId );
    }

    @Override
    public boolean checkEntityTypeExists( UUID entityTypeId ) {
        return entityTypes.containsKey( entityTypeId );
    }

    @Override
    public boolean checkEntitySetExists( String name ) {
        return entitySets.containsKey( name );
    }

    @Override
    public Collection<PropertyType> getPropertyTypes( Set<UUID> propertyIds ) {
        return propertyTypes.getAll( propertyIds ).values();
    }

    @Override
    public AclKeyPathFragment getTypeAclKey( FullQualifiedName type ) {
        return Util.getSafely( aclKeys, type );
    }

        @Override
    public PropertyType getPropertyType( UUID propertyTypeId ) {
        return Util.getSafely( propertyTypes, propertyTypeId );
    }

    @Override
    public FullQualifiedName getPropertyTypeFqn( UUID propertyTypeId ) {
        return Util.getSafely( fqns, new AclKeyPathFragment( SecurableObjectType.PropertyTypeInEntitySet, propertyTypeId ) );
    }

    @Override
    public FullQualifiedName getEntityTypeFqn( UUID entityTypeId ) {
        return Util.getSafely( fqns, new AclKeyPathFragment( SecurableObjectType.PropertyTypeInEntitySet, entityTypeId ) );
    }

}
