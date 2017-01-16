package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.exceptions.AclKeyConflictException;
import com.dataloom.edm.exceptions.TypeExistsException;
import com.dataloom.edm.internal.AbstractSchemaAssociatedSecurableType;
import com.dataloom.edm.internal.AbstractSecurableObject;
import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;

public class HazelcastAclKeyReservationService {
    /**
     * This keeps mapping between SecurableObjectTypes that aren't FQN associated and their placeholder FQN.
     */
    private static final EnumMap<SecurableObjectType, FullQualifiedName> FQNS                 = new EnumMap<SecurableObjectType, FullQualifiedName>(
            SecurableObjectType.class );
    /*
     * List of FQN associated types. Roughly things that extend AbstractSchemaAssociatedSecurableType.
     */
    private static final EnumSet<SecurableObjectType>                    FQN_ASSOCIATED_TYPES = EnumSet
            .of( SecurableObjectType.EntityType, SecurableObjectType.PropertyTypeInEntitySet );

    static {
        /*
         * 
         */
        for ( SecurableObjectType objectType : SecurableObjectType.values() ) {
            if ( !FQN_ASSOCIATED_TYPES.contains( objectType ) ) {
                FQNS.put( objectType, new FullQualifiedName( "loom", objectType.name() ) );
            }
        }
    }

    private final IMap<FullQualifiedName, UUID> aclKeys;
    private final IMap<UUID, FullQualifiedName> fqns;

    public HazelcastAclKeyReservationService( HazelcastInstance hazelcast ) {
        this.aclKeys = hazelcast.getMap( HazelcastMap.ACL_KEYS.name() );
        this.fqns = hazelcast.getMap( HazelcastMap.FQNS.name() );
    }

    public void renameReservation( FullQualifiedName oldFqn, FullQualifiedName newFqn ) {
        /*
         * Attempt to associated newFqn with existing aclKey
         */
        final UUID existingAclKey = aclKeys.putIfAbsent( newFqn, Util.getSafely( aclKeys, oldFqn ) );

        if ( existingAclKey == null ) {
            aclKeys.delete( oldFqn );
        } else {
            throw new TypeExistsException(
                    "Cannot rename " + oldFqn.getFullQualifiedNameAsString() + " to existing type "
                            + newFqn.getFullQualifiedNameAsString() );
        }
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
        final FullQualifiedName fqn = fqns.putIfAbsent( type.getId(), type.getType() );
        final boolean fqnMatches = type.getType().equals( fqn );

        if ( fqn == null || fqnMatches ) {
            /*
             * AclKey <-> Type association exists and is correct. Safe to try and register AclKey for type.
             */
            final UUID existingAclKey = aclKeys.putIfAbsent( type.getType(), type.getId() );

            /*
             * Even if aclKey matches, letting two threads go through type creation creates potential problems when
             * entity types and entity sets are created using property types that have not quiesced. Easier for now to
             * just let one thread win and simplifies code path a lot.
             */

            if ( existingAclKey != null ) {
                if ( fqn == null ) {
                    // We need to remove UUID reservation
                    fqns.delete( type.getId() );
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

    /**
     * This function reserves an {@code AclKey} for a SecurableObject. It throws unchecked exceptions
     * {@link TypeExistsException} if the type already exists or {@link AclKeyConflictException} if a different AclKey
     * is already associated with the type.
     * 
     * @param type
     */
    public void reserveAclKey( AbstractSecurableObject type ) {
        checkArgument( FQNS.containsKey( type.getCategory() ), "Unsupported securable type for reservation" );
        /*
         * Template this call and make wrappers that directly insert into type maps making fqns redundant.
         */
        final FullQualifiedName fqn = fqns.putIfAbsent( type.getId(),
                Util.getSafely( FQNS, type.getCategory() ) );

        /*
         * We don't care if FQN matches in this case as it provides us no additional validation information.
         */

        if ( fqn != null ) {
            throw new AclKeyConflictException( "AclKey is already associated with different FQN." );
        }
    }

}
