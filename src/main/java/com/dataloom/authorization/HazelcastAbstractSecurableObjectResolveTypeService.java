package com.dataloom.authorization;

import java.util.List;
import java.util.UUID;

import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
 
public class HazelcastAbstractSecurableObjectResolveTypeService implements AbstractSecurableObjectResolveTypeService {
    
    private final IMap<List<UUID>, SecurableObjectType> securableObjectTypes;
    
    public HazelcastAbstractSecurableObjectResolveTypeService( HazelcastInstance hazelcastInstance ) {
        securableObjectTypes = hazelcastInstance.getMap( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );
    }
    
    @Override
    public void createSecurableObjectType( List<UUID> aclKey, SecurableObjectType type ) {
        securableObjectTypes.set( aclKey, type );
        
    }

    @Override
    public void deleteSecurableObjectType( List<UUID> aclKey ) {
        securableObjectTypes.remove( aclKey );
    }
    
    @Override
    public SecurableObjectType getSecurableObjectType( List<UUID> aclKey ) {
        return securableObjectTypes.get( aclKey );
    }

}
