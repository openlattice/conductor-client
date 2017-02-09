package com.dataloom.authorization;

import java.util.List;
import java.util.UUID;

public interface AbstractSecurableObjectResolveTypeService {
    
    void createSecurableObjectType( List<UUID> aclKey, SecurableObjectType type );
    
    void deleteSecurableObjectType( List<UUID> aclKey );
    
    SecurableObjectType getSecurableObjectType( List<UUID> aclKey );

}
