package com.dataloom.edm.util;

import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.SecurableObjectType;

public final class EdmUtil {
    private EdmUtil() {}

    public static boolean isNonNullPropertyTypeAclKey( AclKeyPathFragment aclKey ) {
        return aclKey!=null && aclKey.getType().equals( SecurableObjectType.PropertyTypeInEntitySet );
    }
    
    public static boolean isNonNullEntityTypeAclKey( AclKeyPathFragment aclKey ) {
        return aclKey!=null && aclKey.getType().equals( SecurableObjectType.EntityType );
    }
}
