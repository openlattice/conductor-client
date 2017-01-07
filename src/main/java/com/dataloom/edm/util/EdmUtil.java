package com.dataloom.edm.util;

import com.dataloom.authorization.AclKey;
import com.dataloom.authorization.SecurableObjectType;

public final class EdmUtil {
    private EdmUtil() {}

    public static boolean isNonNullPropertyTypeAclKey( AclKey aclKey ) {
        return aclKey!=null && aclKey.getType().equals( SecurableObjectType.PropertyTypeInEntitySet );
    }
    
    public static boolean isNonNullEntityTypeAclKey( AclKey aclKey ) {
        return aclKey!=null && aclKey.getType().equals( SecurableObjectType.EntityType );
    }
}
