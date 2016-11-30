package com.dataloom.authorization;

import java.util.HashMap;
import java.util.Set;

import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;

public class AccessControlList extends HashMap<Principal, Set<Permission>> {
    private static final long serialVersionUID = 7630681130415422564L;

    public AccessControlList() {
        super();
    }

    public AccessControlList( int initialCapacity ) {
        super( initialCapacity );
    }
}
