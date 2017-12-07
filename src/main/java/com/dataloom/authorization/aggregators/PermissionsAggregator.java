package com.dataloom.authorization.aggregators;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.google.common.collect.Sets;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.AclKey;

import java.util.Map;
import java.util.Set;

public class PermissionsAggregator extends Aggregator<Map.Entry<AceKey, AceValue>, Set<AclKey>>
        implements HazelcastInstanceAware {
    private static final long serialVersionUID = -1015754054455567010L;

    private final SecurableObjectType objectType;
    private final Set<Principal> principals;
    private final Set<Permission> permissions;

    private Set<AclKey> result;

    public PermissionsAggregator( SecurableObjectType objectType, Set<Principal> principals, Set<Permission> permissions ) {
        this( objectType, principals, permissions, Sets.newHashSet() );
    }

    public PermissionsAggregator( SecurableObjectType objectType, Set<Principal> principals, Set<Permission> permissions, Set<AclKey> result ) {
        this.objectType = objectType;
        this.principals = principals;
        this.permissions = permissions;
        this.result = result;
    }

    @Override public void accumulate( Map.Entry<AceKey, AceValue> input ) {

    }

    @Override public void combine( Aggregator aggregator ) {
        if (aggregator instanceof PermissionsAggregator) {
            result.addAll( ( (PermissionsAggregator) aggregator ).result );
        }
    }

    @Override public Set<AclKey> aggregate() {
        return result;
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {

    }

    public SecurableObjectType getObjectType() {
        return objectType;
    }

    public Set<Principal> getPrincipals() {
        return principals;
    }

    public Set<Permission> getPermissions() {
        return permissions;
    }

    public Set<AclKey> getResult() {
        return result;
    }
}
