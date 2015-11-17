package com.kryptnostic.conductor.orchestra;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class ServiceDescriptorSet extends HashSet<ServiceDescriptor>{

	private static final long serialVersionUID = 3266580780735274219L;

	public ServiceDescriptorSet() {
		super();
	}

	public ServiceDescriptorSet(ServiceDescriptor sd) {
		super(ImmutableSet.of(sd));
	}
	
	public ServiceDescriptorSet( int initialCapacity) {
		super( initialCapacity);
	}

	public ServiceDescriptorSet(Set<ServiceDescriptor> objects) {
		super( objects );
	} 
	
	public static ServiceDescriptorSet of(ServiceDescriptor serviceDescriptor) {
		ServiceDescriptorSet ss = new ServiceDescriptorSet(1);
		ss.add(serviceDescriptor);
		return ss;
		
	}

	
}
