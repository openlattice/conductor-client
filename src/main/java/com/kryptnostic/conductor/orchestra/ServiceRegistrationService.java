package com.kryptnostic.conductor.orchestra;

import java.util.Set;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.orchestra.NameConstants;



public class ServiceRegistrationService {
	private IMap<String, Set<ServiceDescriptor>> services;

	public ServiceRegistrationService(HazelcastInstance hazelcast) {
		this.services = hazelcast.getMap(NameConstants.CONDUCTOR_MANAGED_SERVICES);
	}

	public void register(ServiceDescriptor desc) {
		services.submitToKey(desc.getServiceName(), new ServiceRegistrationServiceEntryProcessor(new ServiceDescriptorSet(desc)));
	}
	
}
