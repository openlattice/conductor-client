package com.kryptnostic.conductor.orchestra;

import java.util.Set;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.v1.NameConstants;
import com.kryptnostic.conductor.v1.objects.ServiceDescriptor;
import com.kryptnostic.conductor.v1.objects.ServiceDescriptorSet;
import com.kryptnostic.conductor.v1processors.ServiceRegistrationServiceEntryProcessor;



public class ServiceRegistrationService {
	private IMap<String, Set<ServiceDescriptor>> services;

	public ServiceRegistrationService(HazelcastInstance hazelcast) {
		this.services = hazelcast.getMap(NameConstants.CONDUCTOR_MANAGED_SERVICES);
	}

	public void register(ServiceDescriptor desc) {
		services.submitToKey(desc.getServiceName(), new ServiceRegistrationServiceEntryProcessor(new ServiceDescriptorSet(desc)));
	}
	
}
