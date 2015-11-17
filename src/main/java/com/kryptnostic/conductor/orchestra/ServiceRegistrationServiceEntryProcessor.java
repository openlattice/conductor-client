package com.kryptnostic.conductor.orchestra;

import java.util.Set;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;

public class ServiceRegistrationServiceEntryProcessor extends AbstractMerger<String, ServiceDescriptorSet, ServiceDescriptor>{

	private static final long serialVersionUID = -7766201773239438248L;

	public ServiceRegistrationServiceEntryProcessor(Set<ServiceDescriptor> objects) {
		this( new ServiceDescriptorSet( objects ) );
	}

	protected ServiceRegistrationServiceEntryProcessor(ServiceDescriptorSet objects) {
		super(objects);
	}

	@Override
	protected ServiceDescriptorSet newEmptyCollection() {
		return new ServiceDescriptorSet();
	}

}
