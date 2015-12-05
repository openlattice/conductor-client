package com.kryptnostic.conductor.orchestra;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.v1.objects.ServiceDescriptor;
import com.kryptnostic.conductor.v1.objects.ServiceDescriptorSet;
import com.kryptnostic.conductor.v1.processors.ServiceRegistrationServiceEntryProcessor;
import com.kryptnostic.mapstores.v1.constants.HazelcastNames.Maps;

public class ServiceRegistrationService {

    private IMap<String, ServiceDescriptorSet> services;

    public ServiceRegistrationService( HazelcastInstance hazelcast ) {
        this.services = hazelcast.getMap( Maps.CONDUCTOR_MANAGED_SERVICES );
    }

    public void register( ServiceDescriptor desc ) {
        services.submitToKey( desc.getServiceName(),
                new ServiceRegistrationServiceEntryProcessor( new ServiceDescriptorSet( desc ) ) );
    }
}
