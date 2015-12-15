package com.kryptnostic.conductor.orchestra;

import org.junit.Test;
import org.mockito.Mockito;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.v1.objects.ServiceDescriptor;
import com.kryptnostic.conductor.v1.objects.ServiceDescriptorSet;
import com.kryptnostic.conductor.v1.processors.ServiceRegistrationServiceEntryProcessor;
import com.kryptnostic.mapstores.v1.constants.HazelcastNames.Maps;

public class ServiceRegistrationServiceTest {

    @Test
    public void testRegistration() {
        HazelcastInstance hazelcastInstance = Mockito.mock( HazelcastInstance.class );
        IMap<String, ServiceDescriptorSet> m = Mockito.mock( IMapSSDS.class );
        Mockito.when( hazelcastInstance.<String, ServiceDescriptorSet> getMap( Maps.CONDUCTOR_MANAGED_SERVICES ) )
                .thenReturn( m );
        String serviceName = "blah";
        ServiceDescriptor sd = new ServiceDescriptor( serviceName, "localhost", 80, "http://localhost", "/" );
        ServiceRegistrationServiceEntryProcessor p = new ServiceRegistrationServiceEntryProcessor(
                new ServiceDescriptorSet( sd ) );
        ServiceRegistrationService service = new ServiceRegistrationService( hazelcastInstance );
        service.register( sd );
        Mockito.verify( m, Mockito.atLeastOnce() ).submitToKey( serviceName, p );
    }

    public static interface IMapSSDS extends IMap<String, ServiceDescriptorSet> {

    }
}
