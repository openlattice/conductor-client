package com.kryptnostic.conductor.orchestra;

import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;

public class ServiceRegistrationServiceEntryProcessorStreamSerializer implements
		SelfRegisteringStreamSerializer<ServiceRegistrationServiceEntryProcessor> {
	private static final Logger logger = LoggerFactory.getLogger(ServiceRegistrationServiceEntryProcessorStreamSerializer.class);

	@Override
	public void write(ObjectDataOutput out, ServiceRegistrationServiceEntryProcessor object) throws IOException {
		SetStreamSerializers.<ServiceDescriptor> serialize(out, object.getBackingCollection(), (ServiceDescriptor sd) -> {
			try {
				ServiceDescriptorStreamSerializer.serialize(out, sd);
			} catch (Exception e) {
				logger.error("Failure when serializing set.");
			}
		});
	}

	@Override
	public ServiceRegistrationServiceEntryProcessor read(ObjectDataInput in) throws IOException {
		Set<ServiceDescriptor> sds = SetStreamSerializers.<ServiceDescriptor> deserialize(in, (ObjectDataInput input) -> {
			try {
				return ServiceDescriptorStreamSerializer.deserialize(in);
			} catch (Exception e) {
				logger.error("Failure when deserializing set.");
				return null;
			}
		});
		return new ServiceRegistrationServiceEntryProcessor(sds);
	}

	@Override
	public int getTypeId() {
		// TODO
		// return HazelcastSerializerTypeIds.SERVICE_REGISTRATION_SERVICE
		return 0;
	}

	@Override
	public void destroy() {}

	@Override
	public Class<ServiceRegistrationServiceEntryProcessor> getClazz() {
		return ServiceRegistrationServiceEntryProcessor.class;
	}

}
