package com.kryptnostic.conductor.orchestra;

import java.io.IOException;

import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public class ServiceStatusStreamSerializer implements 
		SelfRegisteringStreamSerializer<ServiceStatus>{

	@Override
	public void write(ObjectDataOutput out, ServiceStatus object) throws IOException {
		
		serialize(out, object);
	}

	@Override
	public ServiceStatus read(ObjectDataInput in) throws IOException {
		
		return deserialize(in);
	}



	@Override
	public int getTypeId() {
		// TODO Auto-generated method stub
		// return HazelcastSerializerTypeIds.SERVICE_STATUS
		return 0;
	}

	@Override
	public void destroy() {}

	@Override
	public Class<ServiceStatus> getClazz() {
		return ServiceStatus.class;
	}
	
	public static void serialize(ObjectDataOutput out, ServiceStatus object) throws IOException {
		
		ServiceDescriptorStreamSerializer.serialize(out, object.getService());
		out.writeBoolean(object.isConnectable());
	}
	
	public static ServiceStatus deserialize(ObjectDataInput in) throws IOException {
		ServiceDescriptor service = ServiceDescriptorStreamSerializer.deserialize(in);
		boolean connectable = in.readBoolean();
		return new ServiceStatus(service, connectable);
	}

}
