package com.kryptnostic.conductor.orchestra;

import java.io.IOException;

import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
//import com.kryptnostic.mapstores.v1.constants.HazelcastSerializerTypeIds;

public class ServiceDescriptorStreamSerializer implements SelfRegisteringStreamSerializer<ServiceDescriptor> {

	@Override
	public void write(ObjectDataOutput out, ServiceDescriptor object) throws IOException {
		serialize(out, object);
		
	}

	@Override
	public ServiceDescriptor read(ObjectDataInput in) throws IOException {
		return deserialize(in);
	}

	@Override
	public int getTypeId() {
		// TODO
		// return HazelcastSerializerTypeIds.SERVICE_DESCRIPTOR;
		return 0;
	}

	@Override
	public void destroy() {}

	@Override
	public Class<ServiceDescriptor> getClazz() {
		return ServiceDescriptor.class;
	}
	
	public static void serialize(ObjectDataOutput out, ServiceDescriptor object) throws IOException {
		out.writeUTF(object.getServiceName());
		out.writeUTF(object.getServiceHost());
		out.writeInt(object.getServicePort());
		out.writeUTF(object.getServicePingbackUrl());
		out.writeUTF(object.getServiceDeployPath());
		
		
	}
	
	public static ServiceDescriptor deserialize(ObjectDataInput in) throws IOException {
		String name = in.readUTF();
		String host = in.readUTF();
		int port = in.readInt();
		String pingBackUrl = in.readUTF();
		String deployPath = in.readUTF();
		
		return new ServiceDescriptor(name, host, port, pingBackUrl, deployPath );
	}

}
