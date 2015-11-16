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
		out.writeUTF(object.getName());
		out.writeUTF(object.getHost());
		out.writeUTF(object.getPingbackUrl());
		out.writeInt(object.getPort());
		
	}
	
	public static ServiceDescriptor deserialize(ObjectDataInput in) throws IOException {
		String name = in.readUTF();
		String host = in.readUTF();
		String pingBackUrl = in.readUTF();
		int port = in.readInt();
		return new ServiceDescriptor(name, host, pingBackUrl, port);
	}

}
