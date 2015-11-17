package com.kryptnostic.conductor.orchestra;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;


public class ServiceRegistrationService implements MessageListener<ServiceDescriptor> {
	public static final String SERVICE_REGISTRATION_TOPIC = "service-registration";
	private IMap<String, Set<ServiceDescriptor>> services;
	private ITopic<ServiceDescriptor> registrationTopic;

	public ServiceRegistrationService(HazelcastInstance hazelcast) {
		this.registrationTopic = hazelcast.getTopic(SERVICE_REGISTRATION_TOPIC);
		this.services = hazelcast.getMap("conductorManagedServices");
		registrationTopic.addMessageListener(this);
	}

	@Override
	public void onMessage(Message<ServiceDescriptor> message) {
		Set<ServiceDescriptor> sd = services.get(message.getMessageObject().getName());
		if (sd == null) {
			sd = Sets.newHashSet();
		}
		sd.add(message.getMessageObject());
		services.put(message.getMessageObject().getName(), sd);
	}

	public void register(ServiceDescriptor desc) {
		services.submitToKey(desc.getName(),
				new AbstractMerger<String, ServiceDescriptorSet, ServiceDescriptor>(new ServiceDescriptorSet(desc)) {
					private static final long serialVersionUID = -1041320015061928550L;

					@Override
					protected ServiceDescriptorSet newEmptyCollection() {
						return new ServiceDescriptorSet();
					}
				});
	}

	public static class ServiceDescriptorSet extends HashSet<ServiceDescriptor> {
		private static final long serialVersionUID = -4023428026790176578L;

		public ServiceDescriptorSet() {
			super();
		}

		public ServiceDescriptorSet(ServiceDescriptor sd) {
			super(ImmutableSet.of(sd));
		}

	}

}
