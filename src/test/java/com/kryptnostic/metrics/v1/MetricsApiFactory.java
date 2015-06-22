package com.kryptnostic.metrics.v1;

import com.kryptnostic.instrumentation.v1.MetricsApi;

public class MetricsApiFactory {
	public static MetricsApi createMetricsApi(String name) {
		return createMetricsApi(new LoggingMetricsService(name));
	}

	public static MetricsApi createMetricsApi(LoggingMetricsService service) {
		MetricsApi api = new MetricsController();
		return api.setMetricsService(service);
	}
}
