package preti.spark.accesslog;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("serial")
public class Bucket<T> implements Serializable {
	private String bucketId;
	private Map<String, T> metrics;

	public Bucket(String bucketId) {
		super();
		this.bucketId = bucketId;
		this.metrics = new HashMap<>();
	}

	public String getBucketId() {
		return bucketId;
	}

	public Map<String, T> getMetrics() {
		return metrics;
	}

	public void addMetric(String metricId, T metricValue) {
		metrics.put(metricId, metricValue);
	}

	public T getMetric(String metricId) {
		return metrics.get(metricId);
	}

	public boolean hasMetric(String metricId) {
		return metrics.containsKey(metricId);
	}

	public String toString() {
		StringBuilder result = new StringBuilder();
		for (String m : metrics.keySet()) {
			result = result.append(String.format("%s::%s;", m, metrics.get(m)));
		}
		return result.toString();
	}

}
