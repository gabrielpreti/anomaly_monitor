package preti.spark.accesslog;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.rank.Median;

@SuppressWarnings("serial")
public class CustomDescriptiveStatistics extends DescriptiveStatistics {

	public CustomDescriptiveStatistics() {
		super();
	}

	public CustomDescriptiveStatistics(int windowSize) {
		super(windowSize);
	}

	public double getMedian() {
		return super.getPercentile(50);
	}

	public double getMad() {
		double mad = 0;
		double[] data = getValues();
		if (data.length > 0) {
			double median = getMedian();
			double[] deviationSum = new double[data.length];
			for (int i = 0; i < data.length; i++) {
				deviationSum[i] = Math.abs(median - data[i]);
			}
			mad = new Median().evaluate(deviationSum);
		}
		return mad;
	}
}
