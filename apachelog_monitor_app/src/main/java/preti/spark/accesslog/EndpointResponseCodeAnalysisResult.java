package preti.spark.accesslog;

import java.io.Serializable;

@SuppressWarnings("serial")
public class EndpointResponseCodeAnalysisResult implements Serializable {

	private String endpointResponseCode;

	private double[] historicalMeans;
	private double[] currentMeans;
	private double[] historicalSds;
	private double[] currentSds;
	private double[] scores;
	private double[] scoreStatistics;
	private boolean[] alarms;
	private double[] values;
	private String[] buckets;

	public EndpointResponseCodeAnalysisResult(String endpointResponseCode, double[] values, double[] historicalMeans,
			double[] currentMeans, double[] historicalSds, double[] currentSds, double[] scores, double[] scoreStatistics, boolean[] alarms,
			String[] buckets) {
		super();
		this.values = values;
		this.endpointResponseCode = endpointResponseCode;
		this.historicalMeans = historicalMeans;
		this.currentMeans = currentMeans;
		this.historicalSds = historicalSds;
		this.currentSds = currentSds;
		this.scores = scores;
		this.scoreStatistics = scoreStatistics;
		this.alarms = alarms;
		this.buckets = buckets;
	}

	public String getEndpointResponseCode() {
		return endpointResponseCode;
	}

	public double[] getHistoricalMeans() {
		return historicalMeans;
	}

	public double[] getCurrentMeans() {
		return currentMeans;
	}

	public double[] getHistoricalSds() {
		return historicalSds;
	}

	public double[] getCurrentSds() {
		return currentSds;
	}

	public double[] getScores() {
		return scores;
	}
	
	public double[] getScoreStatistics() {
		return scoreStatistics;
	}

	public boolean[] getAlarms() {
		return alarms;
	}

	public String[] getBuckets() {
		return buckets;
	}

	public double[] getValues() {
		return values;
	}

	public boolean hasAlarm() {
		for (boolean alarm : alarms) {
			if (alarm)
				return true;
		}
		return false;
	}

}
