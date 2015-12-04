package preti.spark.accesslog;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@SuppressWarnings("serial")
public class EsEvent implements Serializable {

	private String endpointResponseCode;
	private double historicalMean;
	private double currentMean;
	private double historicalSd;
	private double currentSd;
	private double score;
	private double scoreStatistic;
	private double value;
	private boolean alarm;
	private String bucket;

	public EsEvent(String endpointResponseCode, double value, double historicalMean, double currentMean,
			double historicalSd, double currentSd, double score, double scoreStatistic, boolean alarm, String bucket) {
		super();
		this.value = value;
		this.endpointResponseCode = endpointResponseCode;
		this.historicalMean = historicalMean;
		this.currentMean = currentMean;
		this.historicalSd = historicalSd;
		this.currentSd = currentSd;
		this.score = score;
		this.scoreStatistic = scoreStatistic;
		this.alarm = alarm;
		this.bucket = bucket;
	}

	public String getEndpointResponseCode() {
		return endpointResponseCode;
	}

	public void setEndpointResponseCode(String endpointResponseCode) {
		this.endpointResponseCode = endpointResponseCode;
	}

	public double getHistoricalMean() {
		return historicalMean;
	}

	public void setHistoricalMean(double historicalMean) {
		this.historicalMean = historicalMean;
	}

	public double getCurrentMean() {
		return currentMean;
	}

	public void setCurrentMean(double currentMean) {
		this.currentMean = currentMean;
	}

	public double getHistoricalSd() {
		return historicalSd;
	}

	public void setHistoricalSd(double historicalSd) {
		this.historicalSd = historicalSd;
	}

	public double getCurrentSd() {
		return currentSd;
	}

	public void setCurrentSd(double currentSd) {
		this.currentSd = currentSd;
	}

	public double getScore() {
		return score;
	}

	public double getScoreStatistic() {
		return scoreStatistic;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public boolean isAlarm() {
		return alarm;
	}

	public void setAlarm(boolean alarm) {
		this.alarm = alarm;
	}

	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public double getValue() {
		return value;
	}

	public Date getTimestamp() {
		try {
			return new SimpleDateFormat("yyyyMMddHHmm").parse(bucket);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}

	public long getUnixTimestamp() {
		return getTimestamp().getTime() / 1000l;
	}

	public static Iterable<EsEvent> fromEndpointResponseCodeAnalysisResult(
			EndpointResponseCodeAnalysisResult analysisResult) {

		List<EsEvent> events = new ArrayList<>();

		for (int i = 0; i < analysisResult.getBuckets().length; i++) {
			events.add(new EsEvent(analysisResult.getEndpointResponseCode(), analysisResult.getValues()[i],
					analysisResult.getHistoricalMeans()[i], analysisResult.getCurrentMeans()[i],
					analysisResult.getHistoricalSds()[i], analysisResult.getCurrentSds()[i],
					analysisResult.getScores()[i], analysisResult.getScoreStatistics()[i],
					analysisResult.getAlarms()[i], analysisResult.getBuckets()[i]));
		}

		return events;
	}

}
