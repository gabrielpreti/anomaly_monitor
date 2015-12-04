package preti.spark.accesslog;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import scala.Tuple2;

public class AccesslogAnalysis {
	private static final Log log = LogFactory.getLog(AccesslogAnalysis.class);
	// private static final int WINDOW_SIZE = 30;
	private static final int WINDOW_SIZE = 60;
	// private static final int DELAY = 5;
	private static final int DELAY = 10;
	private static final int TRAINING_SIZE = 60;
	private static final double THRESOLD_REL_SD_MEAN = 0.3;
	private static final double ENOUGH_REQUESTS_THRESHOLD = 5;

	// https://databricks.gitbooks.io/databricks-spark-reference-applications/content/logs_analyzer/chapter1/spark.html
	@SuppressWarnings("resource")
	public static void main(String[] args) throws JsonGenerationException, JsonMappingException, IOException {
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the text file into Spark.
		if (args.length == 0) {
			System.out.println("Must specify an access logs file.");
			System.exit(-1);
		}
		String logFile = args[0];

		JavaRDD<String> logLines = sc.textFile(logFile).filter(ApacheAccessLog::filter);

		JavaRDD<ApacheAccessLog> events = logLines.map(ApacheAccessLog::parseFromLogLine);
		events = events.filter(e -> e.getEndpointResponseCode().contains("/pagseguro.uol.com.br/_200"));

		List<String> bucketsList = events.mapToPair(event -> new Tuple2<>(event.getBucketId(), event)).groupByKey()
				.sortByKey(true).keys().collect();
		JavaRDD<String> endpointResponseCodes = events.map(event -> event.getEndpointResponseCode()).distinct();

		JavaPairRDD<String, ApacheAccessLog> accessLogPairRDD = events
				.mapToPair(log -> new Tuple2<>(log.getBucketId(), log));
		JavaPairRDD<String, Iterable<ApacheAccessLog>> bucketsAccessLog = accessLogPairRDD.groupByKey();

		@SuppressWarnings("resource")
		JavaPairRDD<String, Bucket<Long>> bucketsMetrics = bucketsAccessLog.mapValues(accessLogs -> {
			Bucket<Long> bucket = null;

			for (ApacheAccessLog v : accessLogs) {
				if (bucket == null) {
					bucket = new Bucket<>(v.getBucketId());
				}

				String metricId = v.getEndpointResponseCode();
				Long currentValue = bucket.getMetric(metricId);
				bucket.addMetric(metricId, currentValue == null ? 1 : currentValue + 1);
			}

			return bucket;
		});
		bucketsMetrics = bucketsMetrics.sortByKey(true);
		Map<String, Bucket<Long>> bucketsMetricsMap = bucketsMetrics.collectAsMap();

		JavaPairRDD<String, EndpointResponseCodeAnalysisResult> analysisResults = endpointResponseCodes
				.mapToPair(endpointResponseCode -> {
					double[] historicalMeans = new double[bucketsList.size()];
					double[] currentMeans = new double[bucketsList.size()];
					double[] historicalSds = new double[bucketsList.size()];
					double[] currentSds = new double[bucketsList.size()];
					double[] scores = new double[bucketsList.size()];
					boolean[] alarms = new boolean[bucketsList.size()];
					double[] values = new double[bucketsList.size()];

					CustomDescriptiveStatistics historicalStatistics = new CustomDescriptiveStatistics(WINDOW_SIZE);
					CustomDescriptiveStatistics currentStatistics = new CustomDescriptiveStatistics(DELAY);
					CustomDescriptiveStatistics scoreStatistics = new CustomDescriptiveStatistics();

					for (int currentIndex = 0; currentIndex < bucketsList.size(); currentIndex++) {
						String bucket = bucketsList.get(currentIndex);
						// log.info(String.format("%s_%s_%s", currentIndex,
						// endpointResponseCode, bucket));
						if (bucketsMetricsMap.containsKey(bucket)
								&& bucketsMetricsMap.get(bucket).hasMetric(endpointResponseCode)) {
							currentStatistics.addValue(bucketsMetricsMap.get(bucket).getMetric(endpointResponseCode));
							values[currentIndex] = bucketsMetricsMap.get(bucket).getMetric(endpointResponseCode);
						} else {
							currentStatistics.addValue(0);
						}

						if (currentIndex < DELAY) {
							continue;
						}

						bucket = bucketsList.get(currentIndex - 5);
						if (bucketsMetricsMap.containsKey(bucket)
								&& bucketsMetricsMap.get(bucket).hasMetric(endpointResponseCode)) {
							historicalStatistics
									.addValue(bucketsMetricsMap.get(bucket).getMetric(endpointResponseCode));
						} else {
							historicalStatistics.addValue(0);
						}

						if (currentIndex > (WINDOW_SIZE + DELAY)) {
							// double historicalMean =
							// historicalStatistics.getMedian();
							double historicalMean = historicalStatistics.getMean();
							historicalMeans[currentIndex] = historicalMean;
							// double historicalSd =
							// historicalStatistics.getMad();
							double historicalSd = historicalStatistics.getStandardDeviation();
							historicalSds[currentIndex] = historicalSd;
							if (historicalMean == 0 && historicalSd == 0) {
								continue;
							}

							// double currentMean =
							// currentStatistics.getMedian();
							double currentMean = currentStatistics.getMean();
							currentMeans[currentIndex] = currentMean;
							// currentSds[currentIndex] =
							// currentStatistics.getMad();
							currentSds[currentIndex] = currentStatistics.getStandardDeviation();
							double difference = Math.abs(currentMean - historicalMean);
							double scoreHistoricalSd = difference / historicalSd;
							scores[currentIndex] = scoreHistoricalSd;

							boolean isInTrainingPhase = currentIndex <= (WINDOW_SIZE + DELAY + TRAINING_SIZE);
							boolean isHistoricalVariationSmallEnough = historicalSd
									/ historicalMean < THRESOLD_REL_SD_MEAN;
							boolean isDifferenceGreatherThanHistoricalScore = scoreHistoricalSd > scoreStatistics
									.getMax();
							boolean isHistoricalMeanGreatherThanRequestsThreshold = historicalMean >= ENOUGH_REQUESTS_THRESHOLD;

							if (!isInTrainingPhase && isDifferenceGreatherThanHistoricalScore
									&& isHistoricalVariationSmallEnough
									&& isHistoricalMeanGreatherThanRequestsThreshold) {
								alarms[currentIndex] = true;
							} else {
								scoreStatistics.addValue(scoreHistoricalSd);
							}

						}
					}
					return new Tuple2<>(endpointResponseCode,
							new EndpointResponseCodeAnalysisResult(endpointResponseCode, values, historicalMeans,
									currentMeans, historicalSds, currentSds, scores, alarms,
									bucketsList.toArray(new String[] {})));
				});

		// analysisResults = analysisResults.filter(result ->
		// result._2().hasAlarm());
		JavaPairRDD<String, EsEvent> esEvents = analysisResults.flatMapValues(analysisResult -> {
			return EsEvent.fromEndpointResponseCodeAnalysisResult(analysisResult);
		});

		esEvents.values().foreachPartition(partitionEvents -> {
			Socket socket = new Socket("localhost", 5000);
			OutputStream stream = socket.getOutputStream();
			PrintWriter writer = new PrintWriter(stream, true);
			ObjectMapper mapper = new ObjectMapper();

			while (partitionEvents.hasNext()) {
				EsEvent e = partitionEvents.next();
				writer.println(mapper.writeValueAsString(e));
			}
			writer.close();
		});

		// esEvents.values().foreach(esEvent -> new ObjectMapper()
		// .writeValue(new SocketOutputStream(new Socket("localhost", 5000),
		// 2000), esEvent));

		// JavaEsSpark.saveToEsWithMeta(esEvents.values().mapToPair(esEvent -> {
		// Map<Metadata, Date> metadata = new HashMap<>();
		// metadata.put(Metadata.TIMESTAMP, esEvent.getTimestamp());
		// return new Tuple2(metadata, esEvent);
		// }), "spark/docs");
		// esEvents.saveAsTextFile("/tmp/output");

		// JavaEsSpark.saveToEs(esEvents.values(), "spark/docs");
		// bucketsMetrics.saveAsTextFile("/tmp/output");
		sc.close();
	}
}
