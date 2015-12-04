package preti.spark.accesslog;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

@SuppressWarnings("serial")
public class ApacheAccessLog implements Serializable {
	private static final Logger logger = Logger.getLogger(ApacheAccessLog.class);

	private String ip;
	private String port;
	private String dateTimeString;
	private String method;
	private String endpoint;
	private String protocol;
	private int responseCode;
	private long contentSize;
	private String referrer;
	private String userAgent;
	private long duration;

	private Calendar dateTime;
	private String bucketId;

	private final Pattern ENDPOINT_SANITIZATION_PATTERN_1 = Pattern.compile("(.*?)\\?.*");

	public ApacheAccessLog(String ip, String port, String dateTimeString, String method, String endpoint,
			String protocol, String responseCode, String contentSize, String referrer, String userAgent,
			String duration) {
		super();
		this.ip = ip;
		this.port = port;
		this.dateTimeString = dateTimeString;
		this.method = method;

		Matcher m = ENDPOINT_SANITIZATION_PATTERN_1.matcher(endpoint);
		if (m.matches()) {
			this.endpoint = m.group(1);
		} else {
			this.endpoint = endpoint;
		}
		if (endpoint.matches("ws.pagseguro.uol.com.br\\/v2\\/transactions\\/.*")) {
			this.endpoint = "ws.pagseguro.uol.com.br/v2/transactions/";
		}

		this.protocol = protocol;
		this.responseCode = Integer.parseInt(responseCode);
		if (!"-".equals(contentSize)) {
			this.contentSize = Long.parseLong(contentSize);
		}
		this.referrer = referrer;
		this.userAgent = userAgent;
		this.duration = Long.parseLong(duration);

		this.dateTime = Calendar.getInstance();
		try {
			this.dateTime.setTime(new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss").parse(dateTimeString));
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}

		// this.bucketId = String.format("%s%s", new
		// SimpleDateFormat("yyyyMMddHH").format(this.dateTime.getTime()),
		// new DecimalFormat("00").format(5 *
		// (this.dateTime.get(Calendar.MINUTE) / 5)));
		this.bucketId = new SimpleDateFormat("yyyyMMddHHmm").format(this.dateTime.getTime());
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getDateTimeString() {
		return dateTimeString;
	}

	public void setDateTimeString(String dateTimeString) {
		this.dateTimeString = dateTimeString;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public int getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	public long getContentSize() {
		return contentSize;
	}

	public void setContentSize(long contentSize) {
		this.contentSize = contentSize;
	}

	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}

	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	public Calendar getDateTime() {
		return dateTime;
	}

	public void setDateTime(Calendar dateTime) {
		this.dateTime = dateTime;
	}

	public String getBucketId() {
		return bucketId;
	}

	public void setBucketId(String bucket) {
		this.bucketId = bucket;
	}

	public String getEndpointResponseCode() {
		return String.format("%s_%s", endpoint, responseCode);
	}

	public String toString() {
		return String.format(
				"ip=%s port=%s datTimeString=%s method=%s, endpoint=%s protocol=%s responseCode=%s contentSize=%s referrer=%s userAgent=%s duration=%s dateTime=%s bucket=%s",
				ip, port, dateTimeString, method, endpoint, protocol, responseCode, contentSize, referrer, userAgent,
				duration, new SimpleDateFormat("yyyy-MM-dd HH:mm").format(dateTime.getTime()), bucketId);
	}

	private static final String LOG_ENTRY_PATTERN = "^(\\S+?) (\\S+?) .*?\\[(\\S+?) .*?\"(\\S+?) (.+?) (HTTP)*(\\S+?)\" (\\S+?) (\\S+?) \"(.*?)\" \"(.+?)\" \".*\" (\\S+)";
	private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

	public static boolean filter(String logLine) {
		Matcher m = PATTERN.matcher(logLine);

		if (!m.find()) {
			logger.error("Cannot parse logline " + logLine);
			throw new RuntimeException("Error parsing logline " + logLine);
		}

		String endpoint = m.group(5);
		if (endpoint.matches(".*ws.acesso.intranet\\/cryptologin.*"))
			return false;
		if (endpoint.matches(".*\\.(jpg|gif|png)"))
			return false;
		if (endpoint.matches(".*hc\\.html"))
			return false;
		if (endpoint.matches(".*server\\-status.*"))
			return false;
		if (endpoint.matches(".*favicon\\.ico"))
			return false;
		if (endpoint.matches(".*robots\\.txt"))
			return false;
		if (endpoint.matches(".*checkout\\/metrics\\/(info|save)\\.jhtml"))
			return false;
		if (endpoint.matches(".*\\;.*"))
			return false;
		if (endpoint.matches(".*\\<\\/pre\\>.*"))
			return false;
		if (endpoint.matches("/.*\\\".*"))
			return false;

		String responseCode = m.group(8);
		if (responseCode.matches("30\\d"))
			return false;

		return true;
	}

	public static ApacheAccessLog parseFromLogLine(String logline) {
		Matcher m = PATTERN.matcher(logline);

		if (!m.find()) {
			logger.error("Cannot parse logline " + logline);
			throw new RuntimeException("Error parsing logline " + logline);
		}

		return new ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(7), m.group(8),
				m.group(9), m.group(10), m.group(11), m.group(12));
	}

//	private static final String LOG_ENTRY_PATTERN = "^(\\S+?) (\\S+?) .*?\\[(\\S+?) .*?\"(\\S+?) (.+?) (HTTP)*(\\S+?)\" (\\S+?) (\\S+?) \"(.*?)\" \"(.+?)\" \".*\" (\\S+)";
//	private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);
	public static void main(String[] args){
		String logline="177.13.235.5 11089 - \"-|-\" [03/Dec/2015:11:28:27 -0200] \"GET /pagseguro.uol.com.br/checkout/payment/booklet/download_pdf.jhtml?c=2e90da16cb4a6c0b550d12aa16650d6c9fcb877d256f4d389525b7fcdd673de2684c88bad863b589 HTTP/1.1\" 200 25951 \"\" \"Mozilla/5.0 (Linux; Android 5.0; SM-G900MD Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.76 Mobile Safari/537.36\" \"VmBDewqGECwAAFyF-Q0AAAD9\" 510254";
//		String logline="200.221.128.57 35698 - \"-|-\" [03/Dec/2015:10:40:13 -0200] \"- /-- -\" 408 - \"-\" \"-\" \"-\" 16";
		
		Matcher m = PATTERN.matcher(logline);
		System.out.println(m.find());
		ApacheAccessLog ap =new ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(7), m.group(8),
				m.group(9), m.group(10), m.group(11), m.group(12));
		System.out.println(ap);
	}

}
