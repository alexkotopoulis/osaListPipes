package osaListPipes;


import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Example class to show GGSA API for listing pipelines combined with Spark API output to get status of applications.
 * Written to be used with Oracle GoldenGate Stream Analytics OCI Marketplace version 19.1.0.0.6+
 * 
 * @author akotopou
 *
 */
public class OsaListPipelines {

	String osaHost;
	String osaUriPath = "/osa/services/v1/pipelines";
	String sparkUriPath = "/json/api/v1/json";
	String sparkUser;
	String sparkPassword;
	String osaUser;
	String osaPassword;

	boolean validateCertificate;

	/** 
	 * Example Main to list pipelines from a given GGSA server with current run status for published and draft execution.
	 * @param args
	 */
	public static void main(String[] args) {



		if (args.length != 7) {
			System.err.println(
					"arguments: [osaHost] [osaUser] [osaPassword] [sparkUser] [sparkPassword] [validateCertificate] [onlyActive]");
			throw new RuntimeException("Wrong arguments!");
		}
		boolean onlyActive = Boolean.parseBoolean(args[6]); 
		OsaListPipelines listPipes = new OsaListPipelines(args[0], args[1], args[2], args[3], args[4], Boolean.parseBoolean(args[5]));
		Map<String, Pipeline> pipeMap = listPipes.getPipelines();

		for (Map.Entry<String, Pipeline> entry : pipeMap.entrySet()) {
			Pipeline pipe = entry.getValue();
			String status = "";
			if (!pipe.published && pipe.lastDraftStartTime > 0) {
				status += " " + pipe.lastDraftState + "(draft) " + dateToString(pipe.lastDraftStartTime);
			}
			if (pipe.published && pipe.lastPublicStartTime > 0) {
				status += " " + pipe.lastPublicState + "(published) " + dateToString(pipe.lastPublicStartTime);
			}
			if (!onlyActive || status.length() > 0)
				System.out.println(pipe.name + " " + status);
		}
	}
	
	/**
	 * Constructor for Listing Pipelines
	 * @param osaHost
	 * @param osaUriPath
	 * @param sparkUriPath
	 * @param osaUser
	 * @param osaPassword
	 * @param sparkUser
	 * @param sparkPassword
	 * @param validateCertificate
	 */

	public OsaListPipelines(String osaHost, String osaUser, String osaPassword,
			String sparkUser, String sparkPassword, boolean validateCertificate) {
		this.osaHost = osaHost;
		this.osaUser = osaUser;
		this.osaPassword = osaPassword;
		this.sparkUser = sparkUser;
		this.sparkPassword = sparkPassword;
		this.validateCertificate = validateCertificate;
	}


	/**
	 * Get information about all pipelines in GGSA server
	 * @return Hashmap with GGSA id of pipelines as key, pipeline object as value
	 */
	public Map<String, Pipeline> getPipelines() {

		HashMap<String, Pipeline> pipes = new HashMap<>();

		JsonNode pipeArray = restGet(osaHost, osaUriPath, osaUser, osaPassword, validateCertificate);

		for (int i = 0; i < pipeArray.size(); i++) {
			Pipeline pipe = new Pipeline();

			JsonNode pipeJson = pipeArray.path(i);
			pipe.id = pipeJson.path("id").asText();
			pipe.name = pipeJson.path("name").asText();

			pipe.displayName = pipeJson.path("displayName").asText();
			pipe.description = pipeJson.path("description").asText();
			pipe.createdBy = pipeJson.path("createdBy").asText();
			pipe.published = pipeJson.path("published").asBoolean();
			pipes.put(pipe.id, pipe);
		}

		JsonNode root = restGet(osaHost, sparkUriPath, sparkUser, sparkPassword, validateCertificate);

		JsonNode appsArray = root.path("activeapps");

		for (int i = 0; i < appsArray.size(); i++) {
			JsonNode app = appsArray.path(i);
			updatePipe(app, pipes);
		}

		appsArray = root.path("completedapps");

		for (int i = 0; i < appsArray.size(); i++) {
			JsonNode app = appsArray.path(i);
			updatePipe(app, pipes);
		}
		return pipes;
	}

	/**
	 * Update the pipeline hashmap with the Spark listing info of a given application
	 * @param app Parsed JSON document of a Spark app result
	 * @param pipes Hashmap with GGSA pipelines
	 */
	private void updatePipe(JsonNode app, HashMap<String, Pipeline> pipes) {
		String name = app.path("name").asText();
		String state = app.path("state").asText();
		long starttime = app.path("starttime").asLong();

		boolean isDraft = endsWith(name, "_draft");
		boolean isPublic = endsWith(name, "_public");

		String id = getId(name);
		if (id != null) {
			Pipeline pipe = pipes.get(id);
			if (null != pipe) {
				if (isDraft && pipe.lastDraftStartTime < starttime) {
					pipe.lastDraftStartTime = starttime;
					pipe.lastDraftState = state;
				} else if (isPublic && pipe.lastPublicStartTime < starttime) {
					pipe.lastPublicStartTime = starttime;
					pipe.lastPublicState = state;
				}
			}

		}
	}

	/**
	 * Call a REST GET operation given the connection info and return result as parsed JSON
	 * @param host
	 * @param uriPath
	 * @param login
	 * @param password
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	public JsonNode restGet(String host, String uriPath, String login, String password, boolean validateCertificate) {
		HttpRequestBase request;

		try {

			String uri = "https://" + host + uriPath;

			TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
				public java.security.cert.X509Certificate[] getAcceptedIssuers() {
					return null;
				}

				public void checkClientTrusted(X509Certificate[] certs, String authType) {
				}

				@Override
				public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
					// TODO Auto-generated method stub

				}
			}

			};

			SSLContext sc = SSLContext.getInstance("SSL");
			sc.init(null, trustAllCerts, new SecureRandom());

			request = new HttpGet(uri);

			final BasicCredentialsProvider provider = new BasicCredentialsProvider();
			// AuthScope authScope = new AuthScope(targetHost);
			provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(login, password));

			CloseableHttpClient client;

			if (!validateCertificate) {
				client = HttpClients.custom().setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).setSslcontext(sc)
						.setDefaultCredentialsProvider(provider).build();
			} else {
				client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();
			}

			HttpResponse response = client.execute(request);
			String resultJson = EntityUtils.toString(response.getEntity());
			// System.out.println("resultJson: " + resultJson);
			StatusLine sl = response.getStatusLine();
			int code = sl.getStatusCode();
			if (code < 200 || code >= 300) {
				System.err.println("" + code + " : " + sl.getReasonPhrase());
				throw new RuntimeException("" + code + " : " + sl.getReasonPhrase());
			} else {
				ObjectMapper mapper = new ObjectMapper();
				JsonNode root = mapper.readValue(resultJson, JsonNode.class);
				// System.out.println("resultJson root: " + resultJson);
				return root;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Check if a string ends with a given substring
	 * @param str
	 * @param substr
	 * @return
	 */

	static boolean endsWith(String str, String substr) {
		if (null == str || null == substr)
			return false;
		return (str.lastIndexOf(substr) == str.length() - substr.length());
	}
	
	/**
	 * Get the GGSA pipeline ID from a Spark application name. Return null if it has wrong format.
	 * @param str Spark app name
	 * @return GGSA pipeline ID
	 */

	static String getId(String str) {
		int suffix_len = 0;

		if (endsWith(str, "_draft"))
			suffix_len = 6;
		else if (endsWith(str, "_public"))
			suffix_len = 7;
		else
			return null;
		int beginIndex = str.length() - suffix_len - 36;

		if (beginIndex < 2)
			return null;

		String id = str.substring(beginIndex, beginIndex + 36).replace('_', '-');

		return id;
	}

	/**
	 * Format a long date to a date-time string
	 * @param dateNum
	 * @return
	 */
	static String dateToString(long dateNum) {
		Date date = new Date(dateNum);
		String dateText = null;

		SimpleDateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
		dateText = df.format(date);
		return dateText;
	}
}
