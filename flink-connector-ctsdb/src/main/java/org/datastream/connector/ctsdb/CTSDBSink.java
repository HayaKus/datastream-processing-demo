package org.datastream.connector.ctsdb;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple1;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A Sink for publishing data into CTSDB.
 * @param <IN>
 */
public class CTSDBSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
	private static final int MAX_HTTP_CONNECTION = 100;
	private static final int REQ_CONNECTION_TIMEOUT = 5000;
	private static final int CONNECT_TIMEOUT = 5000;
	private static final int SOCKET_TIMEOUT = 5000;

	private static final int MSG_NUM_THRESHOLD = 1000;
	private static final int MSG_TIME_THRESHOLd = 5;

	private static final Logger LOG = LoggerFactory.getLogger(CTSDBSink.class);

	protected final String addr;
	protected final String metricName;
	protected final String metricURL;
	protected final String user;
	protected final String password;

	protected SerializationSchema<IN> schema;
	private CloseableHttpClient httpClient;

	// for buffering messge
	private transient ListState<String> checkpointedState;
	private List<String> bufferedMsg;
	private Date lastEmitTime;

	private int batchNum;
	private int batchTime;

	public CTSDBSink(String addr, String metricName, SerializationSchema<IN> schema, int batchNum, int batchTime, String user, String password) {
		this.addr = addr;
		this.metricName = metricName;
		this.schema = schema;
		this.metricURL = addr + metricName + "/doc/_bulk";
		this.user = user;
		this.password = password;

		this.batchNum = batchNum;
		this.batchTime = batchTime;
		this.lastEmitTime = new Date();

		this.bufferedMsg = new ArrayList<>();
	}

	@Override
	public String toString() {
        return "CTSDBSink{" +
                "addr='" + addr + '\'' +
                ", metricName='" + metricName + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", batchNum=" + batchNum +
                ", batchTime='" + batchTime + '\'' +
                '}';
    }

	@Override
	public void open(Configuration conf) throws Exception {
		super.open(conf);
		PoolingHttpClientConnectionManager cm =  new PoolingHttpClientConnectionManager();
		cm.setMaxTotal(MAX_HTTP_CONNECTION);
		httpClient = HttpClients.custom()
			.setConnectionManager(cm)
			.setConnectionManagerShared(true) // share connection manager
			.build();
	}

	@Override
	public void close() throws Exception {
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to pulsar.
	 *
	 * @param value The incoming data
	 */
	@Override
	public void invoke(IN value) {
		String msg = new String(schema.serialize(value));
		if (msg.equalsIgnoreCase("error")) {
			// error message; do nothing
			return;
		}

		Tuple1<String> t = new Tuple1<>(msg);
		bufferedMsg.add(t._1());

		boolean numCondition = bufferedMsg.size() >= batchNum;
		Date now = new Date();
		boolean timeCondition = (now.getTime() - lastEmitTime.getTime()) / 1000 >= batchTime;

		if (!numCondition && !timeCondition) {
			// only store message
			return;
		}

		if (bufferedMsg.size() == 0) {
			// no messages; do nothing
			return;
		}
		LOG.info("[CTSDB] buffered message size: {}", bufferedMsg.size());

		// build message
		StringBuilder builder = new StringBuilder();
		for (String element : bufferedMsg) {
			builder.append(element);
		}

		String requestBody = builder.toString();
		if (requestBody.length() == 0) {
			bufferedMsg.clear();
			LOG.error("failed to build request body");
			return;
		}

		int status = 0;
		CloseableHttpResponse response = null;
		boolean persisted = false;
		try {
			response = SendMsg(metricURL, requestBody, this.user, this.password, REQ_CONNECTION_TIMEOUT, CONNECT_TIMEOUT, SOCKET_TIMEOUT, httpClient);
			status = response.getStatusLine().getStatusCode();
			LOG.info("[CTSDB, {}] response status: {}", this.metricName, status);
			if (status != HttpStatus.SC_OK) {
				LOG.error("HTTP code is" + status + "; Cannot send message {} to {}", requestBody, metricURL);
				// todo throw exception
			}
			lastEmitTime = new Date();
		} catch (Exception e) {
			LOG.error("HTTP code is" + status + "; Cannot send message {} to {}", requestBody, metricURL);
			// todo throw exception
		} finally {
			if (response != null) {
				try {
					EntityUtils.consume(response.getEntity());  // release connection
				} catch (IOException e) {
					e.printStackTrace();
					LOG.error("failed to release connection, status {}", status);
				}
			}
		}

		bufferedMsg.clear();
	}

    public static CloseableHttpResponse SendMsg(String metricURL, String requestBody, String user, String password, int REQ_CONNECTION_TIMEOUT, int CONNECT_TIMEOUT, int SOCKET_TIMEOUT, CloseableHttpClient httpClient) throws IOException, ClientProtocolException {
        CloseableHttpResponse response = null;
        HttpPost post = new HttpPost(metricURL);
        post.setHeader("Content-type", "application/json; charset=utf-8");
        StringEntity entity = new StringEntity(requestBody, Charset.forName("UTF-8"));
        entity.setContentEncoding("UTF-8");
        entity.setContentType("application/json");
        post.setEntity(entity);

        if (!user.isEmpty()) {
            String auth = user + ":" + password;
            String encoding =  Base64.getEncoder().encodeToString(auth.getBytes());
            post.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);
        }

        RequestConfig config = RequestConfig.custom()
                .setConnectionRequestTimeout(REQ_CONNECTION_TIMEOUT)
                .setConnectTimeout(CONNECT_TIMEOUT)
                .setSocketTimeout(SOCKET_TIMEOUT)
                .build();
        post.setConfig(config);

        response = httpClient.execute(post);

        return response;
    }

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		checkpointedState.clear();
		for (String element : bufferedMsg) {
			checkpointedState.add(element);
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		ListStateDescriptor<String> des = new ListStateDescriptor<String>("bufferedMsg",
				TypeInformation.of(new TypeHint<String>() {}));
		checkpointedState = context.getOperatorStateStore().getListState(des);
		if (context.isRestored()) {
			for (String element : checkpointedState.get()) {
				bufferedMsg.add(element);
			}
		}
	}
}
