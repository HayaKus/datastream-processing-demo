package org.datastream.connector.zhiyan;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import com.tencent.teg.monitor.sdk.TegMonitor;
import com.tencent.teg.monitor.sdk.CurveReporter;

/**
 * A Sink for publishing data into ZhiYan.
 * @param <IN>
 */
public class ZhiYanSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(ZhiYanSink.class);

	private static final int MSG_NUM_THRESHOLD = 1000;
	private static final int MSG_TIME_THRESHOLD = 10;

	// for buffering messge
	private transient ListState<Double> checkpointedState;
	private List<Double> bufferedMsg;
	private Date lastEmitTime;

	private int batchNum;
	private int batchTime;

	protected final String zhiyanAppMark;

	protected SerializationSchema<IN> schema;

	/**
	 * @param schema A {@link SerializationSchema} for turning the Java objects received into bytes
     */
	public ZhiYanSink(SerializationSchema<IN> schema, String zhiyanAppMark) {
		this.zhiyanAppMark = zhiyanAppMark;
		this.schema = schema;
		
		this.batchNum = MSG_NUM_THRESHOLD;
		this.batchTime = MSG_TIME_THRESHOLD;
		this.lastEmitTime = new Date();
		
		this.bufferedMsg = new ArrayList<>();

		TegMonitor.init();
	}

	@Override
	public String toString() {
        return "ZhiYanSink{" +
                "zhiyanAppMark='" + zhiyanAppMark + '\'' +
                ", batchNum=" + batchNum +
                ", batchTime='" + batchTime + '\'' +
                '}';
    }

	@Override
	public void invoke(IN value) {
		String msg = new String(schema.serialize(value));
		double delay = Double.valueOf(msg);

		bufferedMsg.add(delay);

		boolean numCondition = bufferedMsg.size() >= batchNum;
		Date now = new Date();
		boolean timeCondition = (now.getTime() - lastEmitTime.getTime()) / 1000 >= batchTime;

		if (!numCondition && !timeCondition) {
			// only store message
			return;
		}

		if (bufferedMsg.size() == 0) {
			lastEmitTime = new Date();
			bufferedMsg.clear();
			return;
		}
		LOG.info("[ZhiYan] buffered message size: {}", bufferedMsg.size());

		List<Double> failSendMsg = sendMsg(bufferedMsg);
		lastEmitTime = new Date();
		bufferedMsg.clear();
		
		if (failSendMsg.size() > 0) {
			bufferedMsg.addAll(failSendMsg);
		}
	}

	private List<Double> sendMsg(List<Double> bufferedMsg) {
		try{
			TegMonitor.init();
			CurveReporter reporter = TegMonitor.curveReporter().appMark(zhiyanAppMark);

			for (double element : bufferedMsg) {
				reporter.avgMetric("msg_delay", element);
				reporter.report();
			}
		} catch (Exception e) {
			LOG.error("[ZhiYan] send msg error: {}", e.getMessage());
			return bufferedMsg;
		}

		return null;
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		checkpointedState.clear();
		for (Double element : bufferedMsg) {
			checkpointedState.add(element);
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		ListStateDescriptor<Double> des = new ListStateDescriptor<Double>("bufferedMsg",
				TypeInformation.of(new TypeHint<Double>() {}));
		checkpointedState = context.getOperatorStateStore().getListState(des);
		if (context.isRestored()) {
			for (Double element : checkpointedState.get()) {
				bufferedMsg.add(element);
			}
		}
	}
}
