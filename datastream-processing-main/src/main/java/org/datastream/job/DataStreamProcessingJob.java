package org.datastream.job;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.datastream.connector.pulsar.PulsarSource;
import org.datastream.connector.pulsar.PulsarConfig;
import org.datastream.connector.ctsdb.CTSDBSink;
import org.datastream.connector.zhiyan.ZhiYanSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import com.tencent.teg.monitor.sdk.TegMonitor;
import com.tencent.teg.monitor.sdk.CurveReporter;

public class DataStreamProcessingJob {
    private static ParameterTool parameter;
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamProcessingJob.class);
    private static final OutputTag<String> outputSessionTag = new OutputTag<String>("session-output") {}; // session msg
    private static final OutputTag<String> outputZhiYanTag = new OutputTag<String>("zhiyan-output") {}; // zhiyan

    static class ProcessFunctionForCdbJob extends ProcessFunction<String, String> {
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            // emit data to regular output
            out.collect(value);

            // emit data to side output
            int valueLength = value.length();
            if (valueLength <= 64 ) { // the length of msg shoule be more than 64
                return;
            }
            LOG.info("Receive msg: " + value);

            String moduleName = value.substring(0, 16).trim();
            String tail = value.substring(64);
            if (moduleName.equals("session")) {
                // send session msg into session-output
                ctx.output(DataStreamProcessingJob.outputSessionTag, tail);
                long currentTime = System.currentTimeMillis(); // get current time
                String msgSendTime = value.substring(16, 32).trim(); // get msg send time
                long msgSendTimeLong = Long.parseLong(msgSendTime);
                long delay = currentTime - msgSendTimeLong;
                // send delay into zhiyan-output
                ctx.output(DataStreamProcessingJob.outputZhiYanTag, String.valueOf(delay));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // parse params
        Options options = new Options();
        options.addOption("h", false, "list help");
        options.addOption("c", "config", true, "config file");
        options.addOption("p", "parallelism", true, "job parallelism");
        options.addOption("t", "pulsar_topics", true, "pulsar topics list, split by , ");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        LOG.info(Arrays.toString(cmd.getArgs()));

        if (cmd.hasOption("h")) {
            LOG.info("usage:-c config_file -t topics_names -p parallelism");
            return;
        }
        if (!cmd.hasOption("c")) {
            LOG.error("NOTE: must specify -c option.");
            return;
        }
        String configFile = cmd.getOptionValue("c");
        parameter = ParameterTool.fromPropertiesFile(configFile);

        // parallelism should be tested before set
        int parallelism = parameter.getInt("parallelism", 8);
        // use custom parallelism
        if (cmd.hasOption("p")) {
            parallelism = Integer.parseInt(cmd.getOptionValue("p"));
        }

        int stateBackendMemLim = parameter.getInt("state_backend_mem_lim", 104857600); // 100M

        // build job
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // in case the system is constantly taking checkpoints
        int checkpoint_interval  = parameter.getInt("checkpoint_interval", 5000);
        int checkpoint_min_pause = parameter.getInt("checkpoint_min_pause", 5000);
        int max_concurrent_checkpoints = parameter.getInt("max_concurrent_checkpoints", 1);
        if (max_concurrent_checkpoints > 1) {
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(max_concurrent_checkpoints); // 1
        } else {
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpoint_min_pause); // 5s
        }
        env.enableCheckpointing(checkpoint_interval, CheckpointingMode.EXACTLY_ONCE); // exactly once

        env.setParallelism(parallelism);
        env.setStateBackend(new MemoryStateBackend(stateBackendMemLim, true));
        // shared in all job
        env.getConfig().setGlobalJobParameters(parameter);
        // use event timestamp
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        int source_parallelism = parameter.getInt("source_parallelism", parallelism);
        int sink_parallelism = parameter.getInt("sink_parallelism", parallelism);
      
        // config zhiyan
        String zhiyanAppMark = parameter.get("zhiyan_app_mark", "6132_37304_cdb_snapshot_monitor");

        // config pulsar source
        String pulsarTopics = "";
        if (cmd.hasOption("pulsar_topics")) {
            pulsarTopics = cmd.getOptionValue("pulsar_topics", "");
        }
        String pulsarRegion = parameter.get("pulsar_region", "ap-gz");
        String pulsarTenant = parameter.get("pulsar_tenant", "");
        String pulsarToken = parameter.get("pulsar_token", "");
        String pulsarNamespace = parameter.get("pulsar_namespace", "");
        String pulsarSubscription = parameter.get("pulsar_subscription", "queue_test");
        boolean pulsarResend = parameter.getBoolean("pulsar_enable_resend", true);

        Preconditions.checkNotNull(pulsarTopics, "require parameter -t (--pulsar_topics) not present");
        String[] topics = pulsarTopics.split(",");
        PulsarConfig config = new PulsarConfig(pulsarRegion, pulsarTenant, pulsarNamespace, pulsarToken, topics, pulsarSubscription, pulsarResend);
        LOG.info("pulsar config: " + config.toString());
        MsgDeserializationScheme schema = new MsgDeserializationScheme();

        SourceFunction<String> source = new PulsarSource<>(config, schema);
        String sourceName = String.format("%s-%s", pulsarNamespace, pulsarTopics);
        String jobName = sourceName;

        DataStream<String> srcStream = env.addSource(source).name(sourceName);
        SingleOutputStreamOperator<String> mainStream = srcStream.process(new ProcessFunctionForCdbJob());
        mainStream.uid(jobName).name(jobName);

        // sink data to ctsdb
        String ctsd_addr   = parameter.get("ctsdb_addr");
        String session_metric_name = parameter.get("ctsdb_session_metric");

        int batch_size = parameter.getInt("ctsdb_batch_size", 1000);
        int batch_time = parameter.getInt("ctsdb_batch_time", 5);
        String user = parameter.get("ctsdb_user", "");
        String passwd = parameter.get("ctsdb_passwd", "");

        // session data stream into ctsdb
        DataStream<String> sessionStream = mainStream.getSideOutput(outputSessionTag);
        MsgSerializationScheme serSchema = new MsgSerializationScheme();
        CTSDBSink<String> sessionCtsdbSink = new CTSDBSink<>(ctsd_addr, session_metric_name, serSchema, batch_size, batch_time, user, passwd);
        LOG.info("ctsdb config: " + sessionCtsdbSink.toString());
        sessionStream.addSink(sessionCtsdbSink).name(ctsd_addr + session_metric_name).setParallelism(sink_parallelism);

        // delay data stream into zhiyan
        DataStream<String> zhiyanStream = mainStream.getSideOutput(outputZhiYanTag);
        ZhiYanSink<String> zhiyanSink = new ZhiYanSink<>(serSchema, zhiyanAppMark);
        LOG.info("zhiyanSink config: " + zhiyanSink.toString());
        zhiyanStream.addSink(zhiyanSink).name("ZhiYanReport").setParallelism(sink_parallelism);

        env.execute(jobName);
    }

    private static class MsgDeserializationScheme implements DeserializationSchema<String> {

        @Override
        public String deserialize(byte[] message) {
            String data = new String(message, ConfigConstants.DEFAULT_CHARSET);
            return data;
        }

        @Override
        public boolean isEndOfStream(String nextElement) { return false; }

        @Override
        public TypeInformation<String> getProducedType() { return TypeExtractor.getForClass(String.class); }
    }

    private static class MsgSerializationScheme implements SerializationSchema<String> {

        @Override
        public byte[] serialize(String message) {
            return message.getBytes();
        }
    }

    private static String readFromCmdOrConf(String name, CommandLine cmd) {
        if (cmd.hasOption(name)) {
            return cmd.getOptionValue(name);
        } else {
            return parameter.get(name);
        }
    }
}