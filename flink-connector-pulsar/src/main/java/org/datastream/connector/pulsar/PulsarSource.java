package org.datastream.connector.pulsar;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class PulsarSource<OUT> extends MessageAcknowledgingSourceBase<OUT, MessageId>
        implements ResultTypeQueryable<OUT>, ParallelSourceFunction<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSource.class);

    private final PulsarConfig config;
    private volatile PulsarClient client;
    private volatile Consumer<byte[]> consumer;
    private Lock initLock = new ReentrantLock();
    private Condition initCond = initLock.newCondition();
    private volatile boolean initFinish = false;

    protected DeserializationSchema<OUT> schema;

    // the max retry time for sending
    private static final int ACK_FAILED_MAX_RETRY = 3;
    // the timeout of ack
    private static final int ACK_TIMEOUT = 60;

    public PulsarSource(PulsarConfig config, DeserializationSchema<OUT> schema) {
        super(MessageId.class);
        this.config = config;
        this.schema = schema;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }

    @Override
    public void run(SourceContext<OUT> ctx) throws Exception {
        LOG.info("[Pulsar] Source config: {}", config.toString());

        try {
            connect(ctx);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("failed to connect Pulsar {}", e.getMessage());
            disconnect();
            throw e;
        }

        LOG.info("[Pulsar] Source stopped");
    }

    @Override
    public void cancel() {
        LOG.info("[Pulsar] task try canceled");
        disconnect();
        LOG.info("[Pulsar] task canceled successfully");
    }

    /**
     * the function to be called when checkpoint is finished
     */
    @Override
    protected void acknowledgeIDs(long checkpointId, Set<MessageId> ids) {
        if (ids.size() == 0) {
            return;
        }

        Consumer<byte[]> consumerSnap = consumer;
        if (consumerSnap == null && !initFinish) {
            try {
                initLock.lock();
                while (!initFinish) {
                    LOG.warn("[Pulsar] consumer is initializing... waiting");
                    initCond.await(1, TimeUnit.MINUTES);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                initLock.unlock();
            }
            consumerSnap = consumer;
        }
        if (consumerSnap == null) {
            LOG.warn("[Pulsar] consumer is null! may fail to create");
            return;
        }

        List<MessageId> messageIds = new ArrayList<>(ids);
        for (int i = 1; i <= ACK_FAILED_MAX_RETRY; i++) {
            try {
                consumerSnap.acknowledge(messageIds);
                LOG.debug("[Pulsar] ack {} msgs at checkpoint", messageIds.size());
                break;
            } catch (PulsarClientException e) {
                e.printStackTrace();
                LOG.error("[Pulsar] failed to ack msgs: {} retry time: {}", e.getMessage(), i);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ignore) {}
            }
        }
    }

    /**
     * create comsumer and start to receive message
     */
    private void connect(SourceContext<OUT> ctx) throws Exception {
        try {
            initLock.lock();

            LOG.info("[Pulsar] try to connect Pulsar");
            String serviceUrl = String.format(PulsarConfig.PULSAR_SERVICE_URL_TPL, this.config.getRegion());
            client = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .authentication(AuthenticationFactory.token(config.getToken()))
                    .connectionTimeout(30, TimeUnit.SECONDS)
                    .operationTimeout(30, TimeUnit.SECONDS)
                    .build();
            LOG.info("[Pulsar] pulsarClient created");

            String[] topics = this.config.getTopics();
            List<String> fullTopics = Arrays.stream(topics)
                    .map(topic -> String.format(PulsarConfig.PULSAR_TOPIC_TPL, config.getTenant(), config.getNamespace(), topic))
                    .collect(Collectors.toList());   

            consumer = client.newConsumer()
                    .topics(fullTopics) // topic list
                    .subscriptionName(config.getSubscription())
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .autoUpdatePartitions(true)
                    .autoUpdatePartitionsInterval(30, TimeUnit.SECONDS)
                    .ackTimeout(ACK_TIMEOUT, TimeUnit.SECONDS)
                    .messageListener(new MsgListener(ctx))
                    .subscribe();
            LOG.info("[Pulsar] consumer created");

        } finally {
            initFinish = true;
            initCond.signalAll();
            initLock.unlock();
        }
    }

    private void disconnect() {
        LOG.info("Pulsar try to disconnect");
        try {
            if (consumer != null) {
                consumer.close();
            }
            if (client != null) {
                client.close();
            }
            LOG.info("[Pulsar] Pulsar disconnected successfully");
        } catch (PulsarClientException ex) {
            LOG.error("[Pulsar] failed to disconnect from Pulsar: {}", ex.getMessage());
        }
    }

    private class MsgListener implements MessageListener<byte[]> {

        private final SourceContext<OUT> ctx;

        public MsgListener(SourceContext<OUT> ctx) {
            this.ctx = ctx;
        }

        @Override
        public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
            OUT result;
            try {
                result = schema.deserialize(msg.getData());
            } catch (IOException e) {
                LOG.error("msg deserialize failed : {}", e.getMessage());
                return;
            }
            if (schema.isEndOfStream(result)) {
                return;
            }
            boolean needReconsume = false;
            MessageId msgId = msg.getMessageId();
            synchronized (ctx.getCheckpointLock()) {
                if (!addId(msgId)) {
                    return;
                }
                try {
                    ctx.collect(result);
                } catch (RuntimeException e) {
                    LOG.error("[Pulsar] failed to push msg to downstream: {}", e.getMessage());
                    needReconsume = true;
                }
            }
            if (config.getResendIfFailed() && needReconsume) {
                consumer.negativeAcknowledge(msgId);
            }
        }
    }
}


