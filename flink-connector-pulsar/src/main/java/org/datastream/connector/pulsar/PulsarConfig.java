package org.datastream.connector.pulsar;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;

public class PulsarConfig implements Serializable {

    public static final String PULSAR_SERVICE_URL_TPL = "http://tdmq.%s.tencentyun.com:8080";
    public static final String PULSAR_TOPIC_TPL = "persistent://%s/%s/%s";

    private String region;
    private String tenant;
    private String namespace;
    private String token;
    private String[] topics;
    private String subscription;
    private boolean resendIfFailed;

    public PulsarConfig(String region, String tenant, String namespace, String token, String[] topics, String subscription, boolean resendIfFailed) {
        Preconditions.checkNotNull(region, "region can not be null");
        Preconditions.checkNotNull(tenant, "tenant can not be null");
        Preconditions.checkNotNull(namespace, "namespace can not be null");
        Preconditions.checkNotNull(token, "token can not be null");
        Preconditions.checkNotNull(topics, "topics can not be null");
        Preconditions.checkNotNull(subscription, "subscription can not be null");
        Preconditions.checkNotNull(resendIfFailed, "resendIfFailed can not be null");

        this.region = region;
        this.tenant = tenant;
        this.namespace = namespace;
        this.token = token;
        this.topics = topics;
        this.subscription = subscription;
        this.resendIfFailed = resendIfFailed;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String[] getTopics() {
        return topics;
    }

    public void setTopics(String[] topics) {
        this.topics = topics;
    }

    public String getSubscription() {
        return subscription;
    }

    public void setSubscription(String subscription) {
        this.subscription = subscription;
    }

    public boolean getResendIfFailed() {
        return resendIfFailed;
    }

    public void setResendIfFailed(boolean resendIfFailed) {
        this.resendIfFailed = resendIfFailed;
    }

    @Override
    public String toString() {
        return "PulsarConfig{" +
                "region='" + region + '\'' +
                ", tenant='" + tenant + '\'' +
                ", namespace='" + namespace + '\'' +
                ", token='" + token + '\'' +
                ", topics=" + Arrays.toString(topics) +
                ", subscription='" + subscription + '\'' +
                '}';
    }
}

