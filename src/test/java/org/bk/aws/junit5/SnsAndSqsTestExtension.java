package org.bk.aws.junit5;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SNS;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

public final class SnsAndSqsTestExtension implements BeforeAllCallback, AfterAllCallback {

    private LocalStackContainer server;
    private String snsEndpoint;
    private String sqsEndpoint;

    @Override
    public void beforeAll(ExtensionContext context) {
        System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), "access-key");
        System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), "secret-key");

        try {
            this.server = new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.12.12")).withServices(SNS, SQS);
            this.server.start();
            this.snsEndpoint = this.server.getEndpointConfiguration(SQS).getServiceEndpoint();
            this.sqsEndpoint = this.server.getEndpointConfiguration(SQS).getServiceEndpoint();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (this.server == null) {
            return;
        }

        try {
            this.server.stop();
            System.clearProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property());
            System.clearProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getSnsEndpoint() {
        return snsEndpoint;
    }

    public String getSqsEndpoint() {
        return sqsEndpoint;
    }
}
