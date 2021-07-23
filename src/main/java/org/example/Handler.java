package org.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class Handler implements RequestHandler<APIGatewayV2HTTPEvent, Void> {

    private static final String region = "placeholder";
    private static final String accessKeyId = "placeholder";
    private static final String secretAccessKey = "placeholder";
    private static final String topicArn = "placeholder";
    private static final String sqsUrl = "placeholder";

    private AmazonSNSClient amazonSNSClient = (AmazonSNSClient) AmazonSNSClientBuilder
            .standard()
            .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(accessKeyId, secretAccessKey)))
            .build();

    private AmazonSQSClient amazonSQSClient = (AmazonSQSClient) AmazonSQSClientBuilder
            .standard()
            .withRegion(region)
            .withCredentials(new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(accessKeyId, secretAccessKey)))
            .build();

    @Override
    public Void handleRequest(APIGatewayV2HTTPEvent apiGatewayV2HTTPEvent, Context context) {
        context.getLogger().log("Scheduler is running");

        ReceiveMessageResult receiveMessageResult = amazonSQSClient.receiveMessage(sqsUrl);
        receiveMessageResult.getMessages().stream()
                .peek(m -> amazonSQSClient.deleteMessage(sqsUrl, m.getReceiptHandle()))
                .map(Message::getBody)
                .peek(m -> context.getLogger().log(String.format("Message received %s", m)))
                .forEach(b -> amazonSNSClient.publish(new PublishRequest(topicArn, b, "New image has been uploaded")));

        return null;
    }
}
