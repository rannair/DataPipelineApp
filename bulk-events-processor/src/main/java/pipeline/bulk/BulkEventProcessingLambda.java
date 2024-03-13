package pipeline.bulk;


import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import pipeline.model.WeatherEvent;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handler for requests to Lambda function.
 */
public class BulkEventProcessingLambda {
    static String FAN_OUT_TOPIC_ENV = "FAN_OUT_TOPIC";

    private final S3Client s3Client;

    private final SnsClient snsClient;

    private final ObjectMapper objectMapper;
    private final String snsTopic;

    public BulkEventProcessingLambda() {
        this(S3Client.builder().build(), SnsClient.builder().build());
    }

    public BulkEventProcessingLambda(S3Client s3Client, SnsClient snsClient) {
        this.s3Client = s3Client;
        this.snsClient = snsClient;
        this.objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
        this.snsTopic = System.getenv(FAN_OUT_TOPIC_ENV);
        if (this.snsTopic == null) {
            throw new RuntimeException(String.format("%s must be set", FAN_OUT_TOPIC_ENV));
        }
    }

    public void handleRequest(final S3Event s3Event) {
        // Get list of Weather events by reading  data from s3.
        List<WeatherEvent> weatherEvents = s3Event.getRecords().stream()
                .map(this::getObjectFromS3)
                .map(this::toWeatherEvents)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        // Convert and send message to sns
        weatherEvents.stream()
                .map(this::toSnsMessage)
                .forEach(this::publishToSns);

        System.out.println("Published " + weatherEvents.size()
                + " weather events to SNS");
    }

    public ResponseBytes<GetObjectResponse> getObjectFromS3(S3EventNotification.S3EventNotificationRecord s3Record) {
        String bucketName = s3Record.getS3().getBucket().getName();
        String keyName = s3Record.getS3().getObject().getKey();
        GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .key(keyName)
                .bucket(bucketName)
                .build();
        ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
        return objectBytes;
    }

    public List<WeatherEvent> toWeatherEvents(ResponseBytes<GetObjectResponse> objectBytes) {
        byte[] data = objectBytes.asByteArray();
        WeatherEvent[] weatherEvents;
        try {
            weatherEvents = objectMapper.readValue(data, WeatherEvent[].class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Arrays.asList(weatherEvents);
    }

    private void publishToSns(String message) {
        snsClient.publish(
                PublishRequest.builder().
                        message(message)
                        .topicArn(snsTopic).
                        build());
    }

    public String toSnsMessage(WeatherEvent weatherEvent) {
        try {
            return objectMapper.writeValueAsString(weatherEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
