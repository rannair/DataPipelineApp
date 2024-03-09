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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Handler for requests to Lambda function.
 */
public class BulkEventProcessingLambda {

    private final S3Client s3Client = S3Client.builder().build();
    private final SnsClient snsClient = SnsClient.builder().build();
    private final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
            false);
    private final String topic = System.getenv("FAN_OUT_TOPIC");


    public void handleRequest(final S3Event s3Event) {
        s3Event.getRecords().forEach(this::processS3EventRecord);
        SnsClient.builder().build();
    }

    private void processS3EventRecord(S3EventNotification.S3EventNotificationRecord s3Record) {
        List<WeatherEvent> weatherEvents = getWeatherEvents(s3Record);
        weatherEvents.stream().map(this::toSnsMessage).forEach(message -> snsClient.publish(
                PublishRequest.builder().
                        message(message)
                        .topicArn(topic).
                        build()));

    }

    private String toSnsMessage(WeatherEvent weatherEvent) {
        try {
            return objectMapper.writeValueAsString(weatherEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private List<WeatherEvent> getWeatherEvents(S3EventNotification.S3EventNotificationRecord s3Record) {
        String bucketName = s3Record.getS3().getBucket().getName();
        String keyName = s3Record.getS3().getObject().getKey();
        GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .key(keyName)
                .bucket(bucketName)
                .build();
        ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
        byte[] data = objectBytes.asByteArray();
        WeatherEvent[] weatherEvents;
        try {
            weatherEvents = objectMapper.readValue(data, WeatherEvent[].class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Arrays.asList(weatherEvents);
    }

}
