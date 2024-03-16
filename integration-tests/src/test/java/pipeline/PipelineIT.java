package pipeline;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.DescribeStackResourceRequest;
import software.amazon.awssdk.services.cloudformation.model.DescribeStackResourceResponse;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;

public class PipelineIT {
    private String stackName;
    private final Region region = Region.US_EAST_1;
    private final CloudFormationClient cloudFormationClient = CloudFormationClient
            .builder().region(region).build();
    private final CloudWatchLogsClient cloudWatchLogsClient = CloudWatchLogsClient.builder()
            .region(region).build();
    private final S3Client s3Client = S3Client.builder().build();


    public PipelineIT() {
        this.stackName = System.getProperty("stackName");
        if (stackName == null) {
            throw new RuntimeException("Stack Name property must be set");
        }
    }

    @Test
    public void endToEndTest() throws InterruptedException {

        //Upload File to s3 bucket
        String bucketName = resolvePhysicalId("WeatherEventsData");
        String key = UUID.randomUUID().toString();
        File file = new File(getClass().getResource("/bulk_data.json").getFile());
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));

        //Check for execution of SingleEventProcessingLambda function
        Thread.sleep(30000);
        Set<String> logMessages = getLogMessages("SingleEventProcessingLambda");
        assertThat(logMessages, CoreMatchers.hasItems(
                "Received weather event  from sns -->WeatherEvent(locationName=Charlottesville, VA, latitude=38.02, longitude=-78.47, temperature=87.0, timestamp=1564428899)",
                "Received weather event  from sns -->WeatherEvent(locationName=Oxford, UK, latitude=51.75, longitude=-1.25, temperature=64.0, timestamp=1564428898)",
                "Received weather event  from sns -->WeatherEvent(locationName=Brooklyn, NY, latitude=40.7, longitude=-73.99, temperature=91.0, timestamp=1564428897)"
        ));

        // 3. Delete object from S3 bucket (to allow a clean CloudFormation teardown)
        s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(key).build());

        // 4. Delete Lambda log groups
        cloudWatchLogsClient.deleteLogGroup(DeleteLogGroupRequest.builder()
                .logGroupName(getLogGroup(resolvePhysicalId("SingleEventProcessingLambda")))
                .build());
        cloudWatchLogsClient.deleteLogGroup(DeleteLogGroupRequest.builder()
                .logGroupName(getLogGroup(resolvePhysicalId("BulkEventProcessingLambda")))
                .build());


    }

    private Set<String> getLogMessages(String lambdaName) {
        String logGroupName = getLogGroup(resolvePhysicalId(lambdaName));

        DescribeLogStreamsRequest logStreamsRequest = DescribeLogStreamsRequest
                .builder().logGroupName(logGroupName)
                .build();
        return cloudWatchLogsClient.describeLogStreams(logStreamsRequest)
                .logStreams().stream()
                .map(LogStream::logStreamName)
                .flatMap(logStreamName -> cloudWatchLogsClient.getLogEvents(GetLogEventsRequest
                                .builder().logGroupName(logGroupName)
                                .logStreamName(logStreamName).build())
                        .events().stream())
                .map(OutputLogEvent::message)
                .filter(message -> message.contains("WeatherEvent"))
                .map(String::trim).collect(Collectors.toSet());
    }

    private String resolvePhysicalId(String logicalId) {
        DescribeStackResourceRequest describeStackResourceRequest = DescribeStackResourceRequest
                .builder().stackName(stackName).logicalResourceId(logicalId).build();

        DescribeStackResourceResponse describeStackResourceResponse =
                cloudFormationClient.describeStackResource(describeStackResourceRequest);
        return describeStackResourceResponse.stackResourceDetail().physicalResourceId();

    }


    private String getLogGroup(String lambdaName) {
        return String.format("/aws/lambda/%s", lambdaName);
    }

}