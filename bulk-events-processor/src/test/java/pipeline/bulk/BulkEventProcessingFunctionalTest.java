package pipeline.bulk;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.tests.EventLoader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.io.IOException;

@ExtendWith(SystemStubsExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BulkEventProcessingFunctionalTest {

    @SystemStub
    public EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @BeforeAll
    public void setEnvironmentVariables() {
        environmentVariables.set(BulkEventProcessingLambda.FAN_OUT_TOPIC_ENV, "test-topic");
    }

    @Test
    public void handleRequestPublishesToSnsCorrectly() throws IOException {
        // Set up mock AWS SDK clients
        S3Client mockS3Client = Mockito.mock(S3Client.class);
        SnsClient mockSnsClient = Mockito.mock(SnsClient.class);

        // Fixture S3 event
        S3Event s3Event = EventLoader.loadS3Event("/s3_event.json");

        // Fixture S3 return value
        GetObjectResponse getObjectResponse = GetObjectResponse.builder().build();
        byte[] mockContent = getClass().getResourceAsStream("/bulk_data.json").readAllBytes();
        ResponseBytes<GetObjectResponse> mocks3Response = ResponseBytes.fromByteArray(getObjectResponse, mockContent);
        Mockito.when(mockS3Client.getObjectAsBytes(Mockito.any(GetObjectRequest.class)))
                .thenReturn(mocks3Response);

        // Construct Lambda function class, and invoke handler
        BulkEventProcessingLambda bulkEventProcessingLambda = new BulkEventProcessingLambda(mockS3Client, mockSnsClient);
        bulkEventProcessingLambda.handleRequest(s3Event);

        ArgumentCaptor<PublishRequest> publishRequest = ArgumentCaptor.forClass(PublishRequest.class);
        Mockito.verify(mockSnsClient, Mockito.times(3)).publish(publishRequest.capture());

    }
}
