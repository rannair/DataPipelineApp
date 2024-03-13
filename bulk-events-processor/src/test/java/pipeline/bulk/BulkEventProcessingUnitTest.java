package pipeline.bulk;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import pipeline.model.WeatherEvent;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.io.IOException;
import java.util.List;
@ExtendWith(SystemStubsExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BulkEventProcessingUnitTest {

   @SystemStub
    public EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @BeforeAll
    public void setEnvironmentVariables() {
        environmentVariables.set(BulkEventProcessingLambda.FAN_OUT_TOPIC_ENV, "test-topic");
    }

    @Test
    public void toWeatherEventsShouldMapCorrectly() throws IOException {
        BulkEventProcessingLambda bulkEventProcessingLambda = new BulkEventProcessingLambda();
        GetObjectResponse getObjectResponse = GetObjectResponse.builder().build();
        byte[] mockContent = getClass().getResourceAsStream("/bulk_data.json").readAllBytes();
        List<WeatherEvent> weatherEvents = bulkEventProcessingLambda.
                toWeatherEvents(ResponseBytes.fromByteArray(getObjectResponse, mockContent));

        // Assert

        Assertions.assertEquals(3, weatherEvents.size());
        Assertions.assertEquals("Brooklyn, NY", weatherEvents.get(0).getLocationName());
        Assertions.assertEquals(91.0, weatherEvents.get(0).getTemperature(), 0.0);
        Assertions.assertEquals(1564428897L, weatherEvents.get(0).getTimestamp(), 0);
        Assertions.assertEquals(40.7, weatherEvents.get(0).getLatitude(), 0.0);
        Assertions.assertEquals(-73.99, weatherEvents.get(0).getLongitude(), 0.0);

        Assertions.assertEquals("Oxford, UK", weatherEvents.get(1).getLocationName());
        Assertions.assertEquals(64.0, weatherEvents.get(1).getTemperature(), 0.0);
        Assertions.assertEquals(1564428897L, weatherEvents.get(1).getTimestamp(), 0);
        Assertions.assertEquals(51.75, weatherEvents.get(1).getLatitude(), 0.0);
        Assertions.assertEquals(-1.25, weatherEvents.get(1).getLongitude(), 0.0);

        Assertions.assertEquals("Charlottesville, VA", weatherEvents.get(2).getLocationName());
        Assertions.assertEquals(87.0, weatherEvents.get(2).getTemperature(), 0.0);
        Assertions.assertEquals(1564428897L, weatherEvents.get(2).getTimestamp(), 0);
        Assertions.assertEquals(38.02, weatherEvents.get(2).getLatitude(), 0.0);
        Assertions.assertEquals(-78.47, weatherEvents.get(2).getLongitude(), 0.0);
    }

    @Test
    public void toSnsMessageShouldReturnErrorForBadData() throws IOException {
        BulkEventProcessingLambda bulkEventProcessingLambda = new BulkEventProcessingLambda();
        GetObjectResponse getObjectResponse = GetObjectResponse.builder().build();
        byte[] mockContent = getClass().getResourceAsStream("/bad_data.json").readAllBytes();
        Assertions.assertThrows(RuntimeException.class,()->bulkEventProcessingLambda.toWeatherEvents(
                ResponseBytes.fromByteArray(getObjectResponse, mockContent) )
        );
    }

    @Test
    public void toSnsMessageShouldMapCorrectly() {
        BulkEventProcessingLambda bulkEventProcessingLambda = new BulkEventProcessingLambda();
        WeatherEvent weatherEvent = new WeatherEvent();
        weatherEvent.setLocationName("Brooklyn, NY");
        weatherEvent.setLatitude(40.7);
        weatherEvent.setLongitude(-73.99);
        weatherEvent.setTimestamp(1564428897L);
        weatherEvent.setTemperature(91.0);

        String snsMessage = bulkEventProcessingLambda.toSnsMessage(weatherEvent);
        Assertions.assertEquals("{\"locationName\":\"Brooklyn, NY\",\"latitude\":40.7," +
                        "\"longitude\":-73.99,\"temperature\":91.0,\"timestamp\":1564428897}",
                snsMessage);

    }
}
