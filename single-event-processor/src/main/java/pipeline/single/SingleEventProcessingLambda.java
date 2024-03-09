package pipeline.single;


import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import pipeline.model.WeatherEvent;


public class SingleEventProcessingLambda {
    private final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
            false);

    public void handleRequest(final SNSEvent snsEvent) {
        snsEvent.getRecords().forEach(this::refactorMessage);
    }

    private void refactorMessage(SNSEvent.SNSRecord snsRecord) {
        String message = snsRecord.getSNS().getMessage();
        WeatherEvent weatherEvent;
        try {
            weatherEvent = objectMapper.readValue(message, WeatherEvent.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Received weather event  from sns -->"+ weatherEvent);
    }

}
