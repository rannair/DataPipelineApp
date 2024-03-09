package pipeline.model;

import lombok.Data;

@Data
public class WeatherEvent {
    private String locationName;
    private Double latitude;
    private Double longitude;
    private Double temperature;
    private Long timestamp;

}
