package ma.enset.springcloudkafka.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import java.util.Date;

@Data @AllArgsConstructor @NoArgsConstructor @ToString
public class PageEvent {
    private String name;
    private String username;
    private Date date;
    private Long duration;

}
