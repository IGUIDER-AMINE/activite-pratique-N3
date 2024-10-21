package ma.enset.springcloudkafka.service;

import ma.enset.springcloudkafka.entities.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.beans.BeanProperty;
import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.springframework.integration.graph.LinkNode.Type.input;


@Service
public class PageEventService {

   @Bean
    public Consumer<PageEvent> pageEventConsumer(){
       System.out.println("***************************");
       return (input)->{
            System.out.println("***************************");
            System.out.println(input.toString());

        };

    }
    /*
    @KafkaListener(topics = "R1",groupId ="test-group")
    public void consume(PageEvent input) {
        System.out.println("***************************");
        System.out.println(input.toString());
    }*/
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return ()->
            new PageEvent(Math.random() > 0.5 ? "P1" : "p2",
                    Math.random() > 0.5 ? "U1" : "U2",
                    new Date(),
                    new Random().nextLong(9000));

    }
   /* @Bean
    public Function<PageEvent,PageEvent> pageEventFunction() {
        return (input) -> {
            input.setName("page1");
            input.setUsername("khadija");
            return input;
        };
    }*/
    @Bean
    public Function<KStream<String,PageEvent>,KStream<String,Long>> kStreamFonction(){
        return (input)->
            input.filter((k,v)->v.getDuration()>100)
                    .map((k,v)->new KeyValue<>(v.getName(),v.getDuration()))
                    .groupByKey(Grouped.with(Serdes.String(),Serdes.Long()))
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(1)))
                    .reduce((acc, v)->{long sum=(acc+v)/2; return sum;}, Materialized.as("stats-store"))
                    .toStream()
                    .map((k,v)->new KeyValue<>("=>"+k.window().startTime()+k.window().endTime()+":"+k.key(),v));

    }
}
