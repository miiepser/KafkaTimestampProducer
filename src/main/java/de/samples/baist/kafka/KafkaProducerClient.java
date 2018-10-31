package de.samples.baist.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.apache.kafka.streams.StreamsConfig;


@Component
public class KafkaProducerClient implements CommandLineRunner {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaProducerClient.class);
    public static final String TOPIC = "test-producer";
    private final KafkaProducer myPro;
    private static final Properties PROPERTIES= new Properties();;
    {
        PROPERTIES.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit-prod");

        PROPERTIES.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093");
//        props.put("key.serializer", Serdes.String().getClass());
        //props.put("value.serializer", Serdes.String().getClass());

        PROPERTIES.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        PROPERTIES.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        PROPERTIES.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        PROPERTIES.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    }


    public KafkaProducerClient() {
        this.myPro = new KafkaProducer(PROPERTIES);
    }


    private static void parseProperties(String[] optionalProps) {
        for(int i=0;i<Arra)
    }

    @Override
    public void run(String... args) throws Exception {
        AtomicBoolean ab = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                ab.set(false);
            }
        });


        while (ab.get()) {
            try {
                long sleepTime = (long) (50000d * Math.random());
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                LOG.error("Thread got interrupted", e);
                return;
            }
            for (int i = 0; i < (Math.random() * 4); ++i) {
                send(TOPIC, "some value to be received. Now it is: " + new Date().getTime());
//                client.send(TOPIC, new SendStartTime(new Date(), "We translate no texts, you won't underst∞nd", i));
            }
        }

    }




    public void send(final String topic, final String value) {
        ProducerRecord pr = new ProducerRecord(topic, value);
        new ProducerRecord<String, String>(topic, value);
        myPro.send(pr);
    }

}
