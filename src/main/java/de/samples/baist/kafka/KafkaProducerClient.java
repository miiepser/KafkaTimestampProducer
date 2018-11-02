package de.samples.baist.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
    public static String TOPIC = "test-producer";
    private KafkaProducer myPro;
    private static final Properties PROPERTIES = new Properties();
    ;

    {
        PROPERTIES.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit-prod");

        PROPERTIES.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        PROPERTIES.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        PROPERTIES.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        PROPERTIES.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        PROPERTIES.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    }


    public KafkaProducerClient() {
    }


    private static void parseProperties(String[] optionalProps) {
        for (int i = 0; i < CollectionUtils.size(optionalProps); ++i) {
            final String currentArg = optionalProps[i];
            switch (currentArg) {
                case "bootstrap.servers":
                    PROPERTIES.put(currentArg, optionalProps[++i]);
                    break;
                case "send.topic":
                    TOPIC = optionalProps[++i];
                    break;
                default:
                    break;
            }
        }
        addPropertyIfSet("bootstrap.servers");
        TOPIC = StringUtils.defaultIfEmpty(System.getenv("send.topic"), TOPIC);
    }


    private static void addPropertyIfSet(final String propertyName) {
        final String propertyValue = System.getenv(propertyName);
        if (null != propertyValue) {
            PROPERTIES.put(propertyName, propertyValue);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        parseProperties(args);

        LOG.info("given configuration {}, {}" , PROPERTIES, TOPIC);
        LOG.info("Sending to Topic {} received configured broker server {} ", TOPIC, PROPERTIES.getProperty("bootstrap.servers"));
        this.myPro = new KafkaProducer(PROPERTIES);

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
            final int iterations = (int)(Math.random() * 4);
            LOG.info("Sending {} messages. ", iterations);
            for (int i = 0; i < iterations; ++i) {
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
