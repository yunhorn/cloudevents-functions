package com.yunhorn.core.cloudevents.functions;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventSerializer;
import java.net.URI;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.springframework.util.StringUtils;

/**
 * pulsar functions of kafka cloudevents
 */
public class CloudEventKafkaFunction implements Function<byte[], byte[]> {

    KafkaProducer<String, CloudEvent> producer;

    @Override
    public byte[] process(byte[] input, Context context) {

        String server = context.getUserConfigMap().getOrDefault("bootstrap.servers","").toString();
        if(!StringUtils.hasText(server)){
            return input;
        }

        if(producer==null){
            synchronized (producer){
                if(producer==null){
                    // Basic producer configuration
                    Properties props = new Properties();
                    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
                    props.put(ProducerConfig.CLIENT_ID_CONFIG, context.getUserConfigMap().getOrDefault("client.id","cloudevent-kafka-producer").toString());
                    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

                    // Configure the CloudEventSerializer to emit events as json structured events
                    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
                    props.put(CloudEventSerializer.ENCODING_CONFIG, Encoding.STRUCTURED);
                    props.put(CloudEventSerializer.EVENT_FORMAT_CONFIG, JsonFormat.CONTENT_TYPE);

                    // Create the KafkaProducer
                    producer = new KafkaProducer<>(props);
                }
            }
        }


        String topic = context.getOutputTopic();

        // Create an event template to set basic CloudEvent attributes
        CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                .withSource(URI.create(context.getUserConfigMap().getOrDefault("ce.source","cloudevent-source-kafka-producer").toString()))
                .withType(context.getUserConfigMap().getOrDefault("ce.type","cloudevent-type-kafka-producer").toString());

            try {
                String id = context.getCurrentRecord().getProperties().getOrDefault("ce.id","");
                if(id.length()==0){
                    id = UUID.randomUUID().toString();
                }

                // Create the event starting from the template
                CloudEvent event = eventTemplate.newBuilder()
                        .withId(id)
                        .withData("text/plain", input)
                        .build();

                // Send the record
                RecordMetadata metadata = producer
                        .send(new ProducerRecord<>(topic, id, event))
                        .get();
                context.getLogger().error("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset());
            } catch (Exception e) {
                context.getLogger().error("Error while trying to send the record!",e);
            }

//        producer.flush();

        return input;
    }
}
