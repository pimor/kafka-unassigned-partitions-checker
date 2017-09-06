package com.pim.hiring.scout24.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.Map;

/**
 * Created on 05/09/2017.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(properties = "scheduling.enabled=false")
@DirtiesContext
public class TestTopicPartitionsFiltering {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTopicPartitionsFiltering.class);

    // set to a non-existent topic
    private static String TOPIC_NAME = "topic314";

//    private Map<String, List<Integer>> topicPartitions = new HashMap<String, List<Integer>>();

    @Autowired
    private Map<String, Object> consumerConfigs;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.topics-to-check}")
    String[] topicsToCheck;

    private KafkaTemplate<String, String> template;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    // Create 3 partitions topic
    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 3, TOPIC_NAME);

    KafkaConsumer<String, String> kconsumer = null;


    @Before
    public void setUp() throws Exception {

        //////////////////////////////////////
        // Topic set up
        /////////////////////////////////////

        // set up the Kafka producer properties
        Map<String, Object> senderProperties =
                KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());

        // create a Kafka producer factory
        ProducerFactory<String, String> producerFactory =
                new DefaultKafkaProducerFactory<String, String>(senderProperties);

        // create a Kafka template
        template = new KafkaTemplate<>(producerFactory);
        // set the default topic to send to
        template.setDefaultTopic(TOPIC_NAME);

        // wait until the partitions are assigned
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer,
                    embeddedKafka.getPartitionsPerTopic());
        }


        /////////////////////////////////////
        // Consumer set up
        /////////////////////////////////////
        kconsumer = new KafkaConsumer<String, String>( (Map)consumerConfigs.get("consumerConfigs") );
        TopicPartition partition0 = new TopicPartition(TOPIC_NAME, 0);
        kconsumer.assign( Arrays.asList(partition0) );
        kconsumer.poll(1000);
        kconsumer.close();
    }


    @Test
    public void testTopicPartitionsFiltering() throws Exception {

        Assert.assertFalse( Arrays.asList(topicsToCheck).contains(TOPIC_NAME) );

    }

}
