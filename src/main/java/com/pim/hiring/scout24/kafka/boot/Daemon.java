package com.pim.hiring.scout24.kafka.boot;

import com.pim.hiring.scout24.kafka.checker.UnassignedTopicPartitions;
import kafka.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created on 01/09/2017.
 */
@Service
public class Daemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(Daemon.class);

    @Autowired
    private UnassignedTopicPartitions checker;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Map<String, Object> consumerConfigs;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.topics-to-check}")
    String[] topicsToCheck;

    //executes each xxx ms
    @Scheduled(fixedRate = 60000)
    public void init() {

        checker = new UnassignedTopicPartitions();
        AdminClient adminClient = checker.createKafkaAdminClient(bootstrapServers);

        Map<String, Map<String, List<Integer>>> topicConsumergroupConsumerPartitions =
                checker.getTopicAndPartitionsForAllConsumerGroups(adminClient);

        // iterate over each consumer group
        for(Map.Entry<String, Map<String, List<Integer>>> entry : topicConsumergroupConsumerPartitions.entrySet()) {

            String topic = entry.getValue().keySet().iterator().next();

            if( Arrays.asList(topicsToCheck).contains(topic) || topicsToCheck.length==0 ) {
                LOGGER.info("Checking consumer group " + entry.getKey() + " and topic " + topic);

                // TODO: Create a topic-partition cache to reduce the calls to this method
                // Get the partitions for each topic
                Map<String, List<Integer>> topicPartitionsList =
                        checker.getTopicPartitionsInfo(kafkaTemplate, topic);

                // compare consumer-partitions Vs topic-partitions
                checker.comparePartitionLists(topicPartitionsList, entry);
            } else {
                LOGGER.warn("Skipping topic " + topic + " because it isn't in the topics-to-check list.");
            }
        }
    }
}