package com.pim.hiring.scout24.kafka.checker;

import kafka.admin.AdminClient;
import kafka.coordinator.GroupOverview;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import scala.collection.JavaConversions;

import java.util.*;


/**
 * Created on 28/08/2017.
 */
@Component
public class UnassignedTopicPartitions {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnassignedTopicPartitions.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;


    // Constructor
    public void UnassignedTopicPartitions() {}


    /**
     * Create a Kafka admin client
     * @param bootstrapServers Configured Kafka servers in the application.yml file
     * @return {@link AdminClient}
     */
    public AdminClient createKafkaAdminClient(String bootstrapServers) {
        // create Kafka AdminClient
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );

        AdminClient adminClient = AdminClient.create(props);

        return adminClient;
    }

    /**
     * Return the {@link AdminClient} if exists or create a new one
     * @return {@link AdminClient}
     */
    public AdminClient getKafkaAdminClient() {
        return createKafkaAdminClient(bootstrapServers);
    }


    /**
     * Get the partitions identifiers for a specific topic
     * @param kafkaTemplate The {@link KafkaTemplate} Kafka template
     * @param topic The topic name
     * @return A map with the topic name as key and the partition numbers as value.
     * Return {@code null} if no partitions are found
     */
    public Map<String, List<Integer>> getTopicPartitionsInfo(KafkaTemplate kafkaTemplate, String topic) {
        Map<String, List<Integer>> topicPartitionsInfo = null;
        List<PartitionInfo> partitionInfo = partitionInfo = kafkaTemplate.partitionsFor(topic);

        if (partitionInfo != null) {
            topicPartitionsInfo = new HashMap<>();
            // Store partition number
            List<Integer> partitionsList = new ArrayList<>();
            for (PartitionInfo partition : partitionInfo) {
                partitionsList.add(partition.partition());
            }
            topicPartitionsInfo.put(topic, partitionsList);
        }
        return topicPartitionsInfo;
    }


    /**
     * Get the partition assignment for a specific consumer
     * @param consumer The {@link KafkaConsumer} consumer
     * @return A map with the topic name as key and the partition numbers as value.
     * Return {@code null} if no assignments are found
     */
    public Map<String, List<Integer>> getManualPartitionAssignment(KafkaConsumer<String, String> consumer) {
        Map<String, List<Integer>> consumerTopicPartitionsList = null;

        Set<TopicPartition> topicPartitions = consumer.assignment();

        if( !topicPartitions.isEmpty() ) {
            consumerTopicPartitionsList = new HashMap<>();

            for (TopicPartition topicPartition : topicPartitions) {
                List<Integer> partitionsList = new ArrayList<>();

                String topic = topicPartition.topic();

                // Update partition list if topic exists
                if(consumerTopicPartitionsList.containsKey(topic) )
                    // Update topic-partitions relationship
                    consumerTopicPartitionsList.get(topic).add(topicPartition.partition());
                else {
                    partitionsList.add(topicPartition.partition());
                    // Add a new topic-partition relationship
                    consumerTopicPartitionsList.put(topic, partitionsList);
                }
            }
        }
        return consumerTopicPartitionsList;
    }

    /**
     * Get the assigned partitions to each topic/consumer-group/consumer key
     * [topic#consumer-group, [topic, {partitions}]]
     * @param {@link AdminClient} Kafka admin client
     * @return a map with the topic#consumer-group as key and a Map with the [topic, {partitions}] relationship as value
     */
    public Map<String, Map<String, List<Integer>>> getTopicAndPartitionsForAllConsumerGroups(AdminClient adminClient) {
        Map<String, Map<String, List<Integer>>> topicConsumergroupConsumerPartitions = new HashMap<>();


        // TODO: Why this method doesn't get the assignments?
//        List<GroupOverview> groups =  scala.collection.JavaConversions.seqAsJavaList(
//            adminClient.listAllConsumerGroupsFlattened()
//        );

        List<GroupOverview> groups =  scala.collection.JavaConversions.seqAsJavaList(
                adminClient.listAllGroupsFlattened()
        );


        // for each consumer group...
        for (GroupOverview group : groups) {

            // Get consumer group offsets
            Map<TopicPartition, Object> groupOffsets = JavaConversions.mapAsJavaMap(
                    adminClient.listGroupOffsets( group.groupId() )
            );
            // Extract only the topic partitions
            Set<TopicPartition> consumerTopicPartitions = groupOffsets.keySet();


            // for each partition within the consumer
            for (TopicPartition tp : consumerTopicPartitions) {

                // topic-partitions relationship for a particular consumer
                Map<String, List<Integer>> topicPartitionsList = new HashMap<>();
                // partition list
                List<Integer> partitionsList = new ArrayList<>();

                partitionsList.add( tp.partition() );
                // Add a new topic-partition relationship
                topicPartitionsList.put(tp.topic(), partitionsList);

                // key: topic#consumer-group  value: [topic, {partitions}]
                String key = tp.topic() + "#" + group.groupId();

                if (topicConsumergroupConsumerPartitions.containsKey(key)) {

                    // Add a new partition to an existed relation
                    topicConsumergroupConsumerPartitions.replace(key,
                            addNewPartition(topicConsumergroupConsumerPartitions.get(key), tp.topic(), tp.partition())
                        );
                } else {
                    topicConsumergroupConsumerPartitions.put(key, topicPartitionsList);
                }
            }
        }
        return topicConsumergroupConsumerPartitions;
    }


    /**
     * Prints the topic partitions that have not been configured in the consumer
     * @param topicPartitionsList The topic-partitions list
     * @param consumerGroupConsumerTopicPartitionsList The topic/consumer-group/partitions relationship
     * @return <tt>true</tt> if the same topic and partitions are configured for the topic and the consumer
     */
    public boolean comparePartitionLists(Map<String, List<Integer>> topicPartitionsList,
                                         Map.Entry<String, Map<String, List<Integer>>> consumerGroupConsumerTopicPartitionsList) {

        boolean equals = true;

        // Get the consumer-partitions
        Map<String, List<Integer>> consumerTopicPartitionsList = consumerGroupConsumerTopicPartitionsList.getValue();


        if( !topicPartitionsList.equals(consumerTopicPartitionsList) ) {

            for( String key: topicPartitionsList.keySet() ) {
                if( consumerTopicPartitionsList.containsKey(key) ) {

                    for(Integer topicPartition : topicPartitionsList.get(key)) {
                        if( !consumerTopicPartitionsList.get(key).contains(topicPartition) ) {
                            LOGGER.warn("Consumer " + consumerGroupConsumerTopicPartitionsList.getKey() +
                                    " has missed the partition " + key + "-" + String.valueOf(topicPartition) );
                            equals = false;
                        }
                    }
                }
            }

        }
        return equals;
    }


    /**
     * Add a new partition to an existed topic-consumer relationship
     * @param topicPartitionsList The current topic-partition relationship
     * @param topic The topic where the new partition has to be added
     * @param  newPartition The new partition number
     * @return The topic-partition relationship with the new partition
     */
    public Map<String, List<Integer>> addNewPartition(Map<String, List<Integer>> topicPartitionsList, String topic, int newPartition) {
        List<Integer> newPartitionsList = topicPartitionsList.get(topic);
        newPartitionsList.add(newPartition);
        topicPartitionsList.replace(topic, newPartitionsList);

        return topicPartitionsList;
    }
}
