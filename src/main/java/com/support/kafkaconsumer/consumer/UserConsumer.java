package com.support.kafkaconsumer.consumer;

import com.google.gson.Gson;
import com.support.kafkaconsumer.model.User;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Calendar;

@Component
public class UserConsumer {

    private final Gson jsonConverter;

    public UserConsumer(Gson jsonConverter) {
        this.jsonConverter = jsonConverter;
    }

    @KafkaListener(id = "primaryListener", topics = "test", groupId = "testGroup")
    public void listenToConsumer(String data) {
        try {
            User user = jsonConverter.fromJson(data, User.class);
            System.out.println("Object: " + user);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("data: " + data + "TimeStamp: " + Calendar.getInstance().getTime());
//        throw new TimeoutException();
    }

    /**
     * we can have different error handling for this listener.
     * If these calls also fail, we can log them to error topic
     *
     * @param data
     */
    @KafkaListener(id = "secondaryListener", topics = "test.DLT", groupId = "testGroup", autoStartup = "true", containerFactory = "kafkaListenerContainerFactoryDLT")
    public void listenToTheRetryTopic(String data) {
        System.out.println("retrying the data from DLT topic: " + data);
    }

    @EventListener(condition = "event.listenerId.startsWith('secondaryListener-')")
    public void idle(ListenerContainerIdleEvent event) {
        AbstractMessageListenerContainer<String, String> container = event.getContainer(ConcurrentMessageListenerContainer.class);
        if (container != null) {
            System.out.println("Stopping the container");
            System.out.println(container);
            container.stop();
        }
    }

}
