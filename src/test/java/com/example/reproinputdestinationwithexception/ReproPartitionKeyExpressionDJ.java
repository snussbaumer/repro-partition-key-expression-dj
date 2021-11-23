package com.example.reproinputdestinationwithexception;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.support.MessageBuilder;

@SpringBootTest(properties = { "spring.cloud.stream.source=outputA;outputB",
        "spring.cloud.stream.bindings.outputA-out-0.destination=outputA",
        "spring.cloud.stream.bindings.outputA-out-0.producer.partition-count=3",
        "spring.cloud.stream.bindings.outputA-out-0.producer.partition-key-expression=headers['partitionKey']",
        "spring.cloud.stream.bindings.outputB-out-0.destination=outputB",
        "spring.cloud.stream.bindings.outputB-out-0.producer.partition-count=3",
        // uncomment line below to make the test work. potential workaround, but this should not be necessary
        // "spring.cloud.stream.bindings.outputB-out-0.producer.partition-key-expression=payload",
})
@Import(TestChannelBinderConfiguration.class)
class ReproPartitionKeyExpressionDJ {

    @Autowired
    private StreamBridge streamBridge;

    @Test
    void test() {
        streamBridge.send("outputA-out-0", MessageBuilder.withPayload("A").setHeader("partitionKey", "A").build());
        // Exception "Partition key cannot be null" on the line below, even though we did not define a
        // partition-key-expression for outputB it still expects one (the one defined on outputA)
        streamBridge.send("outputB-out-0", MessageBuilder.withPayload("B").build());
        // note that if If you were to invert the two lines (send on outputB first, then outputA), the test would work !
    }

    @SpringBootApplication
    public static class TestApplication {

        public static void main(String[] args) {
            SpringApplication.run(TestApplication.class, args);
        }

    }

}
