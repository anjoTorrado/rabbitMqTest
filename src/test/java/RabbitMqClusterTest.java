import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
public class RabbitMqClusterTest {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqClusterTest.class);
    private static final Network network = Network.newNetwork();

    // Testcontainers demands that the image name begins with 'rabbitmq:', so we tag the image accordingly.
    // docker pull pivotalrabbitmq/rabbitmq:main-otp-max-bazel
    // docker tag pivotalrabbitmq/rabbitmq:main-otp-max-bazel rabbitmq:main-otp-max-bazel
    private static final String RABBITMQ_IMAGE_MAIN = "rabbitmq:main-otp-max-bazel";
    private static final String RABBITMQ_IMAGE_PROD = "rabbitmq:3.13.0-rc.2";

    public static RabbitMQContainer rabbitMq1 = prepareRabbitMqContainer("rabbitmq1").withExposedPorts(5672);
    public static RabbitMQContainer rabbitMq2 = prepareRabbitMqContainer("rabbitmq2");
    public static RabbitMQContainer rabbitMq3 = prepareRabbitMqContainer("rabbitmq3");

    @BeforeAll
    static void setup() {
        logger.info("starting containers");
        rabbitMq1.start();
        logger.info("rabbitmq1 started");
        rabbitMq2.start();
        logger.info("rabbitmq2 started");
        rabbitMq3.start();
        logger.info("rabbitmq3 started");
    }

    @Test
    void test() throws Exception {
        setupRabbitmqCluster();
        logger.info("Rabbitmq available on port " + rabbitMq1.getAmqpPort());
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitMq1.getHost());
        factory.setPort(rabbitMq1.getAmqpPort());
        killNode2();
        logger.info("Cluster status after node2 has been killed:");
        try (Connection connection = factory.newConnection()) {
            var channelCreationStart = Instant.now();
            try (Channel channel = connection.createChannel()) {
                assertThat(Duration.between(channelCreationStart, Instant.now()))
                  .describedAs("Channel creation took more than 10 seconds")
                  .isLessThan(Duration.ofSeconds(10));

                var queueDeclarationDurations = new ArrayList<Duration>();

                range(0, 1000).forEach(i -> {
                    logger.info("Declaring queue #{}", i);
                    var queueDeclarationStart = Instant.now();
                    try {
                        channel.queueDeclare().getQueue();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    var duration = Duration.between(queueDeclarationStart, Instant.now());
                    queueDeclarationDurations.add(duration);
                });

                var durationPartition = queueDeclarationDurations.stream()
                  .collect(partitioningBy(duration -> duration.compareTo(Duration.ofSeconds(1)) > 0));

                var slowQueueDeclarations = durationPartition.get(true);
                logger.info("##### > 1 second");
                logger.info("Amount of queues: {}", slowQueueDeclarations.size());
                logger.info("Slowest queue declaration took {} ms.",
                            slowQueueDeclarations.stream()
                              .max(Comparator.naturalOrder())
                              .orElse(Duration.ZERO)
                              .toMillis());
                logger.info("Fastest queue declaration took {} ms.",
                            slowQueueDeclarations.stream()
                              .min(Comparator.naturalOrder())
                              .orElse(Duration.ZERO)
                              .toMillis());
                logger.info("Average queue declaration took {} ms.",
                            slowQueueDeclarations.stream().mapToDouble(Duration::toMillis).average().orElse(0));

                logger.info("----------------------------------------------------------------");

                var fastQueueDeclarations = durationPartition.get(false);
                logger.info("##### <= 1 second");
                logger.info("Amount of queues: {}", fastQueueDeclarations.size());
                logger.info("Slowest queue declaration took {} ms.",
                            fastQueueDeclarations.stream()
                              .max(Comparator.naturalOrder())
                              .orElse(Duration.ZERO)
                              .toMillis());
                logger.info("Fastest queue declaration took {} ms.",
                            fastQueueDeclarations.stream()
                              .min(Comparator.naturalOrder())
                              .orElse(Duration.ZERO)
                              .toMillis());
                logger.info("Average queue declaration took {} ms.",
                            fastQueueDeclarations.stream().mapToDouble(Duration::toMillis).average().orElse(0));
            }
        }
    }

    private void setupRabbitmqCluster() throws Exception {
        logger.info("Setting up cluster");
        rabbitMq2.execInContainer("rabbitmqctl", "stop_app");
        rabbitMq2.execInContainer(
          "rabbitmqctl",
          "join_cluster",
          "rabbit@" + rabbitMq1.getContainerId().substring(0, 12)
        );
        rabbitMq2.execInContainer("rabbitmqctl", "start_app");
        rabbitMq3.execInContainer("rabbitmqctl", "stop_app");
        rabbitMq3.execInContainer(
          "rabbitmqctl",
          "join_cluster",
          "rabbit@" + rabbitMq1.getContainerId().substring(0, 12)
        );
        rabbitMq3.execInContainer("rabbitmqctl", "start_app");
    }


    private static String killNode2() throws InterruptedException {
        String nodeToKill = rabbitMq2.getContainerId().substring(0, 12);
        logger.info("Killing: " + nodeToKill);
        rabbitMq2.stop();
        sleep(5);
        logger.info("Killed " + nodeToKill);
        return nodeToKill;
    }

    private static void sleep(int seconds) throws InterruptedException {
        logger.info("Sleeping for " + seconds + "s");
        SECONDS.sleep(seconds);
    }

    private static RabbitMQContainer getRabbitMQContainer() {
        return new RabbitMQContainer(RABBITMQ_IMAGE_MAIN) {
            @Override
            protected void configure() {}
        };
    }

    private static RabbitMQContainer prepareRabbitMqContainer(String containerName) {
        final MountableFile RABBIT_MQ_CONF = MountableFile.forClasspathResource("rabbitmq.conf");
        final Map<String, String> RABBITMQ_ENV = Map.of("RABBITMQ_ERLANG_COOKIE", "WJRMLDGLUGUJDOISLMDP");

        return getRabbitMQContainer()
          .withPrivilegedMode(true)
          .withEnv(RABBITMQ_ENV)
          .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(containerName)))
          .withRabbitMQConfig(RABBIT_MQ_CONF)
          .withNetwork(network);
    }
}
