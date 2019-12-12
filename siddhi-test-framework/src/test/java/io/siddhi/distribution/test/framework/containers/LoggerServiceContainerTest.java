package io.siddhi.distribution.test.framework.containers;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.distribution.test.framework.LoggerServiceContainer;
import io.siddhi.distribution.test.framework.NatsContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LoggerServiceContainerTest {
    private static final Logger log = LoggerFactory.getLogger(LoggerServiceContainerTest.class);
    private WaitingConsumer consumer = new WaitingConsumer();


    @Test
    public void testLoggerServiceStartup() throws Exception {
        LoggerServiceContainer loggerServiceContainer = new LoggerServiceContainer()
                .withLogConsumer(new Slf4jLogConsumer(log));
        loggerServiceContainer.start();

        loggerServiceContainer.followOutput(consumer, OutputFrame.OutputType.STDOUT);
        Thread.sleep(3000);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('API-Request-Throttler')\n" +
                        "@App:description('Enforces throttling on API requests')\n" +
                        "\n" +
                        "@sink(type = 'http', publisher.url = \"" +
                        loggerServiceContainer.getUrl()+
                        "\", method = 'POST'," +
                        "@map(type='json'))\n" +
                        "define stream ThrottleOutputStream (apiName string, version string, user string, " +
                        "tier string, userEmail string);");
        siddhiAppRuntime.start();
        Thread.sleep(5000);
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("ThrottleOutputStream");
        inputHandler.send(new Object[]{"aa","aa","aa","aa","aa"});
        //Thread.sleep(10000);

//        try {
//            consumer.waitUntil(frame ->
//                            frame.getUtf8String().contains("{event={apiName=aa, version=aa, user=aa, tier=aa, userEmail=aa}}"),
//                    50, TimeUnit.SECONDS);
//        } catch (TimeoutException e) {
//            Assert.fail("Assert fail");
//        } finally {
//            siddhiManager.shutdown();
//        }
    }
}
