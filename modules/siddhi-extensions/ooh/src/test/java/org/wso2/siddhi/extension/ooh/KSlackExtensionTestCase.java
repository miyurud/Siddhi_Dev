package org.wso2.siddhi.extension.ooh;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

/**
 * Created by miyurud on 8/10/15.
 */
public class KSlackExtensionTestCase {
    static final Logger log = Logger.getLogger(KSlackExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void OrderTest() throws InterruptedException {
        log.info("KSlackExtensionTestCase TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true')define stream inputStream (eventtt long, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream#window.ooh:kslack() select eventtt , price, volume " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);

                for (org.wso2.siddhi.core.event.Event evt : events) {
                    Object[] dt = evt.getData();
                    System.out.println(dt[0]);
                    //log.info(dt[0]);
                }
            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{0l, 700f, 100l});
        inputHandler.send(new Object[]{1l, 60.5f, 200l});
        inputHandler.send(new Object[]{2l, 60.5f, 200l});
        inputHandler.send(new Object[]{3l, 700f, 100l});
        inputHandler.send(new Object[]{5l, 60.5f, 200l});
        inputHandler.send(new Object[]{4l, 60.5f, 200l});
        inputHandler.send(new Object[]{6l, 700f, 100l});
        inputHandler.send(new Object[]{7l, 60.5f, 200l});
        inputHandler.send(new Object[]{8l, 60.5f, 200l});

//        inputHandler.send(new Object[]{1l, 700f, 100l});
//        inputHandler.send(new Object[]{4l, 60.5f, 200l});
//        inputHandler.send(new Object[]{3l, 60.5f, 200l});
//        inputHandler.send(new Object[]{5l, 700f, 100l});
//        inputHandler.send(new Object[]{6l, 60.5f, 200l});
//        inputHandler.send(new Object[]{9l, 60.5f, 200l});
//        inputHandler.send(new Object[]{7l, 700f, 100l});
//        inputHandler.send(new Object[]{8l, 60.5f, 200l});
//        inputHandler.send(new Object[]{10l, 60.5f, 200l});
//        inputHandler.send(new Object[]{13l, 60.5f, 200l});

        Thread.sleep(10000);
        executionPlanRuntime.shutdown();
    }
}