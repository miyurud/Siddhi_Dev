/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.siddhi.extension.ooh;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;


public class KSlackExtension extends WindowProcessor {
    private long k = 0; //In the beginning the K is zero.
    private long t_curr = 0; //Used to track the greatest timestamp of tuples seen so far in the stream history.
    private long t_last = 0;
    private long delay = 0;
    private ArrayList<StreamEvent> expiredEventBuffer;
    private ArrayList<StreamEvent> eventBuffer;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        expiredEventBuffer = new ArrayList<StreamEvent>();
        eventBuffer = new ArrayList<StreamEvent>();
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>();

        while (streamEventChunk.hasNext()) {
            StreamEvent event = streamEventChunk.next();
            streamEventChunk.remove(); //We might have the rest of the events linked to this event forming a chain.
                                       //To break such occurrences we call remove()
            long ts = (Long) event.getOutputData()[0];

            //The variable t_curr is used to track the greatest timestamp of tuples seen so far in the stream history.
            if (ts > t_curr) {
                eventBuffer.add(event);
                t_curr = ts;
                long minTs = Long.MAX_VALUE;

                for (StreamEvent evt : eventBuffer) {
                    ts = (Long) evt.getOutputData()[0];

                    if (ts < minTs) {
                        minTs = ts;
                    }
                }

                k = (t_curr - minTs) > k ? (t_curr - minTs) : k;

                ArrayList<StreamEvent> buff = new ArrayList<StreamEvent>();
                buff.addAll(eventBuffer);
                buff.addAll(expiredEventBuffer);
                expiredEventBuffer = new ArrayList<StreamEvent>();
                TreeMap<Long, StreamEvent> treeMapOutput = new TreeMap<Long, StreamEvent>();

                for (StreamEvent evt : buff) {
                    ts = (Long) evt.getOutputData()[0];

                    if (ts + k <= t_curr) {
                        treeMapOutput.put(ts, evt);
                    } else {
                        expiredEventBuffer.add(evt);
                    }
                    eventBuffer.remove(evt);
                }
                //At this point the size of the eventBuffer should be zero.
                Iterator<StreamEvent> itr = treeMapOutput.values().iterator();
                while(itr.hasNext()){
                    StreamEvent e = itr.next();
                    complexEventChunk.add(e);
                }
            } else {
                eventBuffer.add(event);
            }
        }

        nextProcessor.process(complexEventChunk);
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {
        //Do nothing
    }
}
