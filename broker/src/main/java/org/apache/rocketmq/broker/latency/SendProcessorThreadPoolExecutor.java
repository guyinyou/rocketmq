/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker.latency;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;

public class SendProcessorThreadPoolExecutor extends BrokerFixedThreadPoolExecutor {

    private BrokerFixedThreadPoolExecutor[] executors;
    private int partition;
    private int threshold;

    public SendProcessorThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
        TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        partition = corePoolSize;
        executors = new BrokerFixedThreadPoolExecutor[partition];
        int queueCap = workQueue.remainingCapacity();
        for (int i = 0; i < executors.length; i++) {
            executors[i] = new BrokerFixedThreadPoolExecutor(1, 1, keepAliveTime, unit, new LinkedBlockingQueue<>(queueCap), threadFactory);
        }
        threshold = queueCap / partition;
    }

    @Override
    public Future<?> submit(Runnable task) {
        RemotingCommand cmd = ((RequestTask) task).getRequest();
        int hash = ((SendMessageRequestHeader) cmd.getCustomHeaderInRequest()).getQueueId();
        int idx = hash % executors.length;
        ThreadPoolExecutor executor = executors[idx];
        if (executor.getQueue().size() > threshold) {
            return super.submit(task);
        }
        return executor.submit(task);
    }
}
