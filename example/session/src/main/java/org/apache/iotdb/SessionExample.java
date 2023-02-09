/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.SessionDataSet.DataIterator;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("squid:S106")
public class SessionExample {

  public static void main(String[] args) throws InterruptedException {
    int threadCount = Integer.parseInt(args[0]);
    String[] ipList = args[1].split(",");
    String sql = args[2];

    AtomicLong totalCost = new AtomicLong(0);
    AtomicLong totalCount = new AtomicLong(0);
    CountDownLatch countDownLatch = new CountDownLatch(threadCount);
    for (int i = 0; i < threadCount; i++) {
      int index = i;
      new Thread(
              () -> {
                Session session = null;
                try {
                  session =
                      new Session.Builder()
                          .host(ipList[index % ipList.length])
                          .port(6667)
                          .username("root")
                          .password("root")
                          .version(Version.V_1_0)
                          .build();

                  session.open(false);

                  // set session fetchSize
                  session.setFetchSize(10000);

                  // warm up twice
                  for (int j = 0; j < 2; j++) {
                    try (SessionDataSet dataSet = session.executeQueryStatement(sql, 3_600_000)) {
                      DataIterator iterator = dataSet.iterator();
                      while (iterator.next()) {}
                    } catch (Throwable t) {
                      // ignore
                    }
                  }

                  // final Test
                  long startNanoTime = System.nanoTime();
                  try (SessionDataSet dataSet = session.executeQueryStatement(sql, 3_600_000)) {
                    DataIterator iterator = dataSet.iterator();
                    while (iterator.next()) {}

                  } catch (Throwable t) {
                    // ignore
                  } finally {
                    totalCount.incrementAndGet();
                    totalCost.addAndGet(System.nanoTime() - startNanoTime);
                  }

                } catch (IoTDBConnectionException e) {
                  throw new RuntimeException(e);
                } finally {
                  if (session != null) {
                    try {
                      session.close();
                    } catch (IoTDBConnectionException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  countDownLatch.countDown();
                }
              })
          .start();
    }

    boolean result = countDownLatch.await(6, TimeUnit.MINUTES);
    long cost = totalCost.get();
    long count = totalCount.get();
    if (count != 0) {
      System.out.println("avg cost is: " + cost / count / 1_000_000 + "ms");
    } else {
      System.out.println("timeout!!!!");
    }
  }
}
