//package io.vertx.mods.mongo.test.integration.java;
///*
// * Copyright 2013 Red Hat, Inc.
// *
// * Red Hat licenses this file to you under the Apache License, version 2.0
// * (the "License"); you may not use this file except in compliance with the
// * License.  You may obtain a copy of the License at:
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// * License for the specific language governing permissions and limitations
// * under the License.
// *
// * @author <a href="http://tfox.org">Tim Fox</a>
// */
//
//import org.junit.Test;
//import io.vertx.core.Handler;
//import io.vertx.core.eventbus.Message;
//import io.vertx.core.json.JsonArray;
//import io.vertx.core.json.JsonObject;
//
//import java.util.concurrent.atomic.AtomicInteger;
//
//import static io.vertx.testtools.VertxAssert.assertEquals;
//import static io.vertx.testtools.VertxAssert.testComplete;
//
//public class PersistorTest extends PersistorTestParent {
//
//  @Test
//  public void testPersistor() throws Exception {
//    deleteAll(new Handler<Message<JsonObject>>() {
//      public void handle(Message<JsonObject> reply) {
//        assertEquals("ok", reply.body().getString("status"));
//        final int numDocs = 1;
//        final AtomicInteger count = new AtomicInteger(0);
//        for (int i = 0; i < numDocs; i++) {
//          JsonObject doc = new JsonObject()
//                  .put("name", "joe bloggs")
//                  .put("age", 40)
//                  .put("cat-name", "watt");
//
//          JsonObject json = new JsonObject()
//                  .put("collection", COLLECTION)
//                  .put("action", "save")
//                  .put("document", doc);
//
//          eb.send(ADDRESS, json, new Handler<Message<JsonObject>>() {
//            public void handle(Message<JsonObject> reply) {
//              assertEquals("ok", reply.body().getString("status"));
//              if (count.incrementAndGet() == numDocs) {
//                JsonObject matcher = new JsonObject().put("name", "joe bloggs");
//
//                JsonObject json = new JsonObject()
//                        .put("collection", COLLECTION)
//                        .put("action", "find")
//                        .put("matcher", matcher);
//
//                eb.send(ADDRESS, json, new Handler<Message<JsonObject>>() {
//                  public void handle(Message<JsonObject> reply) {
//                    assertEquals("ok", reply.body().getString("status"));
//                    JsonArray results = reply.body().getJsonArray("results");
//                    assertEquals(numDocs, results.size());
//                    testComplete();
//                  }
//                });
//              }
//            }
//          });
//        }
//      }
//    });
//  }
//
//  @Test
//  public void testCommand() throws Exception {
//    JsonObject ping = new JsonObject()
//            .put("action", "command")
//            .put("command", "{ping:1}");
//
//    eb.send(ADDRESS, ping, new Handler<Message<JsonObject>>() {
//      public void handle(Message<JsonObject> reply) {
//        Number ok = reply.body()
//                .getJsonObject("result")
//                .getNumber("ok");
//
//        assertEquals(1.0, ok);
//        testComplete();
//      }
//    });
//  }
//
//}
//
