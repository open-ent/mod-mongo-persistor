/*
 * Copyright 2011-2012 the original author or authors.
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

package org.vertx.mods;

import com.mongodb.ReadPreference;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.mongo.*;
import org.apache.commons.lang3.StringUtils;
import org.vertx.java.busmods.BusModBase;

import java.util.*;
import java.util.stream.Collectors;

/**
 * MongoDB Persistor Bus Module<p>
 * Please see the README.md for a full description<p>
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author Thomas Risberg
 * @author Richard Warburton
 */
public class MongoPersistor extends BusModBase implements Handler<Message<JsonObject>> {

  protected String address;
  protected String host;
  protected int port;
  protected String username;
  protected String password;
  protected ReadPreference readPreference;

  protected MongoClient mongo;
  private WriteOption writeOption = WriteOption.ACKNOWLEDGED;

  @Override
  public void start() {
    super.start();

    address = getOptionalStringConfig("address", "vertx.mongopersistor");
    this.mongo = MongoClient.createShared(vertx, config);
    eb.consumer(address, this);
  }

  @Override
  public void stop() {
    if (mongo != null) {
      mongo.close();
    }
  }

  @Override
  public void handle(Message<JsonObject> message) {
    String action = message.body().getString("action");

    if (action == null) {
      sendError(message, "action must be specified");
      return;
    }
    try {
      // Note actions should not be in camel case, but should use underscores
      // I have kept the version with camel case so as not to break compatibility

      switch (action) {
        case "save":
          doSave(message);
          break;
        case "update":
          doUpdate(message);
          break;
        case "bulk":
          doBulk(message);
          break;
        case "find":
          doFind(message);
          break;
        case "findone":
          doFindOne(message);
          break;
        // no need for a backwards compatible "findAndModify" since this feature was added after
        case "find_and_modify":
          doFindAndModify(message);
          break;
        case "delete":
          doDelete(message);
          break;
        case "count":
          doCount(message);
          break;
        case "getCollections":
        case "get_collections":
          getCollections(message);
          break;
        case "dropCollection":
        case "drop_collection":
          dropCollection(message);
          break;
        case "collectionStats":
        case "collection_stats":
          getCollectionStats(message);
          break;
        case "aggregate":
          doAggregation(message);
          break;
        case "command":
          runCommand(message);
          break;
        case "distinct":
          doDistinct(message);
          break;
        case "insert":
          doInsert(message);
          break;
        default:
          sendError(message, "Invalid action: " + action);
      }
    } catch (Exception e) {
      sendError(message, e.getMessage(), e);
    }
  }

  private Future<Void> doSave(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.succeededFuture();
    }
    JsonObject doc = getMandatoryObject("document", message);
    if (doc == null) {
      return Future.succeededFuture();
    }
    String genID = generateId(doc);
    return mongo.saveWithOptions(collection, doc, getWriteConcern())
    .onSuccess(result -> {
      if (genID != null) {
        JsonObject reply = new JsonObject().put("_id", genID);
        sendOK(message, reply);
      } else {
        sendOK(message);
      }
    })
    .onFailure(th -> sendError(message, th.getMessage(), th))
    .mapEmpty();
  }

  private @Nullable WriteOption getWriteConcern() {
    return writeOption;
  }
  private @Nullable WriteOption getWriteConcern(final WriteOption defaultWriteConcern) {
    Optional<String> writeConcern = getStringConfig("writeConcern");
    if(!writeConcern.isPresent()) {
      writeConcern = getStringConfig("write_concern");
    }
    return writeConcern.map(WriteOption::valueOf).orElse(defaultWriteConcern);
  }

  private Future<Void> doInsert(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.succeededFuture();
    }
    boolean multipleDocuments = message.body().getBoolean("multiple", false);

    List<BulkOperation> operations = new ArrayList<>();
    final StringBuilder genID = new StringBuilder();
    if (multipleDocuments) {
      JsonArray documents = message.body().getJsonArray("documents");
      if (documents == null) {
        return Future.succeededFuture();
      }
      for (Object o : documents) {
        JsonObject doc = (JsonObject) o;
        generateId(doc);
        operations.add(BulkOperation.createInsert(doc));
      }
    } else {
      JsonObject doc = getMandatoryObject("document", message);
      if (doc == null) {
        return Future.succeededFuture();
      }
      genID.append(generateId(doc));
      operations.add(BulkOperation.createInsert(doc));
    }
    try {
      return mongo.bulkWriteWithOptions(collection, operations, new BulkWriteOptions().setWriteOption(WriteOption.ACKNOWLEDGED))
        .onFailure(th -> sendError(message, th.getMessage(), th))
        .onSuccess(res -> {
          JsonObject reply = new JsonObject();
          reply.put("number", res.getInserts().size());
          if (genID.length() > 0) {
            reply.put("_id", genID.toString());
          }
          sendOK(message, reply);
        })
        .mapEmpty();
    } catch (Exception e){
      sendError(message, e.getMessage());
      return Future.failedFuture(e);
    }
  }

  private String generateId(JsonObject doc) {
    if (doc.getValue("_id") == null) {
      String genID = UUID.randomUUID().toString();
      doc.put("_id", genID);
      return genID;
    }
    return null;
  }

  private Future<Void> doUpdate(Message<JsonObject> message) {
    // mongo collection
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.succeededFuture();
    }

    // query criteria
    JsonObject criteriaJson = getMandatoryObject("criteria", message);
    if (criteriaJson == null) {
      return Future.succeededFuture();
    }

    // query arrayFilters (not mandatory)
    JsonArray arrayFiltersJsonArray = message.body().getJsonArray("arrayFilters");

    // new object for update
    JsonObject objNewJson = getMandatoryObject("objNew", message);
    if (objNewJson == null) {
      return Future.succeededFuture();
    }
    final boolean upsert = message.body().getBoolean("upsert", false);
    final boolean multi = message.body().getBoolean("multi", false);
    final UpdateOptions options = new UpdateOptions()
      .setUpsert(upsert)
      .setWriteOption(getWriteConcern())
      .setMulti(multi);
	if (arrayFiltersJsonArray != null) {
		options.setArrayFilters(arrayFiltersJsonArray);
	}
    return mongo.updateCollectionWithOptions(collection, criteriaJson, objNewJson, options)
      .onSuccess(res -> {
        JsonObject reply = new JsonObject();
        reply.put("number", res.getDocModified());
        sendOK(message, reply);
      })
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .mapEmpty();
  }

  private Future<Void> doBulk(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.succeededFuture();
    }
    JsonArray commands = message.body().getJsonArray("commands");
    if (commands == null || commands.isEmpty()) {
      sendError(message, "Missing commands");
      return Future.failedFuture("mongodb.missing.commands");
    }
    final List<BulkOperation> bulk = new ArrayList<>();
    for (Object o: commands) {
      if (!(o instanceof JsonObject)) continue;
      JsonObject command = (JsonObject) o;
      JsonObject d = command.getJsonObject("document");
      JsonObject c = command.getJsonObject("criteria");
      switch (command.getString("operation", "")) {
        case "insert" :
          if (d != null) {
            bulk.add(BulkOperation.createInsert(d));
          }
          break;
        case "update" :
          if (d != null && c != null) {
            bulk.add(BulkOperation.createUpdate(c, d));
          }
          break;
        case "updateOne":
          if (d != null) {
            bulk.add(BulkOperation.createUpdate(c, d, false, false));
          }
          break;
        case "upsert" :
          if (d != null) {
            bulk.add(BulkOperation.createUpdate(c, d, true, true));
          }
          break;
        case "upsertOne":
          if (d != null && c != null) {
            bulk.add(BulkOperation.createUpdate(c, d, true, false));
          }
          break;
        case "remove":
          if (c != null) {
            bulk.add(BulkOperation.createDelete(c));
          }
          break;
        case "removeOne":
          if (c != null) {
            bulk.add(BulkOperation.createDelete(c).setMulti(false));
          }
          break;
      }
    }
    return mongo.bulkWriteWithOptions(collection, bulk, new BulkWriteOptions().setWriteOption(getWriteConcern()))
      .onSuccess(res -> sendOK(message, new JsonObject()
        .put("inserted", res.getInsertedCount())
        .put("matched", res.getMatchedCount())
        .put("modified", res.getModifiedCount())
        .put("removed", res.getDeletedCount())
      ))
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .mapEmpty();
  }

  private Future<Void> doFind(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.succeededFuture();
    }
    Integer limit = message.body().getInteger("limit");
    if (limit == null) {
      limit = -1;
    }
    Integer skip = message.body().getInteger("skip");
    if (skip == null) {
      skip = -1;
    }
    Integer batchSize = message.body().getInteger("batch_size");
    if (batchSize == null) {
      batchSize = 100;
    }
    Integer timeout = message.body().getInteger("timeout");
    if (timeout == null || timeout < 0) {
      timeout = 10000; // 10 seconds
    }
    JsonObject matcher = message.body().getJsonObject("matcher");
    JsonObject keys = message.body().getJsonObject("keys");

    Object hint = message.body().getValue("hint");
    Object sort = message.body().getValue("sort");
    // call find
    final FindOptions options = new FindOptions();
    if (matcher != null && keys != null) {
      options.setFields(keys);
    }
    if (skip != -1) {
      options.setSkip(skip);
    }
    if (limit != -1) {
      options.setLimit(limit);
    }
    if (sort != null) {
      options.setSort((JsonObject) sort);
    }
    if (hint != null) {
      if (hint instanceof JsonObject) {
        options.setHint((JsonObject) hint);
      } else if (hint instanceof String) {
        options.setHintString((String) hint);
      } else {
        return Future.failedFuture("Cannot handle type " + hint.getClass().getSimpleName());
      }
    }

    return mongo.findWithOptions(collection, matcher == null ? new JsonObject() : matcher, options)
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .onSuccess(results -> message.reply(createBatchMessage("ok", results)))
      .mapEmpty();
  }

  private JsonObject createBatchMessage(String status, final List<?> results) {
    JsonObject reply = new JsonObject();
    reply.put("results", results);
    reply.put("status", status);
    reply.put("number", results.size());
    return reply;
  }

  private Future<Void> doFindOne(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    JsonObject matcher = message.body().getJsonObject("matcher");
    JsonObject keys = message.body().getJsonObject("keys");
    return mongo.findOne(collection, matcher == null ? new JsonObject() : matcher, keys)
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .compose(res -> {
        final Promise<JsonObject> replyPromise = Promise.promise();
        JsonObject reply = new JsonObject();
        if (res == null) {
          replyPromise.complete(reply);
        } else {
          reply.put("result", res);
          JsonArray fetch = message.body().getJsonArray("fetch");
          if (fetch == null) {
            replyPromise.complete(reply);
          } else {
            List<Future<?>> fetches = fetch.stream().map(attr -> {
              if (attr instanceof String) {
                String f = (String) attr;
                JsonObject tmp = res.getJsonObject(f);
                final Future<Void> onDbRefFetched;
                if (tmp == null || !(tmp.containsKey("$ref"))) {
                  onDbRefFetched = Future.succeededFuture();
                } else {
                  onDbRefFetched = fetchRef(tmp)
                    .onSuccess(fetched -> res.put(f, fetched))
                    .mapEmpty();
                }
                return onDbRefFetched;
              } else {
                return Future.succeededFuture(null);
              }
            }).collect(Collectors.toList());
            reply.put("result", res);
            Future.join(fetches).onSuccess(e -> replyPromise.complete(reply)).onFailure(replyPromise::fail);
          }
        }
        return replyPromise.future();
      })
      .onSuccess(reply -> sendOK(message, reply))
      .mapEmpty();
  }

  private Future<JsonObject> fetchRef(final JsonObject ref){
    final JsonObject query = new JsonObject().put("_id", ref.getString("$id"));
    return mongo.findOne(ref.getString("$ref"), query, null);
  }

  private Future<Void> doFindAndModify(Message<JsonObject> message) {
    String collectionName = getMandatoryString("collection", message);
    if (collectionName == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    final JsonObject msgBody = message.body();
    final JsonObject update = msgBody.getJsonObject("update");
    final JsonObject query = msgBody.getJsonObject("matcher");
    final JsonObject sort = msgBody.getJsonObject("sort");
    final JsonObject fields = msgBody.getJsonObject("fields");
    final FindOptions findOptions = new FindOptions().setSort(sort).setFields(fields);
    boolean returnNew = msgBody.getBoolean("new", false);
    boolean upsert = msgBody.getBoolean("upsert", false);
    final UpdateOptions updateOptions = new UpdateOptions()
      .setReturningNewDocument(returnNew)
      .setUpsert(upsert);
    boolean remove = msgBody.getBoolean("remove", false);

    if (remove) {
      return mongo.findOneAndDeleteWithOptions(collectionName, query, findOptions)
              .onFailure(th -> sendError(message, th.getMessage(), th))
              .onSuccess(res -> {
                final JsonObject reply = new JsonObject();
                if (res != null) {
                  reply.put("result", res);
                }
                sendOK(message, reply);
              })
              .mapEmpty();
    } else {
      return mongo.findOneAndUpdateWithOptions(collectionName, query, update, findOptions, updateOptions)
              .onFailure(th -> sendError(message, th.getMessage(), th))
              .onSuccess(res -> {
                final JsonObject reply = new JsonObject();
                if (res != null) {
                  reply.put("result", res);
                }
                sendOK(message, reply);
              })
              .mapEmpty();
    }
  }

  private Future<Void> doCount(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    final JsonObject matcher = message.body().getJsonObject("matcher");

    // call find
    return mongo.count(collection, matcher == null ? new JsonObject() : matcher)
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .onSuccess(count -> {
        JsonObject reply = new JsonObject();
        reply.put("count", count);
        sendOK(message, reply);
      })
      .mapEmpty();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Future<Void> doDistinct(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    final String key = getMandatoryString("key", message);
    if (key == null) {
      return Future.failedFuture("collection.key.mandatory");
    }
    final JsonObject matcher = message.body().getJsonObject("matcher");
    String className = message.body().getString("resultClassname");
    if(StringUtils.isBlank(className)) {
      className = String.class.getName();
    }
    final Future<JsonArray> future;
    if(matcher == null) {
      future = mongo.distinct(collection, key, className);
    } else {
      future = mongo.distinctWithQuery(collection, key, className, matcher);
    }
    return future.
      onFailure(th -> sendError(message, th.getMessage(), th))
      .onSuccess(values -> {
        JsonObject reply = new JsonObject().put("values", values);
        sendOK(message, reply);
      })
      .mapEmpty();
  }

  private Future<Void> doDelete(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    JsonObject matcher = getMandatoryObject("matcher", message);
    if (matcher == null) {
      return Future.failedFuture("matcher.mandatory");
    }
    return mongo.removeDocumentsWithOptions(collection, matcher, getWriteConcern())
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .onSuccess(res -> {
        JsonObject reply = new JsonObject().put("number", res.getRemovedCount());
        sendOK(message, reply);
      })
      .mapEmpty();
  }

  private Future<Void> getCollections(Message<JsonObject> message) {
    return mongo.getCollections()
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .onSuccess(colls -> {
        final JsonObject reply = new JsonObject().put("collections", new JsonArray(colls));
        sendOK(message, reply);
      })
      .mapEmpty();
  }

  private Future<Void> dropCollection(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    return mongo.dropCollection(collection)
      .onFailure(th -> sendError(message, "exception thrown when attempting to drop collection: " + collection + "\n" + th.getMessage(), th))
      .onSuccess(e -> sendOK(message, new JsonObject()));
  }

  private Future<Void> getCollectionStats(Message<JsonObject> message) {
    final String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    return mongo.runCommand("collStats", new JsonObject().put("collStats", collection))
      .onFailure(th -> sendError(message, th))
      .onSuccess(stats -> {
        final JsonObject reply = new JsonObject().put("stats", stats);
        sendOK(message, reply);
      })
      .mapEmpty();
  }

  private Future<Void> doAggregation(Message<JsonObject> message) {
    if (isCollectionMissing(message)) {
      return Future.failedFuture("collection.name.mandatory");
    }
    if (isPipelinesMissing(message.body().getJsonArray("pipelines"))) {
      sendError(message, "no pipeline operations found");
      return Future.failedFuture("pipelines.mandatory");
    }
    final String collection = message.body().getString("collection");
    final JsonArray pipelines = message.body().getJsonArray("pipelines");
    final ReadStream<JsonObject> stream = mongo.aggregate(collection, pipelines);
    final Promise<Void> promise = Promise.promise();
    final JsonArray results = new JsonArray();
    stream.endHandler(e -> {
      JsonObject reply = new JsonObject();
      reply.put("results", results);
      sendOK(message, reply);
      promise.complete();
    })
    .handler(results::add)
    .exceptionHandler(th -> {
      sendError(message, th);
      mongo.close();
    });
    return promise.future();
  }

  private boolean isCollectionMissing(Message<JsonObject> message) {
    return getMandatoryString("collection", message) == null;
  }

  private boolean isPipelinesMissing(JsonArray pipelines) {
    return pipelines == null || pipelines.size() == 0;
  }

  private Future<Void> runCommand(Message<JsonObject> message) {
    JsonObject reply = new JsonObject();
    final String commandRaw = getMandatoryString("command", message);
    if (commandRaw == null) {
      return Future.failedFuture("command.mandatory");
    }
    try {
      final JsonObject command = new JsonObject(commandRaw);
      return getCommandName(command).map(commandName -> mongo.runCommand(commandName, command)
          .onFailure(th -> sendError(message, th))
          .onSuccess(result -> {
            result.remove("operationTime");
            result.remove("$clusterTime");
            result.remove("opTime");
            result.remove("electionId");
            reply.put("result", result);
            sendOK(message, reply);
          })).orElseGet(() -> Future.failedFuture("command.name.mandatory"))
        .mapEmpty();
    } catch (Exception th) {
      sendError(message, th);
      return Future.failedFuture(th);
    }
  }

  private Optional<String> getCommandName(final JsonObject command) {
    return command.fieldNames().stream().findFirst();
  }

}

