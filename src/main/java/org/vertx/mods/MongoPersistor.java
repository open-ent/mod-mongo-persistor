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

import com.mongodb.*;
import com.mongodb.util.JSON;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.vertx.java.busmods.BusModBase;

import javax.net.ssl.SSLSocketFactory;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
  protected String dbName;
  protected String dbAuth;
  protected String username;
  protected String password;
  protected ReadPreference readPreference;
  protected int socketTimeout;
  protected boolean useSSL;

  protected Mongo mongo;
  protected DB db;
  private boolean useMongoTypes;

  @Override
  public void start() {
    super.start();

    address = getOptionalStringConfig("address", "vertx.mongopersistor");

    host = getOptionalStringConfig("host", "localhost");
    port = getOptionalIntConfig("port", 27017);
    dbName = getOptionalStringConfig("db_name", "default_db");
    dbAuth = getOptionalStringConfig("db_auth", "default_db");
    username = getOptionalStringConfig("username", null);
    password = getOptionalStringConfig("password", null);
    readPreference = ReadPreference.valueOf(getOptionalStringConfig("read_preference", "primary"));
    int poolSize = getOptionalIntConfig("pool_size", 10);
    socketTimeout = getOptionalIntConfig("socket_timeout", 60000);
    useSSL = getOptionalBooleanConfig("use_ssl", false);
    useMongoTypes = getOptionalBooleanConfig("use_mongo_types", false);

    JsonArray seedsProperty = config.getJsonArray("seeds");

    try {
      MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
      builder.connectionsPerHost(poolSize);
      builder.socketTimeout(socketTimeout);
      builder.readPreference(readPreference);

      if (useSSL) {
        builder.socketFactory(SSLSocketFactory.getDefault());
      }

      final List<MongoCredential> credentials = new ArrayList<>();
      if (username != null && password != null && dbAuth != null) {
        credentials.add(MongoCredential.createScramSha1Credential(username, dbAuth, password.toCharArray()));
      }

      if (seedsProperty == null) {
        ServerAddress address = new ServerAddress(host, port);
        mongo = new MongoClient(address, credentials, builder.build());
      } else {
        List<ServerAddress> seeds = makeSeeds(seedsProperty);
        mongo = new MongoClient(seeds, credentials, builder.build());
      }

      db = mongo.getDB(dbName);
    } catch (UnknownHostException e) {
      logger.error("Failed to connect to mongo server", e);
    }
    eb.consumer(address, this);
  }

  private List<ServerAddress> makeSeeds(JsonArray seedsProperty) throws UnknownHostException {
    List<ServerAddress> seeds = new ArrayList<>();
    for (Object elem : seedsProperty) {
      JsonObject address = (JsonObject) elem;
      String host = address.getString("host");
      int port = address.getInteger("port");
      seeds.add(new ServerAddress(host, port));
    }
    return seeds;
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
    } catch (MongoException e) {
      sendError(message, e.getMessage(), e);
    }
  }

  private void doSave(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    JsonObject doc = getMandatoryObject("document", message);
    if (doc == null) {
      return;
    }
    String genID = generateId(doc);
    DBCollection coll = db.getCollection(collection);
    DBObject obj = jsonToDBObject(doc);
    WriteConcern writeConcern = WriteConcern.valueOf(getOptionalStringConfig("writeConcern", ""));
    // Backwards compatibility
    if (writeConcern == null) {
      writeConcern = WriteConcern.valueOf(getOptionalStringConfig("write_concern", ""));
    }
    if (writeConcern == null) {
      writeConcern = db.getWriteConcern();
    }
    try {
      WriteResult res = coll.save(obj, writeConcern);
      if (genID != null) {
        JsonObject reply = new JsonObject();
        reply.put("_id", genID);
        sendOK(message, reply);
      } else {
        sendOK(message);
      }
    } catch (Exception e){
      sendError(message, e.getMessage());
    }
  }

  private void doInsert(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    boolean multipleDocuments = message.body().getBoolean("multiple", false);

    List<DBObject> dbos = new ArrayList<>();
    String genID = null;
    if (multipleDocuments) {
      JsonArray documents = message.body().getJsonArray("documents");
      if (documents == null) {
        return;
      }
      for (Object o : documents) {
        JsonObject doc = (JsonObject) o;
        generateId(doc);
        dbos.add(jsonToDBObject(doc));
      }
    } else {
      JsonObject doc = getMandatoryObject("document", message);
      if (doc == null) {
        return;
      }
      genID = generateId(doc);
      dbos.add(jsonToDBObject(doc));
    }

    DBCollection coll = db.getCollection(collection);
    WriteConcern writeConcern = WriteConcern.valueOf(getOptionalStringConfig("write_concern",""));
    if (writeConcern == null) {
      writeConcern = db.getWriteConcern();
    }
    writeConcern = WriteConcern.SAFE;
    try {
      WriteResult res = coll.insert(dbos, writeConcern);
      JsonObject reply = new JsonObject();
      reply.put("number", res.getN());
      if (genID != null) {
        reply.put("_id", genID);
      }
      sendOK(message, reply);
    } catch (Exception e){
      sendError(message, e.getMessage());
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

  private void doUpdate(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    JsonObject criteriaJson = getMandatoryObject("criteria", message);
    if (criteriaJson == null) {
      return;
    }
    DBObject criteria = jsonToDBObject(criteriaJson);

    JsonObject objNewJson = getMandatoryObject("objNew", message);
    if (objNewJson == null) {
      return;
    }
    DBObject objNew = jsonToDBObject(objNewJson);
    Boolean upsert = message.body().getBoolean("upsert", false);
    Boolean multi = message.body().getBoolean("multi", false);
    DBCollection coll = db.getCollection(collection);
    WriteConcern writeConcern = WriteConcern.valueOf(getOptionalStringConfig("writeConcern", ""));
    // Backwards compatibility
    if (writeConcern == null) {
      writeConcern = WriteConcern.valueOf(getOptionalStringConfig("write_concern", ""));
    }

    if (writeConcern == null) {
      writeConcern = db.getWriteConcern();
    }
    try {
      WriteResult res = coll.update(criteria, objNew, upsert, multi, writeConcern);
      JsonObject reply = new JsonObject();
      reply.put("number", res.getN());
      sendOK(message, reply);
    } catch (Exception e){
      sendError(message, e.getMessage());
    }
  }

  private void doBulk(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    JsonArray commands = message.body().getJsonArray("commands");
    if (commands == null || commands.size() < 1) {
      sendError(message, "Missing commands");
      return;
    }
    final String writeConcern  = message.body().getString("write_concern");
    DBCollection coll = db.getCollection(collection);
    BulkWriteOperation bulk = coll.initializeUnorderedBulkOperation();
    for (Object o: commands) {
      if (!(o instanceof JsonObject)) continue;
      JsonObject command = (JsonObject) o;
      JsonObject d = command.getJsonObject("document");
      JsonObject c = command.getJsonObject("criteria");
      switch (command.getString("operation", "")) {
        case "insert" :
          if (d != null) {
            bulk.insert(jsonToDBObject(d));
          }
          break;
        case "update" :
          if (d != null && c != null) {
            bulk.find(jsonToDBObject(c)).update(jsonToDBObject(d));
          }
          break;
        case "updateOne":
          if (d != null) {
            bulk.find(jsonToDBObject(c)).updateOne(jsonToDBObject(d));
          }
          break;
        case "upsert" :
          if (d != null) {
            bulk.find(jsonToDBObject(c)).upsert().updateOne(jsonToDBObject(d));
          }
          break;
        case "upsertOne":
          if (d != null && c != null) {
            bulk.find(jsonToDBObject(c)).upsert().updateOne(jsonToDBObject(d));
          }
          break;
        case "remove":
          if (c != null) {
            bulk.find(jsonToDBObject(c)).remove();
          }
          break;
        case "removeOne":
          if (c != null) {
            bulk.find(jsonToDBObject(c)).removeOne();
          }
          break;
      }
    }
    final BulkWriteResult r;
    if (writeConcern != null && !writeConcern.isEmpty()) {
      r = bulk.execute(WriteConcern.valueOf(writeConcern));
    } else {
      r = bulk.execute();
    }
    sendOK(message, new JsonObject()
                    .put("inserted", r.getInsertedCount())
                    .put("matched", r.getMatchedCount())
                    .put("modified", r.getModifiedCount())
                    .put("removed", r.getRemovedCount())
    );
  }

  private void doFind(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    Integer limit = (Integer) message.body().getInteger("limit");
    if (limit == null) {
      limit = -1;
    }
    Integer skip = (Integer) message.body().getInteger("skip");
    if (skip == null) {
      skip = -1;
    }
    Integer batchSize = (Integer) message.body().getInteger("batch_size");
    if (batchSize == null) {
      batchSize = 100;
    }
    Integer timeout = (Integer) message.body().getInteger("timeout");
    if (timeout == null || timeout < 0) {
      timeout = 10000; // 10 seconds
    }
    JsonObject matcher = message.body().getJsonObject("matcher");
    JsonObject keys = message.body().getJsonObject("keys");

    Object hint = message.body().getValue("hint");
    Object sort = message.body().getValue("sort");
    DBCollection coll = db.getCollection(collection);
    // add read preference
    final String overrideReadPreference = message.body().getString("read_preference");
    if(overrideReadPreference != null){
      coll.setReadPreference(ReadPreference.valueOf(overrideReadPreference));
    }
    // call find
    DBCursor cursor;
    if (matcher != null) {
      cursor = (keys == null) ?
          coll.find(jsonToDBObject(matcher)) :
          coll.find(jsonToDBObject(matcher), jsonToDBObject(keys));
    } else {
      cursor = coll.find();
    }
    if (skip != -1) {
      cursor.skip(skip);
    }
    if (limit != -1) {
      cursor.limit(limit);
    }
    if (sort != null) {
      cursor.sort(sortObjectToDBObject(sort));
    }
    if (hint != null) {
      if (hint instanceof JsonObject) {
        cursor.hint(jsonToDBObject((JsonObject) hint));
      } else if (hint instanceof String) {
        cursor.hint((String) hint);
      } else {
        throw new IllegalArgumentException("Cannot handle type " + hint.getClass().getSimpleName());
      }
    }
    sendBatch(message, cursor, batchSize, timeout);
  }

  private DBObject sortObjectToDBObject(Object sortObj) {
    if (sortObj instanceof JsonObject) {
      // Backwards compatability and a simpler syntax for single-property sorting
      return jsonToDBObject((JsonObject) sortObj);
    } else if (sortObj instanceof JsonArray) {
      JsonArray sortJsonObjects = (JsonArray) sortObj;
      DBObject sortDBObject = new BasicDBObject();
      for (Object curSortObj : sortJsonObjects) {
        if (!(curSortObj instanceof JsonObject)) {
          throw new IllegalArgumentException("Cannot handle type "
              + curSortObj.getClass().getSimpleName());
        }

        sortDBObject.putAll(((JsonObject) curSortObj).getMap());
      }

      return sortDBObject;
    } else {
      throw new IllegalArgumentException("Cannot handle type " + sortObj.getClass().getSimpleName());
    }
  }

  private void sendBatch(Message<JsonObject> message, final DBCursor cursor, final int max, final int timeout) {
    JsonArray results = new JsonArray();
    while (cursor.hasNext()) {
      DBObject obj = cursor.next();
      JsonObject m = dbObjectToJsonObject(obj);
      results.add(m);
    }
    JsonObject reply = createBatchMessage("ok", results);
    message.reply(reply);
    cursor.close();
  }

  private JsonObject createBatchMessage(String status, JsonArray results) {
    JsonObject reply = new JsonObject();
    reply.put("results", results);
    reply.put("status", status);
    reply.put("number", results.size());
    return reply;
  }

  private void doFindOne(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    JsonObject matcher = message.body().getJsonObject("matcher");
    JsonObject keys = message.body().getJsonObject("keys");
    DBCollection coll = db.getCollection(collection);
    // add read preference
    final String overrideReadPreference = message.body().getString("read_preference");
    if(overrideReadPreference != null){
      coll.setReadPreference(ReadPreference.valueOf(overrideReadPreference));
    }
    // call find
    DBObject res;
    if (matcher == null) {
      res = keys != null ? coll.findOne(null, jsonToDBObject(keys)) : coll.findOne();
    } else {
      res = keys != null ? coll.findOne(jsonToDBObject(matcher), jsonToDBObject(keys)) : coll.findOne(jsonToDBObject(matcher));
    }
    JsonObject reply = new JsonObject();
	JsonArray fetch = message.body().getJsonArray("fetch");
	if (res != null) {
		if (fetch != null) {
			for (Object attr : fetch) {
				if (!(attr instanceof String)) continue;
				String f = (String) attr;
				Object tmp = res.get(f);
				if (tmp == null || !(tmp instanceof DBRef)) continue;
				res.put(f, fetchRef((DBRef) tmp));
			}
		}
      JsonObject m = dbObjectToJsonObject(res);
      reply.put("result", m);
    }
    sendOK(message, reply);
  }

  private DBObject fetchRef(final DBRef ref){
    return db.getCollection(ref.getCollectionName()).findOne(ref.getId());
  }

  private void doFindAndModify(Message<JsonObject> message) {
    String collectionName = getMandatoryString("collection", message);
    if (collectionName == null) {
      return;
    }
    JsonObject msgBody = message.body();
    DBObject update = jsonToDBObjectNullSafe(msgBody.getJsonObject("update"));
    DBObject query = jsonToDBObjectNullSafe(msgBody.getJsonObject("matcher"));
    DBObject sort = jsonToDBObjectNullSafe(msgBody.getJsonObject("sort"));
    DBObject fields = jsonToDBObjectNullSafe(msgBody.getJsonObject("fields"));
    boolean remove = msgBody.getBoolean("remove", false);
    boolean returnNew = msgBody.getBoolean("new", false);
    boolean upsert = msgBody.getBoolean("upsert", false);

    DBCollection collection = db.getCollection(collectionName);
    DBObject result = collection.findAndModify(query, fields, sort, remove,
      update, returnNew, upsert);

    JsonObject reply = new JsonObject();
    if (result != null) {
      JsonObject resultJson = dbObjectToJsonObject(result);
      reply.put("result", resultJson);
    }
    sendOK(message, reply);
  }

  private void doCount(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    JsonObject matcher = message.body().getJsonObject("matcher");
    DBCollection coll = db.getCollection(collection);
    // add read preference
    final String overrideReadPreference = message.body().getString("read_preference");
    if(overrideReadPreference != null){
      coll.setReadPreference(ReadPreference.valueOf(overrideReadPreference));
    }
    // call find
    long count;
    if (matcher == null) {
      count = coll.count();
    } else {
      count = coll.count(jsonToDBObject(matcher));
    }
    JsonObject reply = new JsonObject();
    reply.put("count", count);
    sendOK(message, reply);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void doDistinct(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    String key = getMandatoryString("key", message);
    if (key == null) {
      return;
    }
    JsonObject matcher = message.body().getJsonObject("matcher");
    DBCollection coll = db.getCollection(collection);
	List values;
    if (matcher == null) {
      values = coll.distinct(key);
    } else {
      values = coll.distinct(key, jsonToDBObject(matcher));
    }
    JsonObject reply = new JsonObject();
    reply.put("values", new JsonArray(values));
    sendOK(message, reply);
  }

  private void doDelete(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    JsonObject matcher = getMandatoryObject("matcher", message);
    if (matcher == null) {
      return;
    }
    DBCollection coll = db.getCollection(collection);
    DBObject obj = jsonToDBObject(matcher);
    WriteConcern writeConcern = WriteConcern.valueOf(getOptionalStringConfig("writeConcern", ""));
    // Backwards compatibility
    if (writeConcern == null) {
      writeConcern = WriteConcern.valueOf(getOptionalStringConfig("write_concern", ""));
    }

    if (writeConcern == null) {
      writeConcern = db.getWriteConcern();
    }
    WriteResult res = coll.remove(obj, writeConcern);
    int deleted = res.getN();
    JsonObject reply = new JsonObject().put("number", deleted);
    sendOK(message, reply);
  }

  private void getCollections(Message<JsonObject> message) {
    JsonObject reply = new JsonObject();
    reply.put("collections", new JsonArray(new ArrayList<>(db.getCollectionNames())));
    sendOK(message, reply);
  }

  private void dropCollection(Message<JsonObject> message) {

    JsonObject reply = new JsonObject();
    String collection = getMandatoryString("collection", message);

    if (collection == null) {
      return;
    }

    DBCollection coll = db.getCollection(collection);

    try {
      coll.drop();
      sendOK(message, reply);
    } catch (MongoException mongoException) {
      sendError(message, "exception thrown when attempting to drop collection: " + collection + " \n" + mongoException.getMessage());
    }
  }

  private void getCollectionStats(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);

    if (collection == null) {
      return;
    }

    DBCollection coll = db.getCollection(collection);
    CommandResult stats = coll.getStats();

    JsonObject reply = new JsonObject();
    reply.put("stats", dbObjectToJsonObject(stats));
    sendOK(message, reply);

  }

  private void doAggregation(Message<JsonObject> message) {
    if (isCollectionMissing(message)) {
      sendError(message, "collection is missing");
      return;
    }
    if (isPipelinesMissing(message.body().getJsonArray("pipelines"))) {
      sendError(message, "no pipeline operations found");
      return;
    }
    String collection = getMandatoryString("collection", message);
    JsonArray pipelinesAsJson = message.body().getJsonArray("pipelines");
    List<DBObject> pipelines = jsonPipelinesToDbObjects(pipelinesAsJson);

    DBCollection dbCollection = db.getCollection(collection);
    // v2.11.1 of the driver has an inefficient method signature in terms
    // of parameters, so we have to remove the first one
    DBObject firstPipelineOp = pipelines.remove(0);
    AggregationOutput aggregationOutput = dbCollection.aggregate(firstPipelineOp, pipelines.toArray(new DBObject[] {}));

    JsonArray results = new JsonArray();
    for (DBObject dbObject : aggregationOutput.results()) {
      results.add(dbObjectToJsonObject(dbObject));
    }

    JsonObject reply = new JsonObject();
    reply.put("results", results);
    sendOK(message, reply);
  }

  private List<DBObject> jsonPipelinesToDbObjects(JsonArray pipelinesAsJson) {
    List<DBObject> pipelines = new ArrayList<>();
    for (Object pipeline : pipelinesAsJson) {
      DBObject dbObject = jsonToDBObject((JsonObject) pipeline);
      pipelines.add(dbObject);
    }
    return pipelines;
  }

  private boolean isCollectionMissing(Message<JsonObject> message) {
    return getMandatoryString("collection", message) == null;
  }

  private boolean isPipelinesMissing(JsonArray pipelines) {
    return pipelines == null || pipelines.size() == 0;
  }

  private void runCommand(Message<JsonObject> message) {
    JsonObject reply = new JsonObject();

    String command = getMandatoryString("command", message);

    if (command == null) {
      return;
    }

    DBObject commandObject = MongoUtil.convertJsonToBson(command);
    CommandResult result = db.command(commandObject);
    Map<String,Object> resultMap = result.toMap();
    //BSONTimestamp cannot be serialized => only for replica set
    resultMap.remove("operationTime");
    resultMap.remove("$clusterTime");
    resultMap.remove("opTime");
    resultMap.remove("electionId");
    //
    reply.put("result", new JsonObject(resultMap));
    sendOK(message, reply);
  }

  private JsonObject dbObjectToJsonObject(DBObject obj) {
    if (useMongoTypes) {
      return MongoUtil.convertBsonToJson(obj);
    } else {
      return new JsonObject(obj.toMap());
    }
  }

  private DBObject jsonToDBObject(JsonObject object) {
    if (useMongoTypes) {
      return MongoUtil.convertJsonToBson(object);
    } else {
      return new BasicDBObject(object.getMap());
    }
  }

  private DBObject jsonToDBObjectNullSafe(JsonObject object) {
    if (object != null) {
      return jsonToDBObject(object);
    } else {
      return null;
    }
  }

}

