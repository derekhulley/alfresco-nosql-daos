/*
    Licensed to the Apache Software Foundation (ASF) under one or more
	contributor license agreements.  See the NOTICE file distributed with
	this work for additional information regarding copyright ownership.
	The ASF licenses this file to You under the Apache License, Version 2.0
	(the "License"); you may not use this file except in compliance with
	the License.  You may obtain a copy of the License at
	
	http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
 */
package org.alfresco.module.daos.mongo;

import org.alfresco.repo.domain.node.ibatis.NodeDAOImpl;
import org.alfresco.service.namespace.QName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.dao.ConcurrencyFailureException;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;

/**
 * MongoDB override to store node properties and aspects
 * 
 * @author Gabriele Columbro
 * @author Derek Hulley
 * 
 * @since 1.0
 */
public class MongoNodeDAOImpl extends NodeDAOImpl implements ApplicationListener<ApplicationContextEvent>
{
    public static final String COLLECTION_ASPECTS = "node_aspects";
    public static final String COLLECTION_PROPERTIES = "node_properties";
    
    private static Log log = LogFactory.getLog(MongoNodeDAOImpl.class);

    private boolean duplicateToSql = true;
    
    private String mongoHost;
    private int mongoPort;
    private String mongoDatabase;
    private MongoClient mongo;
    private DBCollection aspects;
    private DBCollection properties;
    
    /**
     * @param duplicateToSql    <tt>true</tt> to write through to the base SQL implementations <b>as well</b>
     */
    public void setDuplicateToSql(boolean duplicateToSql)
    {
        this.duplicateToSql = duplicateToSql;
    }
    
    /**
     * @param mongoHost         the MongoDB host e.g. <b>127.0.0.1</b>
     */
    public void setMongoHost(String mongoHost)
    {
        this.mongoHost = mongoHost;
    }

    /**
     * @param mongoPort      the MongoDB port e.g. <b>27017</b>
     */
    public void setMongoPort(int mongoPort)
    {
        this.mongoPort = mongoPort;
    }

    /**
     * @param mongoDB       the name of the Mongo database e.g. <b></b>
     */
    public void setMongoDatabase(String mongoDatabase)
    {
        this.mongoDatabase = mongoDatabase;
    }

    @Override
    public void onApplicationEvent(ApplicationContextEvent event)
    {
        if (event instanceof ContextRefreshedEvent)
        {
            if (mongo != null)
            {
                try
                {
                    if (mongo != null) { mongo.close(); }
                }
                catch (Exception e)
                {
                    logger.error("Failed to shut MongoDB connection cleanly.", e);
                }
            }
            try
            {
                mongo = new MongoClient(mongoHost, mongoPort);
                logger.info("Created MongoDB connection to " + mongo);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to create MongoDB client.", e);
            }
            DB db = mongo.getDB(mongoDatabase);
            initCollections(db);
        }
        else if (event instanceof ContextClosedEvent)
        {
            logger.info("Shutting down MongoDB connection to " + mongo);
            try
            {
                if (mongo != null) { mongo.close(); }
            }
            catch (Exception e)
            {
                logger.error("Failed to shut MongoDB connection cleanly.", e);
            }
        }
    }
    
    private static final String FIELD_NODE_ID = "nodeId";
    private static final String FIELD_TXN_ID = "txnId";
    private static final String FIELD_QNAME = "qname";
    
    /**
     * Ensure that the necessary indexes are in place for the Mongo collections
     */
    protected void initCollections(DB db)
    {
        aspects = db.getCollection(COLLECTION_ASPECTS);
        
        // @since 2.0
        DBObject ASPECTS_NODE_QNAME = BasicDBObjectBuilder
                .start(FIELD_NODE_ID, 1)
                .add(FIELD_QNAME, 1)
                .get();
        aspects.ensureIndex(ASPECTS_NODE_QNAME, "ASPECTS_NODE_QNAME", true);
        
        // @since 2.0
        DBObject ASPECTS_NODE_TXN_QNAME = BasicDBObjectBuilder
                .start(FIELD_NODE_ID, 1)
                .add(FIELD_TXN_ID, 1)
                .add(FIELD_QNAME, 1)
                .get();
        aspects.ensureIndex(ASPECTS_NODE_TXN_QNAME, "ASPECTS_NODE_TXN_QNAME", false);
        
        properties = db.getCollection(COLLECTION_PROPERTIES);
    }

    @Override
    protected void insertNodeAspect(Long nodeId, Long qnameId)
    {
        // Get the current transaction ID.
        Long txnId = getCurrentTransactionId(true);
        // Resolve the QName
        QName qname = qnameDAO.getQName(qnameId).getSecond();
        String qnameStr = qname.toString();
        
        DBObject insertObj = BasicDBObjectBuilder
                .start()
                .add(FIELD_NODE_ID, nodeId)
                .add(FIELD_TXN_ID, txnId)
                .add(FIELD_QNAME, qnameStr)
                .get();
        WriteResult result = aspects.insert(insertObj);
        if (result.getError() != null)
        {
            throw new ConcurrencyFailureException(
                    "Failed to insert aspect: " + result + "\n" +
                    "   Node ID:    " + nodeId + "\n" +
                    "   QName:      " + qnameStr + "\n" +
                    "   Txn ID:     " + txnId);
        }
        if (duplicateToSql)
        {
            // Duplicate
            super.insertNodeAspect(nodeId, qnameId);
        }
    }
}
