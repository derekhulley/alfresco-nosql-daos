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

import static org.junit.Assert.assertNotNull;

import org.alfresco.model.ContentModel;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.cmr.repository.StoreRef;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.tradeshift.test.remote.Remote;
import com.tradeshift.test.remote.RemoteTestRunner;

/**
 * Tests the {@link MongoNodeDAOImpl}.
 */
@RunWith(RemoteTestRunner.class)
@Remote(runnerClass=SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:alfresco/application-context.xml")
public class MongoNodeDAOImplTest
{
    private static Logger logger = Logger.getLogger(MongoNodeDAOImplTest.class);

    @Autowired
    @Qualifier("NodeService")
    protected NodeService nodeService;
    
    private NodeRef createNode()
    {
        AuthenticationUtil.setAdminUserAsFullyAuthenticatedUser();
        StoreRef storeRef = nodeService.createStore(StoreRef.PROTOCOL_TEST, "" + System.nanoTime());
        NodeRef rootNodeRef = nodeService.getRootNode(storeRef);
        NodeRef nodeRef = nodeService.createNode(
                rootNodeRef,
                ContentModel.ASSOC_CHILDREN,
                QName.createQName(NamespaceService.ALFRESCO_URI, "testNode"),
                ContentModel.TYPE_CONTENT).getChildRef();
        return nodeRef;
    }

    @Test
    public void testWiring()
    {
        assertNotNull(nodeService);
    }
    
    @Test
    public void testAddAspect()
    {
        NodeRef nodeRef = createNode();
        nodeService.addAspect(nodeRef, ContentModel.ASPECT_TEMPORARY, null);
    }
}
