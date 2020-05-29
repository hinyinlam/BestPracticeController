package io.example.bestpracticecontroller;

import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.workqueue.RateLimitingQueue;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.openapi.models.V1Deployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyEventHandler {
    private final static Logger logger = LoggerFactory.getLogger(MyEventHandler.class);

    public static ResourceEventHandler<V1Deployment> getDeploymentEventHandler(RateLimitingQueue<Request> workQueue) {
        return new ResourceEventHandler<V1Deployment>() {
            @Override
            public void onAdd(V1Deployment deploy) {
                workQueue.add(new Request(deploy.getMetadata().getNamespace(), deploy.getMetadata().getName()));
                logger.debug("WQ len: " + workQueue.length() +" Event Handler(OnAdd) Name: " + deploy.getMetadata().getName() + " Version: " + deploy.getMetadata().getResourceVersion());
            }

            @Override
            public void onUpdate(V1Deployment before, V1Deployment after) {
                workQueue.add(new Request(before.getMetadata().getNamespace(), before.getMetadata().getName()));
                logger.debug("WQ len: " + workQueue.length() +" Event Handler(OnUpdate) Name: " + before.getMetadata().getName() + " Before Version: " + before.getMetadata().getResourceVersion() + "After version: " + after.getMetadata().getResourceVersion());
            }

            @Override
            public void onDelete(V1Deployment deploy, boolean b) {
                //workQueue.add(new Request(deploy.getMetadata().getNamespace(), deploy.getMetadata().getName())); //do nothing as this controller does not need to finalize resource
                logger.debug("WQ len: " + workQueue.length() + " Event Handler(OnDelete) Name: " + deploy.getMetadata().getName() + " Delete boolean: " + b );
            }
        };
    }
}
