package io.example.bestpracticecontroller.PatchStyle;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.example.bestpracticecontroller.PatchStyle.Common.applyBestPracticeToWholeDeploymentObject;

public class Replacement {
    private final static Logger logger = LoggerFactory.getLogger(Replacement.class);
    /*
    Using replace is different from JSON patch(both Imperative/Declarative), you have to use a "read-then-write" operation.
    Replace in API = HTTP Put verb in wire level
    An optimistic lock failure can occur, because of changes between read and write operation.
    The optimistic lock is implemented by comparing resourceVersion.
    eg:
    Thread 1 <-read--  V1Deployment [resourceVersion = 1]
    Thread 2 --write-> V1Deployment  #server side version = 2
    Thread 1 --write-> V1Deployment (with changes) [resourceVersion =1 ]  #Conflict here since server side is 2 but thread 1 is version 1
    We are not going to do anything for this conflict because the next reconcile cycle will start with read-then-write again
     */
    public static V1Deployment replaceDeployment(V1Deployment orgDeploy, AppsV1Api api) throws ApiException {
        logger.debug("Using ReplaceDeployment method");
        V1Deployment afterDeploy = applyBestPracticeToWholeDeploymentObject(orgDeploy);
        return api.replaceNamespacedDeployment(
                orgDeploy.getMetadata().getName(),
                orgDeploy.getMetadata().getNamespace(),
                afterDeploy,
                null,
                null,
                null
        );
    }

}
