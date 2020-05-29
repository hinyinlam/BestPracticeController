package io.example.bestpracticecontroller.PatchStyle;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerBuilder;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static io.example.bestpracticecontroller.ControllerHelper.dumpJSON;
import static io.example.bestpracticecontroller.ControllerHelper.executeJSONPatch;

public class JsonMergeDeclarative {
    private static final Logger logger = LoggerFactory.getLogger(JsonMergeDeclarative.class);

    public static V1Deployment updateDeploymentByJsonMerge(V1Deployment deploy, AppsV1Api api) throws ApiException {
        logger.debug("UpdateDeploymentByJsonMerge(Merge - Declarative): " + deploy.getMetadata().getNamespace() + "/" + deploy.getMetadata().getName());
        V1Deployment delta = generateBestPracticeDelta(); //adding the delta
        String patch = dumpJSON(delta);
        return executeJSONPatch(deploy, patch, V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH, api);
    }
    //Declarative way of JSON patch (Merge Patch) -- Just declare what is the delta, you don't even need the original object!
    private static V1Deployment generateBestPracticeDelta(){

        //you have to manually declare the change itself, then server side will determine how to deal with the resource JSON
        V1Container container = new V1ContainerBuilder()
                .withNewResources()
                .addToLimits("cpu", Quantity.fromString("1"))
                .addToLimits("memory", Quantity.fromString("80Mi"))
                .addToRequests("cpu",Quantity.fromString("0.3"))
                .addToRequests("memory",Quantity.fromString("50Mi"))
                .endResources()
                .build();

        V1Deployment deploy = new V1DeploymentBuilder()
                .withNewMetadata()
                .addToLabels("BestPracticeControllerLabel", "Version0.1")
                .addToAnnotations("io.example.bestpracticecontroller/config-version","0.1")
                .endMetadata()
                .withNewSpec()
                .withNewTemplate()
                .withNewSpec()
                .withContainers(Arrays.asList(container))
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        return deploy;
    }

}
