package io.example.bestpracticecontroller.PatchStyle;

import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.example.bestpracticecontroller.ControllerHelper.executeJSONPatch;
import static io.example.bestpracticecontroller.ControllerHelper.getJSONPatchOps;
import static io.example.bestpracticecontroller.PatchStyle.Common.applyBestPracticeToWholeDeploymentObject;

public class JsonPatchImperative {

    public final static Logger logger = LoggerFactory.getLogger(JsonPatchImperative.class);

    public static V1Deployment updateDeploymentByJsonPatch(V1Deployment beforeDeployment, AppsV1Api api) throws ApiException {
        logger.debug("UpdateDeploymentByJsonPatch(Ops - Impreative): " + beforeDeployment.getMetadata().getNamespace() + "/" + beforeDeployment.getMetadata().getName());

        V1Deployment afterDeployment = applyBestPracticeToWholeDeploymentObject(beforeDeployment);

        String patch = getJSONPatchOps(beforeDeployment, afterDeployment);//Perform delta and output a set of instruction on what to change
        return executeJSONPatch(beforeDeployment,patch, V1Patch.PATCH_FORMAT_JSON_PATCH, api);
    }
}
