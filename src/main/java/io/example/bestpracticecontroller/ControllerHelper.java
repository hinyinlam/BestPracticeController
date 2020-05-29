package io.example.bestpracticecontroller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.google.gson.Gson;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.util.PatchUtils;
import io.kubernetes.client.util.Yaml;

public class ControllerHelper {
    public static String dumpJSON(Object crd){
        Gson gson = new Gson();
        return gson.toJson(crd);
    }
    //Imperative way of JSON patch
    public static String getJSONPatchOps(V1Deployment beforeDeploy, V1Deployment afterDeploy){

        beforeDeploy = removeUnnecessaryFieldForPatch(beforeDeploy);
        afterDeploy = removeUnnecessaryFieldForPatch(afterDeploy);

        String beforeJson = dumpJSON(beforeDeploy);
        String afterJson = dumpJSON(afterDeploy);

        ObjectMapper om = new ObjectMapper();
        try {
            JsonNode newDeployJsonNode = om.readValue(afterJson,JsonNode.class);
            JsonNode currentDeployJsonNode = om.readValue(beforeJson,JsonNode.class);
            JsonNode patch = JsonDiff.asJson(currentDeployJsonNode, newDeployJsonNode);
            return patch.toString();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "";
    }

    //Somehow certain field that we do not care get caught by JSONPatch, this allows a cleaner Patch Ops
    //This is very manually
    //#TODO: Any better way to setup resource object that JSONPatch does not do unnecessary work?
    private static V1Deployment removeUnnecessaryFieldForPatch(V1Deployment deployment) {
        deployment.setStatus(null); //No need to patch the status
        deployment.getMetadata().setCreationTimestamp(null);
        return deployment;
    }

    //easiest way (for devloper) is to just export and import
    public static <T> T deepCopy(T rd){
        Class<T> resourceClass = (Class<T>) rd.getClass();
        return Yaml.loadAs(Yaml.dump(rd), resourceClass);
    }


    public static V1Deployment executeJSONPatch(V1Deployment orgDeploy, String patch, String patchType, AppsV1Api api) throws ApiException {
        return PatchUtils.patch(
                V1Deployment.class,
                () -> api.patchNamespacedDeploymentCall(
                        orgDeploy.getMetadata().getName(),
                        orgDeploy.getMetadata().getNamespace(),
                        new V1Patch(patch),
                        null,
                        null,
                        null,
                        null,
                        null
                ),
                patchType,
                api.getApiClient()
        );
    }
}
