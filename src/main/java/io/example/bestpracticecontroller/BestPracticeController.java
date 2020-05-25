package io.example.bestpracticecontroller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.google.gson.Gson;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerManager;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.extended.workqueue.DefaultRateLimitingQueue;
import io.kubernetes.client.extended.workqueue.RateLimitingQueue;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.PatchUtils;
import io.kubernetes.client.util.Yaml;
import okhttp3.Call;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Component
public class BestPracticeController {

    AppsV1Api api;
    final static Logger logger = LoggerFactory.getLogger(BestPracticeController.class);

    public BestPracticeController() throws IOException {

        ApiClient client = Config.defaultClient();
        client.setReadTimeout(10);
        Configuration.setDefaultApiClient(client);
        this.api = new AppsV1Api();

        SharedInformerFactory sharedInformerFactory = new SharedInformerFactory();
        SharedIndexInformer<V1Deployment> informer = sharedInformerFactory.sharedIndexInformerFor(
                callParams -> getDeploymentListWatchCall(
                                    callParams.resourceVersion,
                                    callParams.timeoutSeconds,
                                    callParams.watch
                              ),
                V1Deployment.class,
                V1DeploymentList.class,
                1000*60
        );

        RateLimitingQueue<Request> workQueue = new DefaultRateLimitingQueue<>();
        informer.addEventHandler(getDeploymentEventHandler(workQueue));

        Reconciler reconciler = getReconciler(informer.getIndexer());

        Controller controller = ControllerBuilder.defaultBuilder(sharedInformerFactory)
                .withReconciler(reconciler)
                .withWorkQueue(workQueue)
                .build();
        ControllerManager manager = ControllerBuilder.controllerManagerBuilder(sharedInformerFactory)
                .addController(controller)
                .build();
        manager.run();
    }

    public String dumpJSON(Object crd){
        Gson gson = new Gson();
        return gson.toJson(crd);
    }

    public String dumpYAMLwithWorkaround(Object crd){
        //System.out.println(Yaml.dump(crd));
        //You should see there is a bug that cannot dump the essential boolean value of V1beta1CustomResourceDefinitionVersion
        //See https://github.com/kubernetes-client/java/issues/340

        Gson gson = new Gson();
        String json = gson.toJson(crd);

        org.yaml.snakeyaml.Yaml yaml = new org.yaml.snakeyaml.Yaml();
        Object result = yaml.load(json);

        return yaml.dumpAsMap(result);
    }

    private Reconciler getReconciler(Indexer<V1Deployment> indexer){
        Reconciler reconciler = new Reconciler() { //Yes, i know it's a lambda, for clarity, keep traditional way
            @Override
            public Result reconcile(Request request) {
                logger.debug("Reconciler: " + request.toString());
                V1Deployment orgDeploy = indexer.getByKey(request.getNamespace() + "/" + request.getName());

                if (updateDeploymentByJsonPatch(orgDeploy)) {
                    return new Result(false);
                }
                return new Result(false);
            }
        };
        return reconciler;
    }

    private boolean updateDeploymentByJsonPatch(V1Deployment orgDeploy) {
        //As a practice, when you need to mutate the the resource object state
        //deep copy the object because it is shared amongst all other controller/worker in the same JVM
        V1Deployment beforeDeployment = deepCopy(orgDeploy);
        V1Deployment afterDeployment = changeAnnotationAndLabel(beforeDeployment);

        logger.debug("UpdateDeploymentByJsonPatch(PatchOps): " + orgDeploy.getMetadata().getNamespace() + "/" + orgDeploy.getMetadata().getName());
        String patch = getJSONPatchOps(beforeDeployment, afterDeployment);
        try {
            executeJSONPatch(beforeDeployment,patch, V1Patch.PATCH_FORMAT_JSON_PATCH);
            return true;
        } catch (ApiException e) {
            logger.warn(e.getResponseBody());
            e.printStackTrace();
        }
        return false;
    }

    //This is the changes we want to make to the Deployment
    private V1Deployment changeAnnotationAndLabel(V1Deployment beforeDeployment) {
        V1Deployment afterDeployment = deepCopy(beforeDeployment);//make sure the before and after has no referencing
        V1ObjectMeta meta = afterDeployment.getMetadata();
        meta.putLabelsItem("a", "b");
        meta.putAnnotationsItem("annotationkey","value");
        return afterDeployment;
    }

    //Somehow certain field that we do not care get caught by JSONPatch, this allows a cleaner Patch Ops
    //This is very manually
    //#TODO: Any better way to setup resource object that JSONPatch does not do unnecessary work?
    private V1Deployment removeUnnecessaryFieldForPatch(V1Deployment deployment) {
        deployment.setStatus(null); //No need to patch the status
        deployment.getMetadata().setCreationTimestamp(null);
        return deployment;
    }

    //easiest way (for devloper) is to just export and import
    private <T> T deepCopy(T rd){
        Class<T> resourceClass = (Class<T>) rd.getClass();
        return Yaml.loadAs(Yaml.dump(rd), resourceClass);
    }

    //Declarative way of JSON patch
    private String getAnnotationJSONMergePatch(){
        V1ObjectMeta meta = new V1ObjectMetaBuilder()
                .withAnnotations(Map.of("io.example.bestpractice","managing"))
                .withLabels(Map.of("a", "b"))
                .build();
        V1Deployment newDeploy = new V1DeploymentBuilder()
                .withMetadata(meta)
                .build();
        String patch = dumpJSON(newDeploy);

        logger.debug(patch);
        return patch;
    }

    //Imperative way of JSON patch
    private String getJSONPatchOps(V1Deployment beforeDeploy, V1Deployment afterDeploy){

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

    private V1Deployment executeJSONPatch(V1Deployment orgDeploy, String patch, String patchType) throws ApiException {
        try {
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
        }catch (RuntimeException e){
            e.printStackTrace();
            logger.debug(e.getMessage());
        }
        return null;
    }

    private ResourceEventHandler<V1Deployment> getDeploymentEventHandler(RateLimitingQueue<Request> workQueue) {
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
                //workQueue.add(new Request(deploy.getMetadata().getNamespace(), deploy.getMetadata().getName()));
                logger.debug("WQ len: " + workQueue.length() + " Event Handler(OnDelete) Name: " + deploy.getMetadata().getName() + " Delete boolean: " + b );
            }
        };
    }

    public Call getDeploymentListWatchCall(String resourceVersion, Integer timeoutSeconds, Boolean watch) {
        try {
            return api.listNamespacedDeploymentCall(
                    "prod",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    resourceVersion, //Only interest in changes later than this version
                    timeoutSeconds, //allow retry and rate limit to work
                    watch, //First call is "list", then "watch",when resync watch=false and afterward watch=true
                    null
            );
        } catch (ApiException e) {
            e.printStackTrace();
        }
        return null;
    }
}
