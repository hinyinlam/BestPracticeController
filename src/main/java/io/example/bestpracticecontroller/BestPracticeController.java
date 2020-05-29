package io.example.bestpracticecontroller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.google.gson.Gson;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerManager;
import io.kubernetes.client.extended.controller.DefaultController;
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
import lombok.SneakyThrows;
import okhttp3.Call;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Component
@org.springframework.context.annotation.Configuration
public class BestPracticeController {

    AppsV1Api api;
    final static Logger logger = LoggerFactory.getLogger(BestPracticeController.class);

    public BestPracticeController() throws IOException {

        ApiClient client = Config.defaultClient();
        client.setReadTimeout(10);
        Configuration.setDefaultApiClient(client);
        this.api = new AppsV1Api();


    }

    @Bean
    public CommandLineRunner startController(){
        return arg -> {
            SharedInformerFactory sharedInformerFactory = new SharedInformerFactory();
            SharedIndexInformer<V1Deployment> informer = sharedInformerFactory.sharedIndexInformerFor(
                    callParams -> getDeploymentListWatchCall(
                            callParams.resourceVersion,
                            callParams.timeoutSeconds,
                            callParams.watch
                    ),
                    V1Deployment.class,
                    V1DeploymentList.class,
                    1000*5
            );

            try {
                RateLimitingQueue<Request> workQueue = new DefaultRateLimitingQueue<>();
                informer.addEventHandler(getDeploymentEventHandler(workQueue));

                //Reconciler reconciler = getReconciler(informer.getIndexer());
                Reconciler reconciler = getNullPointerReconciler(informer.getIndexer());

                Controller controller = ControllerBuilder.defaultBuilder(sharedInformerFactory)
                        .withReconciler(reconciler)
                        .withWorkQueue(workQueue)
                        .withWorkerCount(16)
                        .build();

                ControllerManager manager = ControllerBuilder.controllerManagerBuilder(sharedInformerFactory)
                        .addController(controller)
                        .build();
                manager.run();
            }catch (Exception e){
                logger.debug("Exception in running", e.getMessage());
            }
        };
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

    private Reconciler getNullPointerReconciler(Indexer<V1Deployment> indexer){
        return new Reconciler() {

            @Override
            public Result reconcile(Request request) {
                try{
                    System.out.println("Current Reconcile thread name: " + Thread.currentThread().getName());
//                    return this.doWork(request);
                }catch (Exception e){
                    logger.debug(e.getMessage());
                }

                //logger.debug("Throw nullpointer exception");
                //throw new NullPointerException();
                return new Result(false);
            }
        };
    }

    private Reconciler getReconciler(Indexer<V1Deployment> indexer){
        Reconciler reconciler = new Reconciler() { //Yes, i know it's a lambda, for clarity, keep traditional way

            @SneakyThrows
            private Result doWork(Request request){
                logger.debug("Reconciler: " + request.toString());

                //As a practice, when you need to mutate the the resource object state
                //deep copy the object because it is shared amongst all other controller/worker in the same JVM
                logger.debug("Indexer keys: " + indexer.listKeys());
                final V1Deployment _dont_use_orgDeploy = indexer.getByKey(request.getNamespace() + "/" + request.getName());
                /*
                if(_dont_use_orgDeploy==null){
                    //Since the delay between Request coming from workqueue and indexer can be very long
                    //The resource object might have been deleted from indexer, thus causing null pointer exception
                    //If null point exception occuried, this thread will exit
                    logger.debug("Indexer is empty with object:" + request.getNamespace() + "/" + request.getName());
                    return new Result(false); //In our use case, object not exists == we don't apply any best practices
                }
                logger.debug("Indexer getByKey: " + _dont_use_orgDeploy.getMetadata().getName());
                 */

                V1Deployment orgDeploy = deepCopy(_dont_use_orgDeploy);

                V1Deployment responsedDeploy;

                try {
                    responsedDeploy = replaceDeployment(orgDeploy);
                    //responsedDeploy = updateDeploymentByJsonPatch(orgDeploy);
                    //responsedDeploy = updateDeploymentByJsonMerge(orgDeploy);
                    logger.debug("Updated/Patched - ResponsedDeploy: " + responsedDeploy.getMetadata().getName());
                    //return new Result(false);
                } catch (ApiException e) {
                    logger.debug(e.getResponseBody());
                    logger.debug(e.getStackTrace().toString());
                }catch (RuntimeException e){
                    logger.debug(e.getMessage());
                }
                return new Result(true); //requeue because last request failed
            }

            @Override
            public Result reconcile(Request request) {
                try{
                    return this.doWork(request);
                }catch (Exception e){
                    logger.debug(e.getMessage());
                }
                return new Result(false);
            }

        };
        return reconciler;
    }

    /*
    Using replace is different from patch, you have to use a "read-then-write" operation.
    Replace in API = HTTP Put verb in wire level
    An optimistic lock failure can occur, because of changes between read and write operation.
    The optimistic lock is implemented by comparing resourceVersion.
    eg:
    Thread 1 <-read--  V1Deployment [resourceVersion = 1]
    Thread 2 --write-> V1Deployment  #server side version = 2
    Thread 1 --write-> V1Deployment (with changes) [resourceVersion =1 ]  #Conflict here since server side is 2 but thread 1 is version 1
    We are not going to do anything for this conflict because the next reconcile cycle will start with read-then-write again
     */
    private V1Deployment replaceDeployment(V1Deployment orgDeploy) throws ApiException {
        logger.debug("Using ReplaceDeployment method");
        V1Deployment afterDeploy = changeAnnotationAndLabel(orgDeploy);
        return api.replaceNamespacedDeployment(
                orgDeploy.getMetadata().getName(),
                orgDeploy.getMetadata().getNamespace(),
                afterDeploy,
                null,
                null,
                null
                );
    }

    private V1Deployment updateDeploymentByJsonPatch(V1Deployment beforeDeployment) throws ApiException {
        logger.debug("UpdateDeploymentByJsonPatch(Ops - Impreative): " + beforeDeployment.getMetadata().getNamespace() + "/" + beforeDeployment.getMetadata().getName());

        V1Deployment afterDeployment = changeAnnotationAndLabel(deepCopy(beforeDeployment));

        String patch = getJSONPatchOps(beforeDeployment, afterDeployment);
        return executeJSONPatch(beforeDeployment,patch, V1Patch.PATCH_FORMAT_JSON_PATCH);
    }

    private V1Deployment updateDeploymentByJsonMerge(V1Deployment deploy) throws ApiException {
        logger.debug("UpdateDeploymentByJsonMerge(Merge - Declarative): " + deploy.getMetadata().getNamespace() + "/" + deploy.getMetadata().getName());
        String patch = getAnnotationJSONMergePatch();
        return executeJSONPatch(deploy, patch, V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH);
    }

    //This is the changes we want to make to the Deployment
    private V1Deployment changeAnnotationAndLabel(V1Deployment afterDeployment) {
        V1ObjectMeta meta = afterDeployment.getMetadata();
        meta.putLabelsItem("a", "b");
        meta.getLabels().remove("app");
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
                .addToLabels("a","b")
                .addToLabels("app",null)
                .build();
        V1Deployment newDeploy = new V1DeploymentBuilder()
                .withMetadata(meta)
                .build();
        String patch = dumpJSON(newDeploy);
        logger.debug("Get Annotation JSONMergePatch: " + patch);

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
