package io.example.bestpracticecontroller;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerManager;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.extended.workqueue.DefaultRateLimitingQueue;
import io.kubernetes.client.extended.workqueue.RateLimitingQueue;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentList;
import io.kubernetes.client.util.Config;
import okhttp3.Call;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static io.example.bestpracticecontroller.ControllerHelper.deepCopy;
import static io.example.bestpracticecontroller.MyEventHandler.getDeploymentEventHandler;
import static io.example.bestpracticecontroller.PatchStyle.JsonPatchImperative.updateDeploymentByJsonPatch;

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
            //Step 1: Setup Informer so that it can list and watch changes and put things in workqueue and indexer
            //Tell informer what to watch and list for form the API server -- in this case Deployment changes
            SharedInformerFactory sharedInformerFactory = new SharedInformerFactory();
            SharedIndexInformer<V1Deployment> informer = sharedInformerFactory.sharedIndexInformerFor(
                    callParams -> getDeploymentListWatchCall(
                            callParams.resourceVersion,
                            callParams.timeoutSeconds,
                            callParams.watch
                    ),
                    V1Deployment.class,
                    V1DeploymentList.class,
                    1000*5 //in case something not update to date, or re-connection etc, resync fully
            );

            try {
                //Step 2: Setup a Rate limit queue so that informer knows where to put the changes
                RateLimitingQueue<Request> workQueue = new DefaultRateLimitingQueue<>();
                informer.addEventHandler(getDeploymentEventHandler(workQueue));

                //Step 3: Where should the business logic goes to? Also, make sure our logic has access to the resource object
                //By passing indexer from the informer, so business logic can access the local cached copy of resource object
                Reconciler reconciler = getReconciler(informer.getIndexer());

                //Step 4: Wire everything together:
                //        Which reconciler (business logic)
                //        Which workqueue to start when this controller start
                //        How many worker (reconciler) threads this controller should run
                Controller controller = ControllerBuilder.defaultBuilder(sharedInformerFactory)
                        .withReconciler(reconciler)
                        .withWorkQueue(workQueue)
                        .withWorkerCount(16)
                        .build();

                //Step 5: A controller manager to start/shutdown controller, if you have multiple controllers,
                //        this manager will provide multi-thread support. Also, orchestration for informer and
                //        various debug logging checkpoints etc
                ControllerManager manager = ControllerBuilder.controllerManagerBuilder(sharedInformerFactory)
                        .addController(controller)
                        .build();
                manager.run();
            }catch (Exception e){
                logger.debug("Exception in running", e.getMessage());
            }
        };
    }

    private Reconciler getReconciler(Indexer<V1Deployment> indexer){
        Reconciler reconciler = new Reconciler() { //Yes, i know it's a lambda, for clarity, keep traditional way

            private Result doWork(Request request){
                //Step 1: We got trigger by WorkQueue
                logger.debug("Reconciler: " + request.toString());

                //Step 2: Let's get the resource using key from the request
                //As a practice, when you need to mutate the the resource object state
                //deep copy the object because it is shared amongst all other controller/worker in the same JVM
                logger.debug("Indexer keys: " + indexer.listKeys());
                final V1Deployment _dont_use_orgDeploy = indexer.getByKey(request.getNamespace() + "/" + request.getName());
                if(_dont_use_orgDeploy==null){
                    //Since the delay between Request coming from workqueue and indexer can be very long
                    //The resource object might have been deleted from indexer, thus causing null pointer exception
                    //If null point exception occuried, this thread will exit
                    logger.debug("Indexer is empty with object:" + request.getNamespace() + "/" + request.getName());
                    return new Result(false); //In our use case, object not exists == we don't apply any best practices
                }
                //Step 3: Deep Copy to keep in this thread, make sure the indexer's copy isn't touched
                V1Deployment orgDeploy = deepCopy(_dont_use_orgDeploy);
                logger.debug("Indexer getByKey: " + orgDeploy.getMetadata().getName());

                //Step 4: Do business logic to patch/update/replace resource to achieve our goal
                V1Deployment responsedDeploy = doBusinessLogicToResource(orgDeploy, api);

                //Step 5: Report if the business logic has been success or not
                if(responsedDeploy == null){
                    return new Result(true); //requque because last request failed
                }
                return new Result(false);
            }

            @Override
            public Result reconcile(Request request) {
                try{ //protection of uncaught exception
                    //Please read https://github.com/kubernetes-client/java/issues/961 for reason
                    //we want to catch exception to make sure this thread keep alive as much as possible
                    return this.doWork(request);
                }catch (Exception e){
                    logger.debug(e.getMessage());
                }
                return new Result(false);
            }
        };
        return reconciler;
    }

    private V1Deployment doBusinessLogicToResource(V1Deployment orgDeploy, AppsV1Api api) {
        V1Deployment responsedDeploy;
        try {
            //This part is for K8S resource patching, please uncomment the method you want to use
            responsedDeploy = updateDeploymentByJsonPatch(orgDeploy, api);
            //responsedDeploy = updateDeploymentByJsonMerge(orgDeploy, api);
            //responsedDeploy = replaceDeployment(orgDeploy, api);
            logger.debug("Updated/Patched - ResponsedDeploy: " + responsedDeploy.getMetadata().getName());
            return responsedDeploy;
        } catch (ApiException e) {
            e.printStackTrace();
        } catch (RuntimeException e){
            e.printStackTrace();
        }
        return null;
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
