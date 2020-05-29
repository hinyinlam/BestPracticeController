package io.example.bestpracticecontroller.PatchStyle;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;

import java.util.List;

import static io.example.bestpracticecontroller.ControllerHelper.deepCopy;

public class Common {
    //Can be used for replacement or JSON Patch (Imperative/instruction style)
    public static V1Deployment applyBestPracticeToWholeDeploymentObject(V1Deployment before){
        V1Deployment afterDeploy = deepCopy(before);
        //Replica should be >2 for HA
        if(afterDeploy.getSpec().getReplicas() < 2){
            afterDeploy.getSpec().setReplicas(2);
        }

        //Making sure no container uses more than 1 CPU and 80Mi of RAM
        //Yup, this is a very navie way to force every container to have limit, this is just a demo controller!
        List<V1Container> containers = afterDeploy.getSpec().getTemplate().getSpec().getContainers();
        for(V1Container container: containers) {
            V1ResourceRequirements rr = container.getResources();
            rr.putLimitsItem("cpu", Quantity.fromString("1"));
            rr.putLimitsItem("memory", Quantity.fromString("80Mi"));
            rr.putRequestsItem("cpu",Quantity.fromString("0.3"));
            rr.putRequestsItem("memory",Quantity.fromString("50Mi"));
        }

        //Update meta data and labels
        V1ObjectMeta meta = afterDeploy.getMetadata();
        meta.putLabelsItem("BestPracticeControllerLabel", "Version0.1");
        meta.putAnnotationsItem("io.example.bestpracticecontroller/config-version","0.1");

        return afterDeploy;
    }
}
