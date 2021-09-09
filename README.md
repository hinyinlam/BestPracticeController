# Best Practice Controller for Kubernetes

## What is this controller about?

This best practice controller is an example controller written in Pure Java by using K8S Java Client SDK.

Enforcing best practice in K8S in production for multiple teams is hard.

What if there is a way to encode every lesson learnt by someone else into a controller and let this controller be the operator?

By leveraging the operator pattern, we can setup policy for production use and continuous auditing if best practices have been enforced.

---

## Support doc
### Blog
This is the code for the blog:

[Kubernetes Controller — Implement in Java (Part 1)](https://medium.com/@hinyinlam/kubernetes-controller-implement-in-java-part-1-4bd717f88b55)

[K8S Java Client SDK — Patching and update a resource](https://medium.com/@hinyinlam/k8s-java-client-sdk-patching-and-update-a-resource-3b42de7f69ed)

Prerequisite reads:

[Coding K8S resource in Java — Part 1 of 2 (K8S API)](https://medium.com/@hinyinlam/coding-k8s-resource-in-java-part-1-of-2-k8s-api-f46ca6bae3d6)

[Coding K8S resource in Java — Part 2 (Java Client)](https://medium.com/@hinyinlam/coding-k8s-resource-in-java-part-2-java-client-4d3341687477)

### How to Build
Some known issues with Java 11's TLS with minikube
Note: When you build this project with Java 11 and Maven, please use:
```mvn clean package -Djdk.tls.client.protocols=TLSv1.2 -DskipTests```

---

## Release note:

### Version 0.1:
This version does not enforce best practice except showing off how:

1. Deep copy works in OpenAPI generated Java SDK
2. How to generate JSON Patch (imperative way) of submitted patch to the API server
3. How to write a controller with indexer and informer in Java
4. How event handling and reconciler loop

Do NOT use for production!




