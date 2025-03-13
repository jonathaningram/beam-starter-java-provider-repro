# Repro steps

## Run it locally and observe it works

```shell
run_pipeline_on_local.sh pipeline.yaml
```

- Builds a Docker image with the Beam Python SDK and Java.
- Runs `pipeline.yaml`.

### ✅ Passes: Successfully uppercases the names

<details>
<summary>Local output</summary>

```
./run_pipeline_on_local.sh pipeline.yaml
[+] Building 2.7s (8/8) FINISHED                                                                                                                                                                                       docker:desktop-linux
 => [internal] load build definition from Dockerfile                                                                                                                                                                                   0.0s
 => => transferring dockerfile: 201B                                                                                                                                                                                                   0.0s
 => [internal] load metadata for docker.io/apache/beam_python3.11_sdk:2.63.0                                                                                                                                                           2.6s
 => [internal] load .dockerignore                                                                                                                                                                                                      0.0s
 => => transferring context: 2B                                                                                                                                                                                                        0.0s
 => [1/4] FROM docker.io/apache/beam_python3.11_sdk:2.63.0@sha256:3bac3a5e99da7dbc608dc977e8f6981c032a10a7f6e8d11fb394d5ed593885c4                                                                                                     0.0s
 => CACHED [2/4] RUN apt-get update &&     apt-get install -y default-jre &&     apt-get clean                                                                                                                                         0.0s
 => CACHED [3/4] RUN python --version                                                                                                                                                                                                  0.0s
 => CACHED [4/4] RUN java -version                                                                                                                                                                                                     0.0s
 => exporting to image                                                                                                                                                                                                                 0.0s
 => => exporting layers                                                                                                                                                                                                                0.0s
 => => writing image sha256:2b7925a67cd13207cc96b23142fbcc20c6f498fd54afdeee2efbb271fc5567b9                                                                                                                                           0.0s
 => => naming to docker.io/library/beam_python3.11_sdk_with_java:2.63.0                                                                                                                                                                0.0s

What's next:
    View a summary of image vulnerabilities and recommendations → docker scout quickview
INFO:root:Missing pipeline option (runner). Executing pipeline using the default runner: DirectRunner.
INFO:apache_beam.yaml.yaml_transform:Expanding "Create" at line 5
INFO:apache_beam.yaml.yaml_transform:Expanding "Identity" at line 12
INFO:apache_beam.utils.subprocess_server:Downloading job server jar from https://storage.googleapis.com/beam-starter-java-provider-repro/beam-2.63.0/xlang-transforms-bundled-1.0-SNAPSHOT.jar
INFO:root:Starting a JAR-based expansion service from JAR /root/.apache_beam/cache/jars/xlang-transforms-bundled-1.0-SNAPSHOT.jar
INFO:apache_beam.utils.subprocess_server:Starting service with ['java' '-jar' '/root/.apache_beam/cache/jars/xlang-transforms-bundled-1.0-SNAPSHOT.jar' '55249' '--filesToStage=/root/.apache_beam/cache/jars/xlang-transforms-bundled-1.0-SNAPSHOT.jar' '--alsoStartLoopbackWorker']
INFO:apache_beam.utils.subprocess_server:Starting expansion service at localhost:55249
INFO:apache_beam.utils.subprocess_server:[main] INFO org.apache.beam.sdk.expansion.service.ExpansionService - Registering external transforms: [beam:transform:group_into_batches:v1, beam:transform:group_into_batches_with_sharded_key:v1, beam:transform:combine_grouped_values:v1, beam:transform:create_view:v1, beam:transform:teststream:v1, beam:transform:sdf_process_keyed_elements:v1, beam:transform:combine_globally:v1, beam:external:java:generate_sequence:v1, beam:transform:redistribute_by_key:v1, beam:transform:window_into:v1, beam:transform:flatten:v1, beam:transform:impulse:v1, beam:transform:write_files:v1, beam:runners_core:transforms:splittable_process:v1, beam:transform:combine_per_key:v1, beam:transform:group_by_key:v1, beam:transform:redistribute_arbitrarily:v1, beam:transform:reshuffle:v1]
INFO:apache_beam.utils.subprocess_server:
INFO:apache_beam.utils.subprocess_server:Registered transforms:
INFO:apache_beam.utils.subprocess_server:	beam:transform:group_into_batches:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@5f8e8a9d
INFO:apache_beam.utils.subprocess_server:	beam:transform:group_into_batches_with_sharded_key:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@5745ca0e
INFO:apache_beam.utils.subprocess_server:	beam:transform:combine_grouped_values:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@3ad83a66
INFO:apache_beam.utils.subprocess_server:	beam:transform:create_view:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@3cce5371
INFO:apache_beam.utils.subprocess_server:	beam:transform:teststream:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@17bffc17
INFO:apache_beam.utils.subprocess_server:	beam:transform:sdf_process_keyed_elements:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@6e535154
INFO:apache_beam.utils.subprocess_server:	beam:transform:combine_globally:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@15a34df2
INFO:apache_beam.utils.subprocess_server:	beam:external:java:generate_sequence:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@5b38c1ec
INFO:apache_beam.utils.subprocess_server:	beam:transform:redistribute_by_key:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@338fc1d8
INFO:apache_beam.utils.subprocess_server:	beam:transform:window_into:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@4722ef0c
INFO:apache_beam.utils.subprocess_server:	beam:transform:flatten:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@48e1f6c7
INFO:apache_beam.utils.subprocess_server:	beam:transform:impulse:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@55cb6996
INFO:apache_beam.utils.subprocess_server:	beam:transform:write_files:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@1807e3f6
INFO:apache_beam.utils.subprocess_server:	beam:runners_core:transforms:splittable_process:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@480d3575
INFO:apache_beam.utils.subprocess_server:	beam:transform:combine_per_key:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@f1da57d
INFO:apache_beam.utils.subprocess_server:	beam:transform:group_by_key:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@194fad1
INFO:apache_beam.utils.subprocess_server:	beam:transform:redistribute_arbitrarily:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@26abb146
INFO:apache_beam.utils.subprocess_server:	beam:transform:reshuffle:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@72c8e7b
INFO:apache_beam.utils.subprocess_server:
INFO:apache_beam.utils.subprocess_server:Registered SchemaTransformProviders:
INFO:apache_beam.utils.subprocess_server:	some:urn:transform_name:v1
INFO:apache_beam.utils.subprocess_server:	some:urn:to_upper_case:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:filter-java:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:flatten:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:map_to_fields-java:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:window_into_strategy:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:generate_sequence:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:log_for_testing:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:explode:v1
INFO:root:Starting a JAR-based expansion service from JAR /root/.apache_beam/cache/jars/xlang-transforms-bundled-1.0-SNAPSHOT.jar
INFO:apache_beam.utils.subprocess_server:[grpc-default-executor-0] INFO org.apache.beam.sdk.expansion.service.ExpansionService - Expanding 'Identity/some:urn:transform_name:v1' with URN 'beam:expansion:payload:schematransform:v1'
INFO:apache_beam.yaml.yaml_transform:Expanding "ToUpperCase" at line 13
INFO:root:Starting a JAR-based expansion service from JAR /root/.apache_beam/cache/jars/xlang-transforms-bundled-1.0-SNAPSHOT.jar
INFO:apache_beam.utils.subprocess_server:[grpc-default-executor-0] INFO org.apache.beam.sdk.expansion.service.ExpansionService - Expanding 'ToUpperCase/some:urn:to_upper_case:v1' with URN 'beam:expansion:payload:schematransform:v1'
INFO:apache_beam.yaml.yaml_transform:Expanding "LogForTesting" at line 18
INFO:apache_beam.utils.subprocess_server:Downloading job server jar from https://repo.maven.apache.org/maven2/org/apache/beam/beam-sdks-java-extensions-sql-expansion-service/2.63.0/beam-sdks-java-extensions-sql-expansion-service-2.63.0.jar
INFO:root:Starting a JAR-based expansion service from JAR /root/.apache_beam/cache/jars/beam-sdks-java-extensions-sql-expansion-service-2.63.0.jar
INFO:apache_beam.utils.subprocess_server:Starting service with ['java' '-jar' '/root/.apache_beam/cache/jars/beam-sdks-java-extensions-sql-expansion-service-2.63.0.jar' '35881' '--filesToStage=/root/.apache_beam/cache/jars/beam-sdks-java-extensions-sql-expansion-service-2.63.0.jar' '--alsoStartLoopbackWorker']
INFO:apache_beam.utils.subprocess_server:Starting expansion service at localhost:35881
INFO:apache_beam.utils.subprocess_server:Mar 19, 2025 4:24:41 AM org.apache.beam.sdk.expansion.service.ExpansionService loadRegisteredTransforms
INFO:apache_beam.utils.subprocess_server:INFO: Registering external transforms: [beam:transform:org.apache.beam:spanner_insert_or_update:v1, beam:directrunner:transforms:test_stream:v1, beam:transform:org.apache.beam:pubsublite_write:v1, beam:transform:org.apache.beam:kafka_write:v1, beam:transform:combine_grouped_values:v1, beam:transform:combine_globally:v1, beam:external:java:generate_sequence:v1, beam:schematransform:org.apache.beam:bigquery_storage_read:v1, beam:transform:redistribute_by_key:v1, beam:directrunner:transforms:gabw:v1, beam:transform:window_into:v1, beam:transform:org.apache.beam:spanner_update:v1, beam:transform:org.apache.beam:pubsub_write:v1, beam:schematransform:org.apache.beam:kafka_read:v1, beam:schematransform:org.apache.beam:kafka_write:v1, beam:transform:combine_per_key:v1, beam:schematransform:org.apache.beam:bigquery_write:v1, beam:transform:org.apache.beam:kafka_read_with_metadata:v1, beam:transform:group_by_key:v1, beam:transform:pubsub_read:v1, beam:transform:org.apache.beam:spanner_delete:v1, beam:transform:group_into_batches:v1, beam:transform:org.apache.beam:spanner_read:v1, beam:transform:pubsub_write:v1, beam:transform:org.apache.beam:spanner_insert:v1, beam:transform:group_into_batches_with_sharded_key:v1, beam:transform:pubsub_write:v2, beam:transform:create_view:v1, beam:transform:teststream:v1, beam:transform:sdf_process_keyed_elements:v1, beam:transform:org.apache.beam:pubsub_read:v1, beam:external:java:sql:v1, beam:directrunner:transforms:write_view:v1, beam:transform:org.apache.beam:spanner_replace:v1, beam:directrunner:transforms:gbko:v1, beam:transform:org.apache.beam:pubsublite_read:v1, beam:transform:org.apache.beam:bigquery_write:v1, beam:transform:impulse:v1, beam:transform:flatten:v1, beam:transform:write_files:v1, beam:runners_core:transforms:splittable_process:v1, beam:directrunner:transforms:stateful_pardo:v1, beam:transform:org.apache.beam:bigquery_read:v1, beam:transform:org.apache.beam:kafka_read_without_metadata:v1, beam:transform:reshuffle:v1, beam:transform:redistribute_arbitrarily:v1]
INFO:apache_beam.utils.subprocess_server:
INFO:apache_beam.utils.subprocess_server:Registered transforms:
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:spanner_insert_or_update:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@22ffa91a
INFO:apache_beam.utils.subprocess_server:	beam:directrunner:transforms:test_stream:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@74960bfa
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:pubsublite_write:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@42721fe
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:kafka_write:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@40844aab
INFO:apache_beam.utils.subprocess_server:	beam:transform:combine_grouped_values:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@1f6c9cd8
INFO:apache_beam.utils.subprocess_server:	beam:transform:combine_globally:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@5b619d14
INFO:apache_beam.utils.subprocess_server:	beam:external:java:generate_sequence:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@66746f57
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:bigquery_storage_read:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@447a020
INFO:apache_beam.utils.subprocess_server:	beam:transform:redistribute_by_key:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@7f36662c
INFO:apache_beam.utils.subprocess_server:	beam:directrunner:transforms:gabw:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@28e8dde3
INFO:apache_beam.utils.subprocess_server:	beam:transform:window_into:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@6d23017e
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:spanner_update:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@54dcfa5a
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:pubsub_write:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@1817f1eb
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:kafka_read:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@767e20cf
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:kafka_write:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@3a3e78f
INFO:apache_beam.utils.subprocess_server:	beam:transform:combine_per_key:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@301ec38b
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:bigquery_write:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@17a1e4ca
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:kafka_read_with_metadata:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@10ded6a9
INFO:apache_beam.utils.subprocess_server:	beam:transform:group_by_key:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@c5dc4a2
INFO:apache_beam.utils.subprocess_server:	beam:transform:pubsub_read:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@4a194c39
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:spanner_delete:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@52066604
INFO:apache_beam.utils.subprocess_server:	beam:transform:group_into_batches:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@340b9973
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:spanner_read:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@56113384
INFO:apache_beam.utils.subprocess_server:	beam:transform:pubsub_write:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@5669c5fb
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:spanner_insert:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@373ebf74
INFO:apache_beam.utils.subprocess_server:	beam:transform:group_into_batches_with_sharded_key:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@5f9678e1
INFO:apache_beam.utils.subprocess_server:	beam:transform:pubsub_write:v2: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@c4ed84
INFO:apache_beam.utils.subprocess_server:	beam:transform:create_view:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@189aa67a
INFO:apache_beam.utils.subprocess_server:	beam:transform:teststream:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@5a9d6f02
INFO:apache_beam.utils.subprocess_server:	beam:transform:sdf_process_keyed_elements:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@362045c0
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:pubsub_read:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@112f364d
INFO:apache_beam.utils.subprocess_server:	beam:external:java:sql:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@5ccbeb64
INFO:apache_beam.utils.subprocess_server:	beam:directrunner:transforms:write_view:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@6d9f7a80
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:spanner_replace:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@59252cb6
INFO:apache_beam.utils.subprocess_server:	beam:directrunner:transforms:gbko:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@31d0e481
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:pubsublite_read:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@3243b914
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:bigquery_write:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@241e8ea6
INFO:apache_beam.utils.subprocess_server:	beam:transform:impulse:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@542e560f
INFO:apache_beam.utils.subprocess_server:	beam:transform:flatten:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@626c44e7
INFO:apache_beam.utils.subprocess_server:	beam:transform:write_files:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@4dc8caa7
INFO:apache_beam.utils.subprocess_server:	beam:runners_core:transforms:splittable_process:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@1d730606
INFO:apache_beam.utils.subprocess_server:	beam:directrunner:transforms:stateful_pardo:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@3bcbb589
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:bigquery_read:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@3b00856b
INFO:apache_beam.utils.subprocess_server:	beam:transform:org.apache.beam:kafka_read_without_metadata:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForBuilder@3016fd5e
INFO:apache_beam.utils.subprocess_server:	beam:transform:reshuffle:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@35d08e6c
INFO:apache_beam.utils.subprocess_server:	beam:transform:redistribute_arbitrarily:v1: org.apache.beam.sdk.expansion.service.ExpansionService$TransformProviderForPayloadTranslator@53d102a2
INFO:apache_beam.utils.subprocess_server:
INFO:apache_beam.utils.subprocess_server:Registered SchemaTransformProviders:
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:bigquery_fileloads:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:filter-java:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:flatten:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:spanner_cdc_read:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:map_to_fields-java:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:bigtable_write:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:spanner_write:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:log_for_testing:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:pubsublite_read:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:bigquery_storage_read:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:spanner_read:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:bigquery_export_read:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:kafka_read:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:kafka_write:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:bigquery_storage_write:v2
INFO:apache_beam.utils.subprocess_server:	schematransform:org.apache.beam:sql_transform:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:pubsub_read:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:pubsub_write:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:window_into_strategy:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:bigtable_read:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:bigquery_write:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:pubsublite_write:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:generate_sequence:v1
INFO:apache_beam.utils.subprocess_server:	beam:schematransform:org.apache.beam:yaml:explode:v1
INFO:root:Starting a JAR-based expansion service from JAR /root/.apache_beam/cache/jars/beam-sdks-java-extensions-sql-expansion-service-2.63.0.jar
INFO:apache_beam.utils.subprocess_server:Mar 19, 2025 4:24:41 AM org.apache.beam.sdk.expansion.service.ExpansionService expand
INFO:apache_beam.utils.subprocess_server:INFO: Expanding 'LogForTesting/beam:schematransform:org.apache.beam:yaml:log_for_testing:v1' with URN 'beam:expansion:payload:schematransform:v1'
INFO:apache_beam.runners.worker.statecache:Creating state cache with size 104857600
INFO:apache_beam.runners.portability.fn_api_runner.worker_handlers:starting control server on port 36367
INFO:apache_beam.runners.portability.fn_api_runner.worker_handlers:starting data server on port 43161
INFO:apache_beam.runners.portability.fn_api_runner.worker_handlers:starting state server on port 33983
INFO:apache_beam.runners.portability.fn_api_runner.worker_handlers:starting logging server on port 38047
INFO:apache_beam.runners.portability.fn_api_runner.worker_handlers:Requesting worker at localhost:55249
INFO:apache_beam.utils.subprocess_server:[grpc-default-executor-1] INFO org.apache.beam.fn.harness.ExternalWorkerService - Starting worker worker_1 pointing at fb93d15de829:36367.
INFO:apache_beam.runners.portability.fn_api_runner.worker_handlers:Requesting worker at localhost:35881
INFO:apache_beam.utils.subprocess_server:Mar 19, 2025 4:24:43 AM org.apache.beam.fn.harness.ExternalWorkerService startWorker
INFO:apache_beam.utils.subprocess_server:INFO: Starting worker worker_2 pointing at fb93d15de829:36367.
INFO:apache_beam.utils.subprocess_server:[SDK-worker-worker_1] INFO org.apache.beam.fn.harness.FnHarness - Fn Harness started
INFO:apache_beam.utils.subprocess_server:[SDK-worker-worker_1] INFO org.apache.beam.fn.harness.FnHarness - Entering instruction processing loop
INFO:root:severity: INFO
timestamp {
  seconds: 1742358284
  nanos: 138000000
}
message: "Fn Harness started"
log_location: "org.apache.beam.fn.harness.FnHarness"
thread: "24"

INFO:root:severity: INFO
timestamp {
  seconds: 1742358284
  nanos: 176000000
}
message: "Running JvmInitializer#beforeProcessing for org.apache.beam.sdk.io.kafka.KafkaIOInitializer@1f957d9d"
log_location: "org.apache.beam.sdk.fn.JvmInitializers"
thread: "24"

INFO:root:severity: INFO
timestamp {
  seconds: 1742358284
  nanos: 176000000
}
message: "Completed JvmInitializer#beforeProcessing for org.apache.beam.sdk.io.kafka.KafkaIOInitializer@1f957d9d"
log_location: "org.apache.beam.sdk.fn.JvmInitializers"
thread: "24"

INFO:root:severity: INFO
timestamp {
  seconds: 1742358284
  nanos: 176000000
}
message: "Entering instruction processing loop"
log_location: "org.apache.beam.fn.harness.FnHarness"
thread: "24"

ERROR:root:severity: ERROR
timestamp {
  seconds: 1742358284
  nanos: 194000000
}
message: "*~*~*~ Previous channel ManagedChannelImpl{logId=9, target=fb93d15de829:36367} was garbage collected without being shut down! ~*~*~*\n    Make sure to call shutdown()/shutdownNow()"
trace: "java.lang.RuntimeException: ManagedChannel allocation site\n\tat org.apache.beam.vendor.grpc.v1p69p0.io.grpc.internal.ManagedChannelOrphanWrapper$ManagedChannelReference.<init>(ManagedChannelOrphanWrapper.java:102)\n\tat org.apache.beam.vendor.grpc.v1p69p0.io.grpc.internal.ManagedChannelOrphanWrapper.<init>(ManagedChannelOrphanWrapper.java:60)\n\tat org.apache.beam.vendor.grpc.v1p69p0.io.grpc.internal.ManagedChannelOrphanWrapper.<init>(ManagedChannelOrphanWrapper.java:51)\n\tat org.apache.beam.vendor.grpc.v1p69p0.io.grpc.internal.ManagedChannelImplBuilder.build(ManagedChannelImplBuilder.java:710)\n\tat org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ForwardingChannelBuilder2.build(ForwardingChannelBuilder2.java:272)\n\tat org.apache.beam.sdk.fn.channel.ManagedChannelFactory.forDescriptor(ManagedChannelFactory.java:101)\n\tat org.apache.beam.fn.harness.ExternalWorkerService.startWorker(ExternalWorkerService.java:83)\n\tat org.apache.beam.model.fnexecution.v1.BeamFnExternalWorkerPoolGrpc$MethodHandlers.invoke(BeamFnExternalWorkerPoolGrpc.java:296)\n\tat org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.ServerCalls$UnaryServerCallHandler$UnaryServerCallListener.onHalfClose(ServerCalls.java:182)\n\tat org.apache.beam.vendor.grpc.v1p69p0.io.grpc.internal.ServerCallImpl$ServerStreamListenerImpl.halfClosed(ServerCallImpl.java:356)\n\tat org.apache.beam.vendor.grpc.v1p69p0.io.grpc.internal.ServerImpl$JumpToApplicationThreadServerStreamListener$1HalfClosed.runInContext(ServerImpl.java:861)\n\tat org.apache.beam.vendor.grpc.v1p69p0.io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)\n\tat org.apache.beam.vendor.grpc.v1p69p0.io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:133)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)\n\tat java.base/java.lang.Thread.run(Thread.java:840)\n"
instruction_id: "bundle_3"
log_location: "org.apache.beam.vendor.grpc.v1p69p0.io.grpc.internal.ManagedChannelOrphanWrapper"
thread: "35"

INFO:root:severity: INFO
timestamp {
  seconds: 1742358284
  nanos: 496000000
}
message: "{\"display_name\":\"NAME 1\"}"
instruction_id: "bundle_4"
transform_id: "LogForTesting/beam:schematransform:org.apache.beam:yaml:log_for_testing:v1/LogAsJson/ParMultiDo(Anonymous)"
log_location: "org.apache.beam.sdk.schemas.transforms.providers.LoggingTransformProvider$LoggingTransform"
thread: "34"

INFO:root:severity: INFO
timestamp {
  seconds: 1742358284
  nanos: 497000000
}
message: "{\"display_name\":\"NAME 2\"}"
instruction_id: "bundle_4"
transform_id: "LogForTesting/beam:schematransform:org.apache.beam:yaml:log_for_testing:v1/LogAsJson/ParMultiDo(Anonymous)"
log_location: "org.apache.beam.sdk.schemas.transforms.providers.LoggingTransformProvider$LoggingTransform"
thread: "34"

INFO:apache_beam.utils.subprocess_server:[grpc-default-executor-1] INFO org.apache.beam.sdk.fn.data.BeamFnDataGrpcMultiplexer - Hanged up for url: "fb93d15de829:43161"
INFO:apache_beam.utils.subprocess_server:.
INFO:root:severity: INFO
timestamp {
  seconds: 1742358284
  nanos: 509000000
}
message: "Hanged up for url: \"fb93d15de829:43161\"\n."
log_location: "org.apache.beam.sdk.fn.data.BeamFnDataGrpcMultiplexer"
thread: "18"

INFO:apache_beam.utils.subprocess_server:[SDK-worker-worker_1] INFO org.apache.beam.fn.harness.FnHarness - Shutting SDK harness down.
INFO:apache_beam.utils.subprocess_server:[SDK-worker-worker_1] INFO org.apache.beam.fn.harness.ExternalWorkerService - Successfully started worker worker_1.
INFO:apache_beam.utils.subprocess_server:Mar 19, 2025 4:24:45 AM org.apache.beam.fn.harness.FnHarness main
INFO:apache_beam.utils.subprocess_server:INFO: Shutting SDK harness down.
INFO:apache_beam.utils.subprocess_server:Mar 19, 2025 4:24:45 AM org.apache.beam.fn.harness.ExternalWorkerService lambda$startWorker$0
INFO:apache_beam.utils.subprocess_server:INFO: Successfully started worker worker_2.
Building pipeline...
Running pipeline...
```

</details>

## Run it on Dataflow and observe it fails

```shell
run_pipeline_on_dataflow.sh <network-name> <job-name> pipeline.yaml
```

- Calls `gcloud dataflow yaml run`.

### ❌ Fails with a Python error

Note the source error seems to be `TypeError: a bytes-like object is required, not 'str'` coming from a URL scheme `TypeError` created at this line: https://github.com/python/cpython/blob/f576d31d1ca3f91a5e47ee5bb36582bf5cfa39a9/Lib/urllib/parse.py#L495, if I have the file correct.

<details>
<summary>Dataflow output</summary>

```
INFO:apache_beam.yaml.yaml_transform:Expanding "Create" at line 5
INFO:apache_beam.yaml.yaml_transform:Expanding "Identity" at line 12
Building pipeline...
Traceback (most recent call last):
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 371, in create_ptransform
ptransform = provider.create_transform(
^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_provider.py", line 192, in create_transform
self._service = self._service()
^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_provider.py", line 328, in <lambda>
urns, lambda: external.JavaJarExpansionService(jar_provider()))
^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_provider.py", line 260, in <lambda>
urns, lambda: _join_url_or_filepath(provider_base_path, jar))
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_provider.py", line 1282, in _join_url_or_filepath
path_scheme = urllib.parse.urlparse(path, base_scheme).scheme
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/urllib/parse.py", line 395, in urlparse
splitresult = urlsplit(url, scheme, allow_fragments)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/urllib/parse.py", line 478, in urlsplit
scheme = scheme.strip(_WHATWG_C0_CONTROL_OR_SPACE)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
TypeError: a bytes-like object is required, not 'str'
The above exception was the direct cause of the following exception:
Traceback (most recent call last):
File "/template/main.py", line 31, in <module>
run()
File "/template/main.py", line 25, in run
main.run(args)
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/main.py", line 143, in run
yaml_transform.expand_pipeline(
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 1077, in expand_pipeline
providers or {})).expand(beam.pvalue.PBegin(pipeline))
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 1042, in expand
result = expand_transform(
^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 442, in expand_transform
return expand_composite_transform(spec, scope)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 520, in expand_composite_transform
return CompositePTransform.expand(None)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 508, in expand
inner_scope.compute_all()
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 196, in compute_all
self.compute_outputs(transform_id)
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 97, in wrapper
self._cache[key] = func(self, *args)
^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 232, in compute_outputs
return expand_transform(self._transforms_by_uuid[transform_id], self)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 444, in expand_transform
return expand_leaf_transform(spec, scope)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 466, in expand_leaf_transform
ptransform = scope.create_ptransform(spec, inputs_dict.values())
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/lib/python3.11/site-packages/apache_beam/yaml/yaml_transform.py", line 413, in create_ptransform
raise ValueError(
ValueError: Invalid transform specification at "Identity" at line 12: a bytes-like object is required, not 'str'
python failed with exit status 1
Error: Template launch failed: exit status 1
```

</details>

End of updated README for repro.

---

# Creating a Beam Java Transform Catalog and Using in Beam YAML

<!-- TOC -->

- [Creating a Beam Java Transform Catalog and Using in Beam YAML](#creating-a-beam-java-transform-catalog-and-using-in-beam-yaml)
  - [Prerequisites](#prerequisites)
  - [Overview](#overview)
  - [Project Structure](#project-structure)
  - [Creating the pom.xml file](#creating-the-pomxml-file)
    - [Minimal pom.xml](#minimal-pomxml)
  - [Writing the External Transform](#writing-the-external-transform)
    - [SchemaTransformProvider Skeleton Code](#schematransformprovider-skeleton-code)
      - [configurationClass()](#configurationclass)
      - [identifier()](#identifier)
      - [inputCollectionNames()](#inputcollectionnames)
      - [outputCollectionNames()](#outputcollectionnames)
      - [from()](#from)
      - [description()](#description)
    - [ToUpperCaseProvider Configuration Class](#touppercaseprovider-configuration-class)
      - [Error Handling](#error-handling)
      - [Validation](#validation)
    - [ToUpperCaseProvider SchemaTransform Class](#touppercaseprovider-schematransform-class)
  - [Building the Transform Catalog JAR](#building-the-transform-catalog-jar)
  - [Defining the Transform in Beam YAML](#defining-the-transform-in-beam-yaml)
  <!-- TOC -->

## Prerequisites

To complete this tutorial, you must have the following software installed:

- Java 11 or later
- Apache Maven 3.6 or later

## Overview

The purpose of this tutorial is to introduce the fundamental concepts of the
[Cross-Language](https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines) framework that is
leveraged by [Beam YAML](https://beam.apache.org/documentation/sdks/yaml/) to allow a user to specify Beam Java
transforms through the use of transform [providers](https://beam.apache.org/documentation/sdks/yaml/#providers) such
that the transforms can be easily defined in a Beam YAML pipeline.

As we walk through these concepts, we will be constructing a Transform called `ToUpperCase` that will take in a single
parameter `field`, which represents a field in the collection of elements, and modify that field by converting the
string to uppercase.

There are four main steps to follow:

1. Define the transformation itself as a
   [PTransform](https://beam.apache.org/documentation/programming-guide/#composite-transforms)
   that consumes and produces any number of schema'd PCollections.
2. Expose this transform via a
   [SchemaTransformProvider](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html)
   which provides an identifier used to refer to this transform later as well
   as metadata like a human-readable description and its configuration parameters.
3. Build a Jar that contains these classes and vends them via the
   [Service Loader](https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/ToUpperCaseTransformProvider.java#L30)
   infrastructure.
4. Write a [provider specification](https://beam.apache.org/documentation/sdks/yaml/#providers)
   that tells Beam YAML where to find this jar and what it contains.

## Project Structure

The project structure for this tutorial is as follows:

```
MyExternalTransforms
├── pom.xml
└── src
    └── main
        └── java
            └── org
                └── example
                    ├── ToUpperCaseTransformProvider.java
                    └── SkeletonSchemaProvider.java
```

Here is a brief description of each file:

- **pom.xml:** The Maven project configuration file.
- **SkeletonSchemaProvider.java:** The Java class that contains the bare-minimum skeleton code for implementing a
  `SchemaTransform` Identity function.
- **ToUpperCaseTransformProvider.java:** The Java class that contains the `SchemaTransform` to be used in the Beam YAML
  pipeline. This project structure assumes that the java module is `org.example`, but any module path can be used so long
  as the project structure matches.

## Creating the pom.xml file

A `pom.xml` file, which stands for Project Object Model, is an essential file used in Maven projects. It's an XML file
that contains all the critical information about a project, including its configuration details for Maven to build it
successfully. This file specifies things like the project's name, version, dependencies on other libraries, and how
the project should be packaged (e.g., JAR file).

Since this tutorial won’t cover all the details about Maven and `pom.xml`, here's a link to the official documentation
for more details: https://maven.apache.org/pom.html

### Minimal pom.xml

A minimal `pom.xml` file can be found in the project repo [here](pom.xml).

## Writing the External Transform

Writing a transform that is compatible with the Beam YAML framework requires leveraging Beam’s
[cross-language](https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines) framework, and more
specifically, the <code> [SchemaTransformProvider](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html)</code>
interface (and even _more_ specifically, the <code> [TypedSchemaTransformProvider](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/TypedSchemaTransformProvider.html)</code>
interface).

This framework relies on creating a
<code>[PTransform](https://beam.apache.org/documentation/programming-guide/#transforms)</code> that operates solely on
<code>[Beam Row](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/Row.html)</code>’s -
a schema-aware data type built into Beam that is capable of being translated across SDK’s. Leveraging the
<code>[SchemaTransformProvider](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html)</code>
interface removes the need to write a lot of the boilerplate code required to translate data across the SDK’s,
allowing us to focus on the transform functionality itself.

### SchemaTransformProvider Skeleton Code

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/SkeletonSchemaProvider.java#L1-L96

This is the bare minimum code (excluding import and package) to create a `SchemaTransformProvider` that can be used by
the cross-language framework, and therefore allowing the `SchemaTransform` defined within it to be defined in any
Beam YAML pipeline. In this case, the transform will act as an Identity function and will output the input collection
of elements with no alteration.

Let’s start by breaking down the top-level methods that are required to be compatible with the
`SchemaTransformProvider` interface.

#### configurationClass()

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/SkeletonSchemaProvider.java#L27-L30

The <code>[configurationClass()](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#configurationSchema--)</code>
method is responsible for telling the cross-language framework which Java class defines the input parameters to
the Transform. The <code>Configuration</code> class we defined in the skeleton code will be revisited in depth later.

#### identifier()

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/SkeletonSchemaProvider.java#L32-L35

The <code>[identifier()](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#identifier--)</code>
method defines a unique identifier for the Transform. It is important to ensure that this name does not collide with
any other Transform URN that will be given to the External Transform service, both those built-in to Beam, and any that
are defined in a custom catalog such as this.

#### inputCollectionNames()

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/SkeletonSchemaProvider.java#L42-L45

The <code>[inputCollectionNames()](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#inputCollectionNames--)</code>
method returns a list of expected input names for the tagged input collections. In most cases, especially in Beam YAML,
there will be a collection of input elements that are tagged “input”. It is acceptable to use different names
in this method as it is not actually used as a contract between SDK's, but what is important to note is that Beam YAML
will be sending a collection that is tagged "input" to the transform. It is best to use this method definition with
the macros defined at the top of the file.

#### outputCollectionNames()

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/SkeletonSchemaProvider.java#L47-L50

The <code>[outputCollectionNames()](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#outputCollectionNames--)</code>
method returns a list of output names for the tagged output collections. Similar to
<code>inputCollectionNames()</code>, there will be a collection of output elements that are tagged “output”, but in
most cases, there will also be a subset of elements tagged with whatever was defined in the
<code>[error_handling](https://beam.apache.org/documentation/sdks/yaml-errors/)</code> section of the transform config
in Beam YAML. Since this method is also not used as a contract, it is best to use this method definition with
the macros defined at the top of the file.

#### from()

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/SkeletonSchemaProvider.java#L52-L55

The <code>[from()](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#from-org.apache.beam.sdk.values.Row-)</code>
method is the method that is responsible for returning the
<code>[SchemaTransform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransform.html)</code>
itself. This transform is the
<code>[PTransform](https://beam.apache.org/documentation/programming-guide/#transforms)</code> that will actually
perform the transform on the incoming collection of elements. Since it is a PTransform, it requires one method -
<code>[expand()](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/PTransform.html#expand-InputT-)</code>
which defines the expansion of the transform and includes the
<code>[DoFn](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.html)</code>.

#### description()

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/SkeletonSchemaProvider.java#L37-L40

The _optional_ <code>[description()](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#description–)</code>
method is where a description of the transform can be written. This description is largely unused by the Beam YAML
framework, but is useful for generating docs when used in conjunction with the
[generate_yaml_docs.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/generate_yaml_docs.py)
script. This is useful when generating docs for a transform catalog. For example, the
[Beam YAML transform glossary](https://beam.apache.org/releases/yamldoc/current/).

### ToUpperCaseProvider Configuration Class

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/ToUpperCaseTransformProvider.java#L72-L100

The `Configuration` class is responsible for defining the parameters to the transform. This
<code>[AutoValue](https://github.com/google/auto/tree/main/value)</code> class is annotated with the
<code>[AutoValueSchema](https://beam.apache.org/releases/javadoc/2.29.0/org/apache/beam/sdk/schemas/AutoValueSchema.html)</code> interface to
generate the schema for the transform. This interface scrapes the inputs by using all the getter methods. In our
<code>ToUpperCase</code> example, there is one input parameter, <code>field</code>, that specifies the field in the
input collection to perform the operation on. So, we define a getter method, <code>getField</code>, that will tell the
<code>AutoValueSchema</code> class about our input parameter.

Likewise, the `Configuration` class needs a `Builder` subclass annotated with
<code>[AutoValue.Builder](https://github.com/google/auto/blob/main/value/userguide/builders.md)</code> so that the
<code>AutoValue</code> class can be instantiated. This builder needs a setter method for each subsequent getter method
in the parent class.

Optional parameters should be annotated with the `@Nullable` annotation. Required parameters, therefore, should omit
this annotation.

The `@SchemaFieldDescription` annotation can also be optionally used to define the parameter. This is also passed to
the Beam YAML framework and can be used in conjunction with the
[generate_yaml_docs.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/generate_yaml_docs.py)
script to generate docs for the transform. This is useful when generating docs for a transform catalog. For example,
the [Beam YAML transform glossary](https://beam.apache.org/releases/yamldoc/current/).

#### Error Handling

In Beam YAML, there is a built-in [error handling](https://beam.apache.org/documentation/sdks/yaml-errors/) framework
that allows a transform to consume the error output from a transform, both in the turnkey Beam YAML transforms and in
compatible External Transforms. This error output could be any `Exceptions` that are caught and tagged, or any custom
logic that would cause an element to be treated as an error.

To make a transform compatible with this framework, one must take the
<code>[ErrorHandling](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/providers/ErrorHandling.html)</code>
object as an input to the transform, and therefore define it in this `Configuration`.

#### Validation

One last optional method for the `Configuration` class is the `validate()` method. This method is responsible for
checking the input parameters to the transform. In our example, we assume there is a field `metadata` in the input
collection that cannot be modified. So, we perform a check on the `field` parameter to verify the user is not
attempting to modify this field. This method can also be useful for checking dependent inputs, (i.e. parameter A is
required _if_ parameter B is specified).

### ToUpperCaseProvider SchemaTransform Class

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/ToUpperCaseTransformProvider.java#L102-L179

This is the class that will define the actual transform that is performed on the incoming
<code>[PCollection](https://beam.apache.org/documentation/programming-guide/#pcollections)</code>. Let’s first take a
look at the <code>expand()</code> function.

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/ToUpperCaseTransformProvider.java#L141-L177

Every incoming
<code>[PCollectionRowTuple](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/PCollectionRowTuple.html)</code> is
essentially a tagged collection. As stated before, in most cases, with context to Beam YAML, there will only be one
tag, “input”. To get the <code>PCollection</code> of <code>Row </code>elements, we first need to unpack these from
the <code>PCollectionRowTuple</code> using the “input” tag.

From this collection of elements, the schema of the input collection can be obtained. This is useful for assembling our
output collections, since their schema will be based on this schema. In the case of the successful records, the schema
will remain unchanged (since we are modifying a single field in-place), and the error records will use a schema that
essentially wraps the original schema with a couple error-specific fields as defined by
<code>[errorSchema](https://github.com/apache/beam/blob/f4d03d49713cf89260c141ee35b4dadb31ad4193/sdks/java/core/src/main/java/org/apache/beam/sdk/schemas/transforms/providers/ErrorHandling.java#L50-L54)</code>.

Whether to do <code>[error_handling](https://beam.apache.org/documentation/sdks/yaml-errors/)</code> is
determined by the [ErrorHandling](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/providers/ErrorHandling.html)
class. If error handling is specified in the config of the transform in the Beam YAML pipeline, then exceptions will be
caught and stored in an “error”-tagged output as opposed to thrown at Runtime.

Next, the <code>[PTransform](https://beam.apache.org/documentation/programming-guide/#transforms)</code> is actually
applied. The
<code>[DoFn](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.html)</code> will be
detailed more below, but what is important to note here is that the output is tagged using two arbitrary
<code>[TupleTag](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/TupleTag.html)</code>
objects - one for successful records, and one for error records.

After the `PTransform` is applied, it is important to ensure that both output collections have their schema configured
so that the cross-language service can encode and decode the objects back to the Beam YAML SDK.

Finally, the resulting `PCollectionRowTuple` must be constructed. The successful records should be stored and tagged
“output” regardless of if `error_handling` was specified, and if `error_handling` was specified, it can be appended to
the same `PCollectionRowTuple `and tagged according to the output specified by the `error_handling `config in the
Beam YAML pipeline.

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/src/main/java/org/example/ToUpperCaseTransformProvider.java#L116-L139

Now, taking a look at the `createDoFn()` method that is responsible for constructing and returning the actual
<code>[DoFn](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.html)</code>,
you will notice that the only thing that makes this special versus any other <code>DoFn</code> you may write, is that
it checks to see if <code>error_handling </code>was specified, constructs an error record from the input records and
tags those records using the arbitrary error <code>TupleTag</code> discussed previously. Aside from that, this
<code>DoFn</code> is simply taking the input <code>Row</code>, applying the <code>.toUpperCase()</code> method to the
<code>field</code> within the <code>Row</code> and tagging all successful records using the arbitrary successful
<code>TupleTag </code>also discussed previously.

## Building the Transform Catalog JAR

At this point, you should have all the code as defined in the previous section ready to go, and it is now time to build
the JAR that will be provided to the Beam YAML pipeline.

From the root directory, run the following command:

```
mvn package
```

This will create a JAR under `target` called `xlang-transforms-bundled-1.0-SNAPSHOT.jar` that contains the
`ToUpperCaseTransformProvider` along with its dependencies and the external transform service. The external expansion
service is what will be invoked by the Beam YAML SDK to import the transform schema and run the expansion service for
the transform.

https://github.com/Polber/beam-yaml-xlang/blob/95abf0864e313232a89f3c9e57b950d0fb478979/pom.xml#L85-L87
**Note:** The name of the jar is configurable using the `finalName` tag in the `maven-shade-plugin` configuration.

## Defining the Transform in Beam YAML

Now that you have a JAR file that contains the transform catalog, it is time to include it as part of your Beam YAML
pipeline. This is done using <code>[providers](https://beam.apache.org/documentation/sdks/yaml/#providers)</code> -
these providers allow one to define a suite of transforms in a given JAR or python package that can be used within the
Beam YAML pipeline.

We will be utilizing the `javaJar` provider as we are planning to keep the names of the config parameters as they are defined in the transform.

For our example, that looks as follows:

```yaml
providers:
  - type: javaJar
    config:
      jar: xlang-transforms-bundled-1.0-SNAPSHOT.jar
    transforms:
      ToUpperCase: "some:urn:to_upper_case:v1"
      Identity: "some:urn:transform_name:v1"
```

For transforms where one might want to rename the config parameters, the `renaming` provider allows us to map the transform
parameters to alias names. Otherwise the config parameters will inherit the same name that is defined in Java, with camelCase
being converted to snake_case. For example, errorHandling will be called `error_handling` in the YAML config. If there was a
parameter `table_spec`, and we wanted to call in `table` in the YAML config. We could use the `renaming` provider to map the alias.

More robust examples of the `renaming` provider can be found [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/standard_providers.yaml).

Now, `ToUpperCase` can be defined as a transform in the Beam YAML pipeline with the single config parameter - `field`.

A full example:

```yaml
pipeline:
  type: chain
  transforms:
    - type: Create
      config:
        elements:
          - name: "john"
            id: 1
          - name: "jane"
            id: 2
    - type: Identity
    - type: ToUpperCase
      config:
        field: "name"
    - type: LogForTesting

providers:
  - type: javaJar
    config:
      jar: xlang-transforms-bundled-1.0-SNAPSHOT.jar
    transforms:
      ToUpperCase: "some:urn:to_upper_case:v1"
      Identity: "some:urn:transform_name:v1"
```

Expected logs:

```
message: "{\"name\":\"JOHN\",\"id\":1}"
message: "{\"name\":\"JANE\",\"id\":2}"
```

**Note**: Beam YAML will choose the Java implementation of the `LogForTesting` transform to reduce language switching.
The output can get a bit crowded, but look for the logs in the commented “Expected” section at the bottom of the YAML
file.

An example with errors caught and handled:

```yaml
pipeline:
  transforms:
    - type: Create
      config:
        elements:
          - name: "john"
            id: 1
          - name: "jane"
            id: 2
    - type: ToUpperCase
      input: Create
      config:
        field: "unknown"
        error_handling:
          output: errors
    - type: LogForTesting
      input: ToUpperCase
    - type: LogForTesting
      input: ToUpperCase.errors

providers:
  - type: javaJar
    config:
      jar: xlang-transforms-bundled-1.0-SNAPSHOT.jar
    transforms:
      ToUpperCase: "some:urn:to_upper_case:v1"
      Identity: "some:urn:transform_name:v1"
```

If you have Beam Python installed, you can test this pipeline out locally with

```
python -m apache_beam.yaml.main --yaml_pipeline_file=pipeline.yaml
```

or if you have gcloud installed you can run this on dataflow with

```
gcloud dataflow yaml run $JOB_NAME --yaml-pipeline-file=pipeline.yaml --region=$REGION
```

(Note in this case you will need to upload your jar to a gcs bucket or
publish it elsewhere as a globally-accessible URL so it is available to
the dataflow service.)
