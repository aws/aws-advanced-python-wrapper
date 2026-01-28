# Monitoring

Monitoring is the ability to gather data and insights on the execution of an application. Users will also be able to inspect the gathered data and determine potential actions to take depending on the data collected.
The AWS Advanced Python Wrapper has a Telemetry feature allowing you to collect and visualize data of the AWS Advanced Python Wrapper execution at a global level and at plugin level.
You can now monitor the performance of the wrapper as a whole or within specific plugins with your configurations, and determine whether the wrapper's performance meets your expectations.

## Terminology

The AWS Advanced Python Wrapper provides telemetry data through two different forms: **Traces** and **Metrics**.

### Traces

Traces give an overview of what is happening in a specific section of the execution of an application.
A trace is composed by a hierarchical sequence of segments, each of which contain basic information about the execution (e.g., duration), and whether that section was executed successfully or not.

In the AWS Advanced Python Wrapper, initially a trace will be generated for every method call made to the wrapper. Depending on whether the user application has already a trace open, it might be either nested into the opened trace or dropped. And then, for each enabled plugin, another segment will be created only for the plugin execution, linked to the
method call segment.

Traces from the AWS Advanced Python Wrapper are submitted to [**AWS X-Ray**](https://aws.amazon.com/xray/).

### Metrics

Metrics are numeric data that were measured and collected through the execution of an application. Those metrics can give an insight on how many times some action (e.g., failover) has happened, and for actions that may happen multiple
times, their success or failure rate (failover, cache hits, etc.), amongst other related information.

The AWS Advanced Python Wrapper will submit metrics data to [**Amazon Cloudwatch**](https://aws.amazon.com/cloudwatch/).

The list of available metrics for the AWS Advanced Python Wrapper and its plugins is available in the [List of Metrics](#List-Of-Metrics) section of this page.

## Setting up the AWS Distro for OpenTelemetry Collector (ADOT Collector)

## Prerequisites

Before enabling the Telemetry feature, a few setup steps are required to ensure the monitoring data gets properly emitted.

1. In order to visualize the telemetry data in the AWS Console, make sure you have an IAM user or role with permissions
   to [AWS X-Ray](https://docs.aws.amazon.com/xray/latest/devguide/security-iam.html)
   and [Amazon CloudWatch](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/auth-and-access-control-cw.html).

2. Download the [AWS Distro for OpenTelemetry Collector](https://aws-otel.github.io/docs/getting-started/collector) and set it up. The AWS Distro for OpenTelemetry Collector is responsible from receiving telemetry data from the application using the AWS Advanced Python Wrapper and forward it to AWS. Both of those connections happen via HTTP, therefore URLs and ports need to be correctly configured for the collector.

> [!WARNING]
> - The AWS Distro for OpenTelemetry Collector can be set up either locally or remotely. It is up to the user to decide
> where is best to set it up. If you decide to host it remotely, ensure that the application has the necessary permissions
> or allowlists to connect to the Collector.
> - The collector is an external application that is not part of the wrapper itself. Without a collector, the wrapper will
> collect monitoring data from its execution but that data will not be sent anywhere for visualization.

## Using Telemetry

Telemetry for the AWS Advanced Python Wrapper is a monitoring strategy that overlooks all plugins enabled in [`plugins`](UsingThePythonWrapper.md#connection-plugin-manager-parameters) and is not a plugin in itself.
Therefore no changes are required in the `plugins` parameter to enable Telemetry.

In order to enable Telemetry in the AWS Advanced Python Wrapper, you need to:

1. Set the `enable_telemetry` property to `True`. You can either set it through Properties or directly in the connection string.

2. Set up the recorders that will export the telemetry data from the code to the ADOT Collector.

Setting up the recorders require to instantiate an `TracerProvider` and a `MeterProvider` in the application code prior to executing the wrapper.
Instantiating the `TracerProvider` requires you to configure the endpoints where traces are being forwarded to, whereas the `MeterProvider` allows you to configure how meters are exported.

The code sample below shows a simple manner to instantiate trace recording in an application using OpenTelemetry.

```python
provider = TracerProvider(id_generator=AwsXRayIdGenerator())
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317"))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
```

To record meters, see this example:
```python
 resource = Resource(attributes={
     SERVICE_NAME: "python_otlp_telemetry_service"
 })

 reader = PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=1000)
 meterProvider = MeterProvider(resource=resource, metric_readers=[reader])
 metrics.set_meter_provider(meterProvider)
```

We also provide more complete sample application using telemetry in the examples folder of this repository.

- [PGOpenTelemetry](.././examples/PGOpenTelemetry.py)
- [PGXRayTelemetry](.././examples/PGXRayTelemetry.py)

### Telemetry Parameters

In addition to the parameter that enables Telemetry, there are other parameters configuring how telemetry data will be forwarded.

| Parameter                    |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | Default Value |
|------------------------------|:-------:|:--------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `enable_telemetry`           | Boolean |    No    | Telemetry will be enabled when this property is set to `True`, otherwise no telemetry data will be gathered during the execution of the wrapper.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `False`       |
| `telemetry_traces_backend`   | String  |    No    | Determines to which backend the gathered tracing data will be forwarded to. Possible values include: `NONE`, `XRAY`, and `OTLP`.<br>`NONE` indicates that the application will collect tracing data but this data will not be forwarded anywhere.<br>`XRAY` indicates that the traces will be collected by the AWS XRay Daemon.<br>`OTLP` indicates that the traces will be collected by the AWS OTEL Collector.                                                                                                                                                                                                                   | `NONE`        |
| `telemetry_metrics_backend`  | String  |    No    | Determines to which backend the gathered metrics data will be forwarded to. Possible values include: `NONE` and `OTLP`.<br>`NONE` indicates that the application will collect metrics data but this data will not be forwarded anywhere.<br>`OTLP` indicates that the metrics will be collected by the AWS OTEL COllector.                                                                                                                                                                                                                                                                                                         | `NONE`        |
| `telemetry_submit_top_level` | Boolean |    No    | By default the wrapper will look for open traces in the users application prior to record telemetry data. If there is a current open trace, the traces created will be attached to that open trace. If not, all telemetry traces created will be top level. Setting the parameter to `False` means that every method call to the wrapper will generate a trace with no direct parent trace attached to it. If there is already an open trace being recorded by the application, no wrapper traces will be created. See the [Nested tracing strategies section](#nested-tracing-strategies-using-amazon-xray) for more information. | `False`       |

## Nested tracing strategies using Amazon XRay

As you could see in the [Telemetry Parameters](#Telemetry-Parameters) section, the AWS Advanced Python Wrapper allows a user to determine which strategy for nested traces to use when using Telemetry.

Traces are hierarchical entities, and it might be that the user application already has an open trace in a given sequence of code that connects to the AWS Advanced Python Wrapper.
In this case, the Telemetry feature allows users to determine which strategy to use for the Telemetry traces generated when using the wrapper.

A top level trace is a trace that has no link to any other parent trace, and is directly accessible from the list of submitted traces within XRay. In the following pictures, the top level traces of an application are displayed in AWS X-Ray.

<div style="text-align:center"><img src="../images/telemetry_nested.png" /></div>

<div style="text-align:center"><img src="../images/telemetry_traces.png" /></div>

When a trace is hierarchically linked to a parent trace, we say that this trace is nested. An example of nested traces are the individual plugin traces for a given method call.
All the individual plugin traces are linked to a parent trace for the method call. Those nested traces are illustrated in the image below.

<div style="text-align:center"><img src="../images/telemetry_toplevel.png" /></div>

## List of Metrics

The AWS Advanced Python Wrapper also submits a set of metrics to Amazon Cloudwatch when the wrapper is used.
These metrics are predefined, and they help give insight on what is happening inside the plugins when the plugins are used.

Metrics can be one of 3 types: counters, gauges or histograms.

### Host Monitoring Plugin

| Metric name                                 | Metric type | Description                                                                                           |
|---------------------------------------------|-------------|-------------------------------------------------------------------------------------------------------|
| host_monitoring.connections.aborted         | Counter     | Number of times a connection was aborted after being defined as unhealthy by a host monitoring thread |
| host_monitoring.host_unhealthy.count.[HOST] | Counter     | Number of times a specific host has been defined as unhealthy                                         |

### Secrets Manager Plugin

| Metric name                             | Metric type | Description                                                   |
|-----------------------------------------|-------------|---------------------------------------------------------------|
| secrets_manager.fetch_credentials.count | Counter     | Number of times credentials were fetched from Secrets Manager |

### IAM Authentication Plugin

| Metric name           | Metric type | Description                                  |
|-----------------------|-------------|----------------------------------------------|
| iam.fetch_token.count | Counter     | Number of times tokens were fetched from IAM |
| iam.token_cache.size  | Gauge       | Size of the token cache                      |

### Failover Plugin

| Metric name                             | Metric type | Description                                                 |
|-----------------------------------------|-------------|-------------------------------------------------------------|
| writer_failover.triggered.count         | Counter     | Number of times writer failover was triggered               |
| writer_failover.completed.success.count | Counter     | Number of times writer failover was completed and succeeded |
| writer_failover.completed.failed.count  | Counter     | Number of times writer failover was completed and failed    |
| reader_failover.triggered.count         | Counter     | Number of times reader failover was triggered               |
| reader_failover.completed.success.count | Counter     | Number of times reader failover was completed and succeeded |
| reader_failover.completed.failed.count  | Counter     | Number of times reader failover was completed and failed    |

### Fastest Response Time Plugin

| Metric name       | Metric type | Description                                          |
|-------------------|-------------|------------------------------------------------------|
| frt.hosts.count   | Gauge       | Number of hosts the plugin is monitoring             |
| frt.response.time | Gauge       | Time taken for the host to respond to a ping request |

### Stale DNS Plugin

| Metric name              | Metric type | Description                            |
|--------------------------|-------------|----------------------------------------|
| stale_dns.stale.detected | Counter     | Number of times DNS was detected stale |
