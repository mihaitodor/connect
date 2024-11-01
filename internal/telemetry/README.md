Telemetry
=========

## What is this for?

Our main goal is to find out the frequency with which each plugin is used in production environments, as this helps us prioritise enhancements and bug fixes for various plugin families on our roadmap.

Ideally, we'd also like to identify common patterns in plugin usage that may help us plan new work or identify gaps in our functionality. For example, if we were to see that almost all `aws_s3` outputs were paired with a `mutation` processor then we might conclude that embedding a mutation field into the plugin itself could be a useful feature.

## What is being sent?

When a Redpanda Connect instance exports telemetry data to our collection server it sends a JSON payload that contains a high-level and anonymous summary of the contents of the config file being executed. Specific field values are never transmitted, nor are decorations of the config such as label names. For example, with an instance running the following config:

```yaml
input:
  label: fooer
  generate:
    interval: 1s
    mapping: 'root.foo = "bar"'

output:
  label: bazer
  aws_s3:
    bucket: baz
    path: meow.txt
```

We would extract the following information:

- A unique identifier for the Redpanda Connect instance.
- The duration for which the config has been running thus far.
- That the config contains a `generate` input and an `aws_s3` output.
- The IP address of the running Redpanda Connect instance (as a byproduct of the data delivery mechanism).

The code responsible for extracting this data is simple enough to dig into, and we encourage curious users to do so. A good place to start is the data format, which can be found at [`./payload.go`](./payload.go).

## When is it sent?

Telemetry data is sent from an instance of Redpanda Connect that has been running for at least 5 minutes, this is in order to avoid sending data from instances used for testing or experimentation. Once telemetry data starts being emitted it is sent once every 24 hours.

## How do I avoid it?

Any custom build of Redpanda Connect will not send this data, as it is only included in the build artifacts published by us either through Github releases or our official Docker images. You can also prevent telemetry with the cli flag `--disable-telemetry`, where Redpanda Connect will continue operating as normal without sending any telemetry data.

