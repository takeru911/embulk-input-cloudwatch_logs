# Cloudwatch Logs input plugin for Embulk

## Overview

* **Plugin type**: file input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **auth_method**: aws auth method (string, required)
  - instance(default)
  - basic
  - profile
- **access_key**: required if you set basic auth_method (String, optional)
- **secret_key**: required if you set basic auth_method (String, optional)
- **profile_name**: required if you set profile auth_method (String, optional)
- **region**: your aws region name (String, required)
  - default is "ap-north-east-1"
- **log_group_name**: you want to input log group name(String, required)
- **start_time**: export from time(String, optional)
  - format: yyyy-MM-dd HH:mm:ss
- **end_time**: export to time(String, optional)
  - format: yyyy-MM-dd HH:mm:ss
- **limit**: Limit in one processing (Integer, required)
  - default: 10000
  - However, it takes another limit and is not accurate
     - https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/logs/AWSLogs.html#getLogEvents-com.amazonaws.services.logs.model.GetLogEventsRequest-

## Example

```yaml
in:
  type: cloudwatch_logs
  auth_method: instance
  log_group_name:  your_export_log_group_name
  limit: 100
```


## Build

```
$ ./gradlew classpath
```
