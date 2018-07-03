package org.embulk.input.cloudwatch_logs;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

public class CloudWatchLogsStream
{

    private CloudwatchLogsInputPlugin.PluginTask task;
    private String logGroupName;
    private String logStreamName;
    private String nextToken;
    private long startTime;
    private long endTime;
    private final Logger LOGGER = Exec.getLogger(CloudWatchLogsStream.class);

    @JsonCreator
    public CloudWatchLogsStream(
            @JsonProperty("logGroupName")
                    String logGroupName,
            @JsonProperty("logStreamName")
                    String logStreamName
    )
    {
        this.logGroupName = logGroupName;
        this.logStreamName = logStreamName;
        this.task = task;
        //this.startTime = task.getStartTimeUnix();
        //this.endTime = task.getEndTimeUnix();
    }

    @JsonIgnore
    public InputStream generateLogsInputStream(CloudwatchLogsInputPlugin.PluginTask task)
    {
        AWSLogs logClient = buildLogsClient(task);
        GetLogEventsRequest request = new GetLogEventsRequest(logGroupName, logStreamName);
        LOGGER.debug(String.format("logGroupName: %s", logGroupName));
        LOGGER.debug(String.format("logStreamName: %s", logStreamName));
        request.setLimit(task.getLimit());
        request.setStartFromHead(true);
        if (nextToken != null) {
            request.setNextToken(nextToken);
        }
        GetLogEventsResult result = logClient.getLogEvents(request);
        StringBuffer stringBuffer = new StringBuffer();
        List<OutputLogEvent> logEvents = result.getEvents();
        LOGGER.debug(String.format("log event size: %d", logEvents.size()));
        if (logEvents.size() == 0) {
            return null;
        }
        String logLine = logEvents.stream()
                .reduce("", (str1, log) -> mergeTwoString(str1, log.getMessage(), true),
                        (str1, str2) -> mergeTwoString(str1, str2, false)
                );
        nextToken = result.getNextForwardToken();
        return new ByteArrayInputStream(logLine.getBytes());
    }

    @JsonIgnore
    private String mergeTwoString(String str1, String str2, boolean addNewLine)
    {
        StringBuffer buffer = new StringBuffer();
        buffer.append(str1);
        if (addNewLine) {
            buffer.append("\n");
        }
        buffer.append(str2);

        return buffer.toString();
    }

    @JsonIgnore
    private AWSLogs buildLogsClient(final CloudwatchLogsInputPlugin.PluginTask task)
    {
        AWSLogsClientBuilder builder = AWSLogsClientBuilder.standard();
        builder.setCredentials(new EnvironmentVariableCredentialsProvider());
        builder.setRegion("ap-northeast-1");

        return builder.build();
    }

    @JsonIgnore
    public String getNextToken()
    {
        return nextToken;
    }

    @JsonProperty("logGroupName")
    public String getLogGroupName()
    {
        return logGroupName;
    }

    @JsonProperty("logStreamName")
    public String getLogStreamName()
    {
        return logStreamName;
    }
}
