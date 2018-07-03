package org.embulk.input.cloudwatch_logs;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.LogStream;
import com.google.common.base.Optional;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;

public class CloudwatchLogsInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task
    {
        // configuration option 1 (required integer)
        @Config("auth_method")
        @ConfigDefault("\"instance\"")
        public String getAuthMethod();

        // configuration option 2 (optional string, null is not allowed)
        @Config("access_key")
        @ConfigDefault("\"\"")
        public Optional<String> getAccessKey();

        // configuration option 3 (optional string, null is allowed)
        @Config("secret_key")
        @ConfigDefault("\"\"")
        public Optional<String> getSecretKey();

        @Config("profile_name")
        @ConfigDefault("\"\"")
        public Optional<String> getProfileName();

        @Config("region")
        @ConfigDefault("\"ap-northeast-1\"")
        public String getRegion();

        @Config("log_group_name")
        public Optional<String> getLogGroupName();

        @Config("start_time")
        public String getStartTime();

        @Config("end_time")
        public String getEndTime();

        @Config("timezone")
        @ConfigDefault("\"Asia/Tokyo\"")
        public String getTimeZone();

        @Config("limit")
        @ConfigDefault("10000")
        public Integer getLimit();

        public long getStartTimeUnix();
        public void setStartTimeUnix(long startTimeUnix);
        public long getEndTimeUnix();
        public void setEndTimeUnix(long endTimeUnix);

        public List<CloudWatchLogsStream> getLogStreams();
        public void setLogStreams(List<CloudWatchLogsStream> logStreams);

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    private static final Logger LOGGER = Exec.getLogger(CloudwatchLogsInput.class);

    @Override
    public ConfigDiff transaction(ConfigSource config,
            FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        task.setLogStreams(listLogStreams(task));
        task.setStartTimeUnix(convertToUnixTimestamp(task.getStartTime(), task.getTimeZone()));
        task.setEndTimeUnix(convertToUnixTimestamp(task.getEndTime(), task.getTimeZone()));
        int taskCount = task.getLogStreams().size();

        return resume(task.dump(), taskCount, control);
    }

    private long convertToUnixTimestamp(String datetimeStr, String timezoneId){
        DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY-MM-dd mm:HH:ss");
        DateTimeZone timezone = DateTimeZone.forID(timezoneId);
        DateTime dateTime = DateTime.parse(datetimeStr, formatter).withZone(timezone);
        long unixTimestamp = dateTime.getMillis();
        LOGGER.debug(String.format("convert %s -> %d", datetimeStr, unixTimestamp));
        return unixTimestamp;
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            int taskCount,
            FileInputPlugin.Control control)
    {
        control.run(taskSource, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            int taskCount, List<TaskReport> successTaskReports
    )
    {
        // do nothing
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new CloudwatchLogsInput(task, taskIndex);
    }

    private class CloudwatchLogsInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        public CloudwatchLogsInput(PluginTask task, int taskIndex)
        {
            super(task.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }

        public void abort()
        {
        }

        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }

        @Override
        public void close()
        {
        }
    }

    private List<CloudWatchLogsStream> listLogStreams(final PluginTask task)
    {
        LOGGER.debug("Start get log stream");
        AWSLogs logs = buildLogsClient(task);
        String logGroupName = task.getLogGroupName().get();
        boolean isFinished = false;
        List<CloudWatchLogsStream> cloudWatchLogsStreams = new ArrayList<>();
        String nextToken = null;

        while (!isFinished) {
            DescribeLogStreamsRequest request = new DescribeLogStreamsRequest(logGroupName);
            if (nextToken != null) {
                request.setNextToken(nextToken);
            }
            DescribeLogStreamsResult result = logs.describeLogStreams(request);
            List<LogStream> logStreams = result.getLogStreams();
            LOGGER.debug(String.format("fetched log stream counts: %s", logStreams.size()));
            List<CloudWatchLogsStream> list = logStreams.stream()
                    .map(logStream -> new CloudWatchLogsStream(logGroupName, logStream.getLogStreamName()))
                    .collect(Collectors.toList());

            if (cloudWatchLogsStreams.size() != 0 && !isNotExistLogStream(cloudWatchLogsStreams, list.get(0))) {
                isFinished = true;
                continue;
            }

            cloudWatchLogsStreams.addAll(list);
            nextToken = result.getNextToken();
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        LOGGER.debug(String.format("log stream counts: %s", cloudWatchLogsStreams.size()));

        return cloudWatchLogsStreams;
    }

    private boolean isNotExistLogStream(List<CloudWatchLogsStream> list, CloudWatchLogsStream stream)
    {
        int duplicateCount = list.stream()
                .filter(logStream -> logStream.getLogStreamName().equals(stream.getLogStreamName()))
                .collect(Collectors.toList())
                .size();
        return duplicateCount == 0;
    }

    private AWSLogs buildLogsClient(final PluginTask task)
    {
        AWSLogsClientBuilder builder = AWSLogsClientBuilder.standard();
        builder.setCredentials(AwsCredentials.getCredentialProvider(task));
        builder.setRegion(task.getRegion());

        return builder.build();
    }

    private class SingleFileProvider
            implements InputStreamFileInput.Provider
    {
        private PluginTask task;
        private CloudWatchLogsStream logStream;
        private int taskIndex;

        public SingleFileProvider(PluginTask task, int taskIndex)
        {
            this.task = task;
            logStream = task.getLogStreams().get(taskIndex);
            this.taskIndex = taskIndex;
        }

        @Override
        public InputStream openNext()
        {
            LOGGER.debug(String.format("task count: %d", taskIndex));
            try {
                Thread.sleep(400);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            InputStream inputStream = logStream.generateLogsInputStream(task);
            if (inputStream == null) {
                return null;
            }
            return inputStream;
        }

        @Override
        public void close()
        {

        }
    }
}
