package org.embulk.input.cloudwatch_logs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.embulk.config.ConfigException;
import org.embulk.input.cloudwatch_logs.CloudwatchLogsInputPlugin.PluginTask;

import com.amazonaws.auth.AWSCredentialsProvider;

public class AwsCredentials
{

    public static AWSCredentialsProvider getCredentialProvider(PluginTask task)
    {
        return new AWSCredentialsProvider()
        {
            @Override
            public AWSCredentials getCredentials()
            {
                String authMethod = task.getAuthMethod();
                if (authMethod.equals("basic")) {
                    String accessKey = task.getAccessKey().get();
                    String secretKey = task.getSecretKey().get();

                    return new BasicAWSCredentials(accessKey, secretKey);
                }
                else if (authMethod.equals("instance")) {
                    return new EnvironmentVariableCredentialsProvider().getCredentials();
                }
                else if (authMethod.equals("profile")) {
                    String profileName = task.getProfileName().get();
                    return new ProfileCredentialsProvider(profileName).getCredentials();
                }
                else {
                    throw new ConfigException(String.format("Unknown auth_method: %s ", authMethod));
                }
            }

            @Override
            public void refresh()
            {

            }
        };
    }
}
