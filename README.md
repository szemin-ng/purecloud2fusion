# purecloud2fusion
Sample Go application that pulls queue statistics from PureCloud and writes it into a Google Fusion Table.

## Instructions
### PureCloud configuration
Create a new OAuth configuration with Client Credentials login and with a role that has access to queue statistics. Copy the Client ID and Client Secret from the OAuth configuration to the app's JSON configuration file.

### Google configuration
The application stores PureCloud statistics into a Fusion Table. This Fusion Table resides on a Google user's Google Drive. For the application to access the Fusion Table APIs, it must have a Google Client ID and Client Secret. This identifies the Go application to Google. Go to https://console.developers.google.com/ and create a new OAuth Client ID with the type Other. You can create this using your own Google account. Copy the Google Client ID and Secret into the oauthConfig struct in the code.

### JSON config file
```
{
  "pureCloudRegion": "mypurecloud.com.au",
  "pureCloudClientId": <<from PureCloud OAuth configuration>>,
  "pureCloudClientSecret": <<from PureCloud OAuth configuration>>,
  "granularity": "PT15M",
  "pollFrequencySeconds": 15,
  "queues": [
    "c2788c7e-c8c5-40ac-97d9-51c3b364479b","276148ba-40ad-4bad-a5a3-fedb9ddcbbb5"
  ]
}
```

For `queues`, please get the `QueueID` from PureCloud Contact Center Queue administration screen. When the Queue's administration screen is opened, you can see the `QueueID` in the browser's URL, e.g., https://apps.mypurecloud.com.au/directory/#/admin/admin/organization/_queuesV2/97126d94-28fc-4178-b616-b009f466279a.

### Running the application
Enter this. At first run, it will ask you for permission to manage your Google Fusion Tables.  Fusion Tables are in the cloud and the applicatiom must be granted access to manage a user's Fusion Tables.  After logging in, the application will save the Google's token into the same JSON config file.

```
purecloud2fusion <JSONconfigfile>
```

### Looking at the Fusion Table data
Login to Google Drive and look for PureCloudStats Fusion Table.  Open it to see the data.
