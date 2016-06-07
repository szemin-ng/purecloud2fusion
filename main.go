package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	fusiontables "google.golang.org/api/fusiontables/v2"

	"github.com/pkg/browser"
	"github.com/szemin-ng/purecloud"
	"github.com/szemin-ng/purecloud/analytics"
	"github.com/szemin-ng/purecloud/routing"
)

// AppConfig stores the application's config data
type AppConfig struct {
	PureCloudRegion       string        `json:"pureCloudRegion"`
	PureCloudClientID     string        `json:"pureCloudClientId"`
	PureCloudClientSecret string        `json:"pureCloudClientSecret"`
	GoogleToken           *oauth2.Token `json:"googleToken"`
	Granularity           string        `json:"granularity"`
	PollFrequencySeconds  float32       `json:"pollFrequencySeconds"`
	Queues                []string      `json:"queues"`
	Agents                []string      `json:"agents"`
}

// PureCloudStatsRowID is used to map the Fusion Table Row ID to a PureCloud Queue ID. This is because the Fusion UPDATE command can only update by Row ID
type PureCloudStatsRowID struct {
	RowID     string
	QueueID   string
	MediaType string
}

const configFile string = ""

//const configFile string = `c:\users\sze min\documents\go projects\src\purecloud2fusion\config.json`
const pureCloudFusionTable string = "PureCloudStats"
const timeFormat string = "2006-01-02T15:04:05-0700"

var appConfig AppConfig // global app config
var supportedGranularity = map[string]time.Duration{"PT15M": time.Minute * 15, "PT30M": time.Minute * 30, "PT60M": time.Hour * 1, "PT1H": time.Hour * 1}
var supportedMediaType = []string{"voice", "chat", "email"}

// OAuth2 configuration to connect to Google API. The ClientID and ClientSecret is created at https://console.developers.google.com.
// Create an OAuth client ID for application type Other
var oauthConfig = oauth2.Config{
	ClientID:     "323105816832-suipl0l0bpkd57g41mpkv90pl76p02bo.apps.googleusercontent.com",
	ClientSecret: "7BeMiNlcd7AVuSp9s9uEw4cp",
	RedirectURL:  "http://localhost:56000",
	Scopes:       []string{"https://www.googleapis.com/auth/fusiontables"},
	Endpoint:     google.Endpoint,
}

var pureCloudToken purecloud.AccessToken
var fusionService *fusiontables.Service
var statTicker *time.Ticker

func main() {
	var err error

	if err = loadAppConfig(configFile); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// If there's no Google access token, authorize app
	if appConfig.GoogleToken.AccessToken == "" || appConfig.GoogleToken.RefreshToken == "" {
		if err = authorizeApp(); err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
		if err = saveAppConfig(configFile); err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}
	}

	// Link google token to fusiontables API
	var googleClient *http.Client
	googleClient = oauthConfig.Client(oauth2.NoContext, appConfig.GoogleToken)
	if fusionService, err = fusiontables.New(googleClient); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Login to PureCloud using Client Credentials login
	if err = loginToPureCloud(); err != nil {
		return
	}

	// Prepare the Fusion Table for use
	var table *fusiontables.Table
	var rowIDMap []PureCloudStatsRowID
	if table, rowIDMap, err = prepareFusionTable(); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Start polling queue stats from PureCloud
	startGrabbingPureCloudStats(table, rowIDMap)

	//	time.Sleep(time.Second * 5)
	fmt.Println("Press ENTER to STOP")
	fmt.Scanln()
	fmt.Println("Done")
}

func authorizeApp() (err error) {
	const state string = "lskdfjlasdkfj"
	var authCodeChannel chan string
	authCodeChannel = make(chan string)
	defer close(authCodeChannel)

	// Initialize and start web server
	http.HandleFunc("/", func(resp http.ResponseWriter, req *http.Request) {
		if req.FormValue("state") == state {
			authCodeChannel <- req.FormValue("code")
		}
	})
	go func() { http.ListenAndServe("localhost:56000", nil) }()

	// Direct user to login on default web browser
	var u string
	u = oauthConfig.AuthCodeURL(state)
	if err = browser.OpenURL(u); err != nil {
		return
	}

	// Wait for user to authorize or timeout
	var timeout *time.Timer
	timeout = time.NewTimer(60 * time.Second)
	var authCode string

	select { // Blocking select
	case authCode = <-authCodeChannel:
		// Exchange authorization code from Google for an access and refresh token
		if appConfig.GoogleToken, err = oauthConfig.Exchange(oauth2.NoContext, authCode); err != nil {
			return
		}
	case <-timeout.C:
		err = errors.New("Timed out waiting for user to authorize")
	}

	return
}

// createNewFusionTable creates a new Fusion Table to store PureCloud stats
func createNewFusionTable() (table *fusiontables.Table, err error) {
	var r *fusiontables.Table

	fmt.Println("Creating new Fusion Table")
	r = &fusiontables.Table{
		Name:         pureCloudFusionTable,
		Kind:         "fusiontables#table",
		IsExportable: true, // must be set to TRUE
		Columns: []*fusiontables.Column{
			&fusiontables.Column{Name: "QueueID", Kind: "fusiontables#column", Type: "STRING"},
			&fusiontables.Column{Name: "QueueName", Kind: "fusiontables#column", Type: "STRING"},
			&fusiontables.Column{Name: "MediaType", Kind: "fusiontables#column", Type: "STRING"},
			&fusiontables.Column{Name: "nError", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nOffered", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nOutboundAbandoned", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nOutboundAttempted", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nOutboundConnected", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nTransferred", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tAbandon", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtAbandon", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nAbandon", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tAcd", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtAcd", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nAcd", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tAcw", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtAcw", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nAcw", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tAgentResponseTime", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtAgentResponseTime", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nAgentResponseTime", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tAnswered", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtAnswered", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nAnswered", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tHandle", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtHandle", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nHandle", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tHeld", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtHeld", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nHeld", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tHeldComplete", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtHeldComplete", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nHeldComplete", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tIvr", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtIvr", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nIvr", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tTalk", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtTalk", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nTalk", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tTalkComplete", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtTalkComplete", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nTalkComplete", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "tUserResponseTime", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "mtUserResponseTime", Kind: "fusiontables#column", Type: "NUMBER"},
			&fusiontables.Column{Name: "nUserResponseTime", Kind: "fusiontables#column", Type: "NUMBER"},
		},
	}
	if table, err = fusionService.Table.Insert(r).Do(); err != nil {
		return
	}
	return
}

// getPureCloudQueues returns a map of queueIDs and its corresponding queue names. Up to 1,000 active and inactive queues are returned.
func getPureCloudQueues(token purecloud.AccessToken) (queues map[string]string, err error) {
	var p = routing.GetQueueParams{PageSize: 1000, PageNumber: 1, Active: false}
	var queueList routing.QueueEntityListing

	queues = make(map[string]string)

	fmt.Printf("Retrieving list of configured PureCloud queues...\n")
	if queueList, err = routing.GetListOfQueues(token, p); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	for _, queue := range queueList.Entities {
		queues[queue.ID] = queue.Name
	}
	fmt.Printf("Mapped %d queues\n", len(queues))

	return
}

// loadAppConfig loads the config file for the app to run. If a configFile is passed in, e.g., C:\config.json, it uses that file. This is for testing purposes.
// In production, null string should be passed in so that it looks for the config file at os.Args[1]
func loadAppConfig(configFile string) (err error) {
	var f string

	// Config file supplied?
	if configFile == "" {
		if len(os.Args) < 2 {
			err = errors.New("Usage: %s configfile")
			return
		}
		f = os.Args[1]
	} else {
		f = configFile
	}

	// Read config file
	var b []byte
	if b, err = ioutil.ReadFile(f); err != nil {
		return
	}

	// Decode into AppConfig struct
	var d = json.NewDecoder(bytes.NewReader(b))
	if err = d.Decode(&appConfig); err != nil {
		return
	}

	// Validate granularity in config file
	if _, valid := supportedGranularity[appConfig.Granularity]; valid == false {
		err = errors.New("Invalid granularity. Use PT15M, PT30M, PT60M or PT1H")
		return
	}

	// Validate pollFequencySeconds
	if int(appConfig.PollFrequencySeconds) <= 0 || int(appConfig.PollFrequencySeconds) > 60 {
		err = errors.New("Invalid frequency. Keep it within 60 seconds")
		return
	}

	return
}

// loginToPureCloud logs into PureCloud using client credentials login
func loginToPureCloud() (err error) {
	fmt.Printf("Logging into PureCloud...\r")
	if pureCloudToken, err = purecloud.LoginWithClientCredentials(appConfig.PureCloudRegion, appConfig.PureCloudClientID, appConfig.PureCloudClientSecret); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("Successfully logged in.\n")
	return
}

// prepareFusionTable prepares the Fusion Table with the rows of queues and returns a map of row ids which can later be used with the UPDATE Fusion SQL syntax.
// The Fusion UPDATE statement requires a row ID to update rows
func prepareFusionTable() (table *fusiontables.Table, rowIDMap []PureCloudStatsRowID, err error) {
	var tl *fusiontables.TableList

	// List all Fusion Tables
	fmt.Println("Getting list of Fusion Tables")
	if tl, err = fusionService.Table.List().Do(); err != nil {
		return
	}

	// Find Table ID
	for _, t := range tl.Items {
		if t.Name == pureCloudFusionTable {
			table = t
			break
		}
	}

	// If table not found, create new table
	if table == nil {
		if table, err = createNewFusionTable(); err != nil {
			return
		}
	} else {
		fmt.Println("Clearing data in existing Fusion Table")
		if _, err = fusionService.Query.Sql("DELETE FROM " + table.TableId).Do(); err != nil {
			return
		}
	}

	// Get list of queue names
	var queueNames map[string]string
	if queueNames, err = getPureCloudQueues(pureCloudToken); err != nil {
		return
	}

	// ------------TODO: Change this to INSERT multiple rows in HTTP call------------
	// Prepare prerequisite data in Fusion Table
	fmt.Println("Writing initial PureCloud data into Fusion Table")
	for _, queueID := range appConfig.Queues {
		for _, mediaType := range supportedMediaType {
			var res *fusiontables.Sqlresponse
			if res, err = fusionService.Query.Sql(fmt.Sprintf("INSERT INTO %s (QueueID, QueueName, MediaType) VALUES ('%s', '%s', '%s')", table.TableId, queueID, strings.Replace(queueNames[queueID], "'", "''", -1), mediaType)).Do(); err != nil {
				return
			}
			rowIDMap = append(rowIDMap, PureCloudStatsRowID{
				RowID:     res.Rows[0][0].(string),
				QueueID:   queueID,
				MediaType: mediaType,
			})
		}
	}

	return
}

// saveAppConfig saves the config file for the app. If a configFile is passed in, e.g., C:\config.json, it uses that file. This is for testing purposes.
// In production, null string should be passed in so that it looks for the config file at os.Args[1]
func saveAppConfig(configFile string) (err error) {
	var f string

	// Config file supplied?
	if configFile == "" {
		if len(os.Args) < 2 {
			err = errors.New("Usage: %s configfile")
			return
		}
		f = os.Args[1]
	} else {
		f = configFile
	}

	var b []byte
	b, _ = json.Marshal(appConfig)
	if err = ioutil.WriteFile(f, b, 0600); err != nil {
		return
	}

	return
}

// startGrabbingPureCloudStats starts a ticker to query PureCloud stats and write it into the Fusion Table
func startGrabbingPureCloudStats(table *fusiontables.Table, rowIDMap []PureCloudStatsRowID) (err error) {
	var tick time.Duration
	tick = time.Duration(int64(time.Second) * int64(appConfig.PollFrequencySeconds))
	fmt.Printf("Setting ticker to %s\n", tick)
	statTicker = time.NewTicker(tick)

	// goroutine to query PureCloud for stats based on frequency
	go func() {
		for t := range statTicker.C {
			var err error
			var startInterval, endInterval time.Time

			startInterval = time.Now().Truncate(supportedGranularity[appConfig.Granularity])
			endInterval = startInterval.Add(supportedGranularity[appConfig.Granularity])

			if err = queryAndWriteQueueStatsToFusionTable(startInterval, endInterval, table, rowIDMap); err != nil {
				fmt.Printf("%s: Error: %s\n", t.Format(timeFormat), err)
			}
		}
	}()

	return
}

// queryAndWriteQueueStatsToFusionTable queries PureCloud for stats and writes the data into the Fusion Table
func queryAndWriteQueueStatsToFusionTable(startInterval time.Time, endInterval time.Time, table *fusiontables.Table, rowIDMap []PureCloudStatsRowID) (err error) {
	// Create query to retrieve PureCloud queue statistics
	var query = purecloud.AggregationQuery{
		Interval:    startInterval.Format(timeFormat) + "/" + endInterval.Format(timeFormat),
		Granularity: appConfig.Granularity,
		GroupBy:     []string{"mediaType", "queueId"},
		Filter:      &purecloud.AnalyticsQueryFilter{Type: "or"},
		FlattenMultiValuedDimensions: true,
	}

	// Append queue IDs into the PureCloud query
	for _, queueID := range appConfig.Queues {
		query.Filter.Predicates = append(query.Filter.Predicates, purecloud.AnalyticsQueryPredicate{Dimension: "queueId", Value: queueID})
	}

	fmt.Printf("Querying queue stats for interval %s to %s...\n", startInterval.Format(timeFormat), endInterval.Format(timeFormat))

	// Send query to PureCloud
	var resp purecloud.AggregateQueryResponse
	if resp, err = analytics.QueryConversationAggregates(pureCloudToken, query); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Loop through all monitored queues
	for _, queueID := range appConfig.Queues {
		// Loop through all supported media types
		for _, mediaType := range supportedMediaType {
			// Declare variables here so that it gets initialized to zero for every loop
			var nError, nOffered, nOutboundAbandoned, nOutboundAttempted, nOutboundConnected, nTransferred int
			var nAbandon, nAcd, nAcw, nAgentResponseTime, nAnswered, nHandle, nHeld, nHeldComplete, nIvr, nTalk, nTalkComplete, nUserResponseTime int
			var tAbandon, mtAbandon, tAcd, mtAcd, tAcw, mtAcw, tAgentResponseTime, mtAgentResponseTime, tAnswered, mtAnswered, tHandle, mtHandle float64
			var tHeld, mtHeld, tHeldComplete, mtHeldComplete, tIvr, mtIvr, tTalk, mtTalk, tTalkComplete, mtTalkComplete, tUserResponseTime, mtUserResponseTime float64

			// Find result from result set returned from PureCloud
			for _, result := range resp.Results {
				if result.Group.QueueID == queueID && result.Group.MediaType == mediaType {
					// If found, grab all the metrics
					for _, data := range result.Data {
						for _, metric := range data.Metrics {
							switch {
							case metric.Metric == "nError":
								nError = int(metric.Stats.Count)
							case metric.Metric == "nOffered":
								nOffered = int(metric.Stats.Count)
							case metric.Metric == "nOutboundAbandoned":
								nOutboundAbandoned = int(metric.Stats.Count)
							case metric.Metric == "nOutboundAttempted":
								nOutboundAttempted = int(metric.Stats.Count)
							case metric.Metric == "nOutboundConnected":
								nOutboundConnected = int(metric.Stats.Count)
							case metric.Metric == "nTransferred":
								nTransferred = int(metric.Stats.Count)
							case metric.Metric == "tAbandon":
								tAbandon = metric.Stats.Sum
								mtAbandon = metric.Stats.Max
								nAbandon = int(metric.Stats.Count)
							case metric.Metric == "tAcd":
								tAcd = metric.Stats.Sum
								mtAcd = metric.Stats.Max
								nAcd = int(metric.Stats.Count)
							case metric.Metric == "tAcw":
								tAcw = metric.Stats.Sum
								mtAcw = metric.Stats.Max
								nAcw = int(metric.Stats.Count)
							case metric.Metric == "tAgentResponseTime":
								tAgentResponseTime = metric.Stats.Sum
								mtAgentResponseTime = metric.Stats.Max
								nAgentResponseTime = int(metric.Stats.Count)
							case metric.Metric == "tAnswered":
								tAnswered = metric.Stats.Sum
								mtAnswered = metric.Stats.Max
								nAnswered = int(metric.Stats.Count)
							case metric.Metric == "tHandle":
								tHandle = metric.Stats.Sum
								mtHandle = metric.Stats.Max
								nHandle = int(metric.Stats.Count)
							case metric.Metric == "tHeld":
								tHeld = metric.Stats.Sum
								mtHeld = metric.Stats.Max
								nHeld = int(metric.Stats.Count)
							case metric.Metric == "tHeldComplete":
								tHeldComplete = metric.Stats.Sum
								mtHeldComplete = metric.Stats.Max
								nHeldComplete = int(metric.Stats.Count)
							case metric.Metric == "tIvr":
								tIvr = metric.Stats.Sum
								mtIvr = metric.Stats.Max
								nIvr = int(metric.Stats.Count)
							case metric.Metric == "tTalk":
								tTalk = metric.Stats.Sum
								mtTalk = metric.Stats.Max
								nTalk = int(metric.Stats.Count)
							case metric.Metric == "tTalkComplete":
								tTalkComplete = metric.Stats.Sum
								mtTalkComplete = metric.Stats.Max
								nTalkComplete = int(metric.Stats.Count)
							case metric.Metric == "tUserResponseTime":
								tUserResponseTime = metric.Stats.Sum
								mtUserResponseTime = metric.Stats.Max
								nUserResponseTime = int(metric.Stats.Count)
							default:
								panic(fmt.Sprintf("Unrecognized metric %s", metric.Metric)) // panic if we don't recognize the metric, need to fix code
							}
						}
						fmt.Printf("Interval: %.16s, Queue: %.8s..., Media: %s, Off: %d, Ans: %d, Aban: %d\n", data.Interval, queueID, mediaType, nOffered, nAnswered, nAbandon)
					}

					// Found it, break out of loop
					break
				}
			}

			// Find row ID for Fusion Table UPDATE statement
			for _, r := range rowIDMap {
				if r.QueueID == queueID && r.MediaType == mediaType {
					var s = fmt.Sprintf("UPDATE %s SET "+
						"nError = %d, nOffered = %d, nOutboundAbandoned = %d, nOutboundAttempted = %d, nOutboundConnected = %d, nTransferred = %d, "+
						"tAbandon = %f, mtAbandon = %f, nAbandon = %d, tAcd = %f, mtAcd = %f, nAcd = %d, tAcw = %f, mtAcw = %f, nAcw = %d, tAgentResponseTime = %f, mtAgentResponseTime = %f, nAgentResponseTime = %d, "+
						"tAnswered = %f, mtAnswered = %f, nAnswered = %d, tHandle = %f, mtHandle = %f, nHandle = %d, tHeld = %f, mtHeld = %f, nHeld = %d, tHeldComplete = %f, mtHeldComplete = %f, nHeldComplete = %d, "+
						"tIvr = %f, mtIvr = %f, nIvr = %d, tTalk = %f, mtTalk = %f, nTalk = %d, tTalkComplete = %f, mtTalkComplete = %f, nTalkComplete = %d, tUserResponseTime = %f, mtUserResponseTime = %f, nUserResponseTime = %d "+
						"WHERE ROWID = '%s'",
						table.TableId,
						nError, nOffered, nOutboundAbandoned, nOutboundAttempted, nOutboundConnected, nTransferred,
						tAbandon, mtAbandon, nAbandon, tAcd, mtAcd, nAcd, tAcw, mtAcw, nAcw, tAgentResponseTime, mtAgentResponseTime, nAgentResponseTime,
						tAnswered, mtAnswered, nAnswered, tHandle, mtHandle, nHandle, tHeld, mtHeld, nHeld, tHeldComplete, mtHeldComplete, nHeldComplete,
						tIvr, mtIvr, nIvr, tTalk, mtTalk, nTalk, tTalkComplete, mtTalkComplete, nTalkComplete, tUserResponseTime, mtUserResponseTime, nUserResponseTime,
						r.RowID)

					// Write the stats into Fusion Table
					if _, err = fusionService.Query.Sql(s).Do(); err != nil {
						fmt.Println(err)
					}
				}
			}
		}
	}

	return
}
