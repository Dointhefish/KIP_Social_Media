using System;
using System.IO;
using System.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Diagnostics;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Temboo.Core;
using Temboo.Library.Twitter.OAuth;
using Temboo.Library.Twitter.Search;
using Temboo.Library.Twitter.FriendsAndFollowers;
using Temboo.Library.Twitter.Users;
using Temboo.Library.Twitter.Timelines;
using Temboo.Library.Twitter.Tweets;

namespace KIP_Social_Pull
{
    public class Twitter
    {

        String consumer_key = "";
        String consumer_secret = "";
        String callBackId = "";
        String oauthTokenSecret = "";
        String accessToken = "";
        String accessTokenSecret = "";
        String refreshToken = "";
        TembooSession session;
        AwarenessMapDB amDB;
        //Dictionary<string, string> dic_followers;

        public Twitter()
        {
            var appSettings = ConfigurationManager.AppSettings;
            //session = new TembooSession(appSettings.Get("Temboo_account"),
            //    appSettings.Get("Temboo_application"),
            //    appSettings.Get("Temboo_application_key"));
            session = new TembooSession("keepitpumping", "KeepItPumping-Awareness-Map", "692238482e2b4bc7b62d09234193c202");
            consumer_secret = appSettings.Get("Twitter_consumer_secret");
            consumer_key = appSettings.Get("Twitter_consumer_key");

            amDB = new AwarenessMapDB();
            amDB.getTwitterAuth(ref accessToken, ref accessTokenSecret);
            if (accessToken == "" || accessTokenSecret == "")
            {

                InitializeOAuth initializeOAuthChoreo = new InitializeOAuth(session);

                // Set inputs
                initializeOAuthChoreo.setConsumerSecret(consumer_secret);
                initializeOAuthChoreo.setConsumerKey(consumer_key);

                // Execute Choreo
                InitializeOAuthResultSet initializeOAuthResults = initializeOAuthChoreo.execute();

                // Print results
                //Console.WriteLine("Twitter:");
                //Console.WriteLine(initializeOAuthResults.AuthorizationURL);
                //Console.WriteLine(initializeOAuthResults.CallbackID);
                //Console.WriteLine(initializeOAuthResults.OAuthTokenSecret);

                callBackId = initializeOAuthResults.CallbackID;
                oauthTokenSecret = initializeOAuthResults.OAuthTokenSecret;

                Process.Start(initializeOAuthResults.AuthorizationURL);
                Thread.Sleep(10000);

                FinalizeOAuth finalizeOAuthChoreo = new FinalizeOAuth(session);

                // Set inputs
                finalizeOAuthChoreo.setCallbackID(callBackId);
                finalizeOAuthChoreo.setOAuthTokenSecret(oauthTokenSecret);
                finalizeOAuthChoreo.setConsumerSecret(consumer_secret);
                finalizeOAuthChoreo.setConsumerKey(consumer_key);

                // Execute Choreo
                FinalizeOAuthResultSet finalizeOAuthResults = finalizeOAuthChoreo.execute();

                // Print results
                //Console.WriteLine("Secret="+finalizeOAuthResults.AccessTokenSecret);
                //Console.WriteLine("AccessToken="+finalizeOAuthResults.AccessToken);
                //Console.WriteLine("Error="+finalizeOAuthResults.ErrorMessage);
                //Console.WriteLine("SreenName="+finalizeOAuthResults.ScreenName);
                //Console.WriteLine("UserID="+finalizeOAuthResults.UserID);
                accessToken = finalizeOAuthResults.AccessToken;
                accessTokenSecret = finalizeOAuthResults.AccessTokenSecret;
                amDB.updateTwitterAuth(accessToken, accessTokenSecret);
            }
            //Console.ReadKey();
        }

        public void findOnTimeLine(string user_id, string hashtag)
        {
            UserTimeline userTimelineChoreo = new UserTimeline(session);

            // Set credential to use for execution
            userTimelineChoreo.setAccessToken(accessToken);
            userTimelineChoreo.setAccessTokenSecret(accessTokenSecret);
            userTimelineChoreo.setConsumerSecret(consumer_secret);
            userTimelineChoreo.setConsumerKey(consumer_key);
            userTimelineChoreo.setIncludeRetweets("true");
            // Set inputs
            userTimelineChoreo.setCount("200");
            userTimelineChoreo.setScreenName(user_id);

            bool done = false;
            while (!done)
            {
                // get user tweets
                UserTimelineResultSet userTimelineResults = userTimelineChoreo.execute();
                string s_results = userTimelineResults.Response;
                string last_id = "";
                JArray ja;
                if (!s_results.Contains(hashtag))
                {
                    ja = JArray.Parse(s_results);
                    JToken jt = ja.Last;
                    last_id = jt["id"].ToString();
                    userTimelineChoreo.setMaxId(last_id);
                }
                else
                {
                    ja = JArray.Parse(s_results);
                    foreach (JToken jtok in ja)
                    {
                        string tmp = jtok["text"].ToString();
                        if (tmp.Contains(hashtag))
                        {
                            Console.WriteLine("Processing: " + tmp);
                            processTweet(jtok.ToString(), hashtag);
                        }
                    }
                    last_id = ja.Last["id"].ToString();
                    userTimelineChoreo.setMaxId(last_id);
                }
                string s_tweet_date = ja.First["created_at"].ToString();
                DateTime tweet_date = GetTweetDate(s_tweet_date); ;
                if (tweet_date < DateTime.Parse("2015-08-02"))
                {
                    done = true;
                }
            }

        }

        public void getTwitterHashtagData(string hashTag)
        {
            Tweets tweetsChoreo = new Tweets(session);

            // hastag search
            tweetsChoreo.setAccessToken(accessToken);
            tweetsChoreo.setQuery(hashTag);
            tweetsChoreo.setAccessTokenSecret(accessTokenSecret);
            tweetsChoreo.setConsumerSecret(consumer_secret);
            tweetsChoreo.setConsumerKey(consumer_key);
            tweetsChoreo.setCount("200");
            string tomorrow = DateTime.Now.AddDays(1).ToString("yyyy-MM-dd");
            tweetsChoreo.setUntil(tomorrow);

            // Execute Choreo
            TweetsResultSet tweetsResults = tweetsChoreo.execute();

            // Print results
            //Console.WriteLine(tweetsResults.Response);
            Console.WriteLine(DateTime.Now.ToString());
            //Console.WriteLine(tweetsResults.Limit);
            //Console.WriteLine(tweetsResults.Remaining);
            //Console.WriteLine(tweetsResults.Reset);
            //Create JSON objects
            string s_tweets = tweetsResults.Response;
            processTweeting(s_tweets, hashTag);
        }

        public void getTwitterHashtagData(string hashTag, DateTime since)
        {
            Tweets tweetsChoreo = new Tweets(session);

            // hastag search
            tweetsChoreo.setAccessToken(accessToken);
            tweetsChoreo.setQuery(hashTag);
            tweetsChoreo.setAccessTokenSecret(accessTokenSecret);
            tweetsChoreo.setConsumerSecret(consumer_secret);
            tweetsChoreo.setConsumerKey(consumer_key);
            tweetsChoreo.setCount("200");
            tweetsChoreo.setUntil(since.ToString("yyyy-MM-dd"));

            // Execute Choreo
            TweetsResultSet tweetsResults = tweetsChoreo.execute();
            //Create JSON objects
            string s_tweets = tweetsResults.Response;
            processTweeting(s_tweets, hashTag);

            JObject json_tweets = JObject.Parse(s_tweets);
            string tweet_id = (string)json_tweets["statuses"][0]["id"];

            tweetsChoreo = new Tweets(session);
            tweetsChoreo.setAccessToken(accessToken);
            tweetsChoreo.setQuery(hashTag);
            tweetsChoreo.setAccessTokenSecret(accessTokenSecret);
            tweetsChoreo.setConsumerSecret(consumer_secret);
            tweetsChoreo.setConsumerKey(consumer_key);
            tweetsChoreo.setCount("200");
            tweetsChoreo.setSinceId(tweet_id);

            tweetsResults = tweetsChoreo.execute();
            s_tweets = tweetsResults.Response;
            processTweeting(s_tweets, hashTag);
        }

        public void getTwitterUserData(string user_name)
        {
            //List
            UserTimeline userTimelineChoreo = new UserTimeline(session);

            Show showChoreo = new Show(session);
            showChoreo.setScreenName(user_name);
            showChoreo.setAccessToken(accessToken);
            showChoreo.setAccessTokenSecret(accessTokenSecret);
            showChoreo.setConsumerSecret(consumer_secret);
            showChoreo.setConsumerKey(consumer_key);
            showChoreo.setIncludeEntities("false");

            // Get user date
            ShowResultSet showResults = showChoreo.execute();
            // Get user_id
            string s_user = showResults.Response;
            JObject json_user = JObject.Parse(s_user);
            string user_id = (string)json_user["id"];

            // Set inputs for user tweets
            userTimelineChoreo.setAccessToken(accessToken);
            userTimelineChoreo.setAccessTokenSecret(accessTokenSecret);
            userTimelineChoreo.setConsumerSecret(consumer_secret);
            userTimelineChoreo.setUserId(user_id);
            userTimelineChoreo.setConsumerKey(consumer_key);
            userTimelineChoreo.setCount("200");

            // Execute Choreo for users timeline tweets
            UserTimelineResultSet userTimelineResults = userTimelineChoreo.execute();

            //Create JSON objects
            string s_tweets = userTimelineResults.Response;
            string s2_tweets = "{\"statuses\":" + s_tweets + "}";
            processTweeting(s2_tweets, "");

            Mentions mentionsChoreo = new Mentions(session);

            // Set inputs for mentions

            mentionsChoreo.setAccessToken(accessToken);
            mentionsChoreo.setAccessTokenSecret(accessTokenSecret);
            mentionsChoreo.setConsumerSecret(consumer_secret);
            mentionsChoreo.setConsumerKey(consumer_key);

            // Get mentions
            MentionsResultSet mentionsResults = mentionsChoreo.execute();

            s_tweets = mentionsResults.Response;
            s2_tweets = "{\"statuses\":" + s_tweets + "}";
            processTweeting(s2_tweets, user_name);
        }


        public void processTweeting(string p_tweets, string hashTag)
        {
            JObject json_tweets = JObject.Parse(p_tweets);

            //Dictionary<string, string> retweeters;
            //retweeters = GetRetweeters(json_tweets);

            string tweet_text;
            string user_name;
            string user_id;
            string profile_image_url;
            string location;
            string utc_offset;
            string retweeted_by = "";
            string retweeted_by_id = "";
            string retweeted_profile_image = "";
            string retweet_location = "";
            string retweet_utc_offset = "";
            string retweet_count = "";
            string retw_place_name = "";
            string retw_country_code = "";
            string retw_s_create_date = "";
            int retw_followers = 0;
            int retw_lattitude = 0;
            int retw_longitude= 0;
            string lat_lon;
            decimal lattitude = 0;
            decimal longitude = 0;
            string favorite_count;
            int followers_count;
            int ext_followers = 0;
            string place_name = "";
            string country_code = "";
            string s_create_date = "";
            DateTime created_date;

            //Set up loop
            string tweet_id = (string)json_tweets["statuses"][0]["id"];

            int i = 0;
            while (tweet_id != null)
            {
                //check for a retweeting status
                JToken retweeted = json_tweets["statuses"][i]["retweeted_status"];
                retweeted_by = "";
                retweeted_by_id = "";
                retweeted_profile_image = "";
                retweet_location = "";
                retweet_utc_offset = "";
                retweet_count = "";

                //pull relevant data out of JSON tweet & retweet records
                if (retweeted != null && retweeted.Count() > 0) //get tweet and retweet data
                {
                    //track the followers for this retweet
                    //dic_followers = new Dictionary<string, string>();

                    tweet_id = (string)json_tweets["statuses"][i]["retweeted_status"]["id"];
                    tweet_text = (string)json_tweets["statuses"][i]["retweeted_status"]["text"];
                    user_name = (string)json_tweets["statuses"][i]["retweeted_status"]["user"]["screen_name"];
                    user_id = (string)json_tweets["statuses"][i]["retweeted_status"]["user"]["id"];
                    profile_image_url = (string)json_tweets["statuses"][i]["retweeted_status"]["user"]["profile_image_url"];
                    location = (string)json_tweets["statuses"][i]["retweeted_status"]["user"]["location"];
                    utc_offset = (string)json_tweets["statuses"][i]["retweeted_status"]["user"]["utc_offset"];
                    try
                    {
                        followers_count = int.Parse((string)json_tweets["statuses"][i]["retweeted_status"]["user"]["followers_count"]);
                    }
                    catch
                    {
                        followers_count = int.Parse((string)json_tweets["statuses"][i]["retweeted_status"]["user"]["entities"]["followers_count"]);
                    }
                    s_create_date = (string)json_tweets["statuses"][i]["retweeted_status"]["created_at"];

                    //check for place (location details) data
                    JToken place = json_tweets["statuses"][i]["retweeted_status"]["place"];
                    //If place data exists, extract relevant fields
                    place_name = "";
                    country_code = "";
                    lattitude = 0;
                    longitude = 0;
                    if (place != null && place.Count() > 0)
                    {
                        place_name = (string)json_tweets["statuses"][i]["retweeted_status"]["place"]["name"];
                        country_code = (string)json_tweets["statuses"][i]["retweeted_status"]["place"]["country_code"];
                        JArray a_lat = (JArray)json_tweets["statuses"][i]["retweeted_status"]["place"]["bounding_box"]["coordinates"];
                        getCoordinates(a_lat, ref longitude, ref lattitude);
                    }
                    favorite_count = (string)json_tweets["statuses"][i]["retweeted_status"]["favorite_count"];

                    retweeted_by = (string)json_tweets["statuses"][i]["user"]["screen_name"];
                    retweeted_by_id = (string)json_tweets["statuses"][i]["user"]["id"];
                    retweeted_profile_image = (string)json_tweets["statuses"][i]["user"]["profile_image_url"];
                    retweet_location = (string)json_tweets["statuses"][i]["user"]["location"];
                    retweet_utc_offset = (string)json_tweets["statuses"][i]["user"]["utc_offset"];
 
                    retweet_count = (string)json_tweets["statuses"][i]["retweet_count"];
                    int rt_count = int.Parse(retweet_count);

                    // determine the impression of the retweet
                    if (rt_count > 1 && !amDB.isPreviousRetweet(tweet_id, rt_count))
                    {
                        retw_followers = getRTImpression(tweet_id); //get the followers of the retweeters
                    }
                    else
                    {
                        try
                        {
                            retw_followers = int.Parse((string)json_tweets["statuses"][i]["user"]["followers_count"]);
                        }
                        catch
                        {
                            retw_followers = int.Parse((string)json_tweets["statuses"][i]["user"]["entities"]["followers_count"]);
                        }
                    }
                    retw_s_create_date = (string)json_tweets["statuses"][i]["created_at"];

                    //check for place (location details) data
                    JToken retw_place = json_tweets["statuses"][i]["place"];
                    //If place data exists, extract relevant fields
                    retw_place_name = "";
                    retw_country_code = "";
                    retw_lattitude = 0;
                    retw_longitude = 0;
                    if (retw_place != null && retw_place.Count() > 0)
                    {
                        retw_place_name = (string)json_tweets["statuses"][i]["place"]["name"];
                        retw_country_code = (string)json_tweets["statuses"][i]["place"]["country_code"];
                        JArray a_lat = (JArray)json_tweets["statuses"][i]["place"]["bounding_box"]["coordinates"];
                        getCoordinates(a_lat, ref longitude, ref lattitude);
                    }

                }
                else //get just tweet data
                {
                    tweet_id = (string)json_tweets["statuses"][i]["id"];
                    tweet_text = (string)json_tweets["statuses"][i]["text"];
                    user_name = (string)json_tweets["statuses"][i]["user"]["screen_name"];
                    user_id = (string)json_tweets["statuses"][i]["user"]["id"];
                    profile_image_url = (string)json_tweets["statuses"][i]["user"]["profile_image_url"];
                    location = (string)json_tweets["statuses"][i]["user"]["location"];
                    utc_offset = (string)json_tweets["statuses"][i]["user"]["utc_offset"];
                    try
                    {
                        followers_count = int.Parse((string)json_tweets["statuses"][i]["user"]["followers_count"]);
                    }
                    catch
                    {
                        followers_count = int.Parse((string)json_tweets["statuses"][i]["user"]["entities"]["followers_count"]);
                    }
                    s_create_date = (string)json_tweets["statuses"][i]["created_at"];

                    //check for place (location details) data
                    JToken place = json_tweets["statuses"][i]["place"];
                    //If place data exists, extract relevant fields
                    place_name = "";
                    country_code = "";
                    lattitude = 0;
                    longitude = 0;
                    if (place != null && place.Count() > 0)
                    {
                        place_name = (string)json_tweets["statuses"][i]["place"]["name"];
                        country_code = (string)json_tweets["statuses"][i]["place"]["country_code"];
                        JArray a_lat = (JArray)json_tweets["statuses"][i]["place"]["bounding_box"]["coordinates"];
                        getCoordinates(a_lat, ref longitude, ref lattitude);
                        //lattitude = decimal.Parse(s_lat);
                        //string s_long = (string)json_tweets["statuses"][i]["place"]["bounding_box"]["coordinates"][0][1];
                        //longitude = decimal.Parse(s_long);
                    }
                    favorite_count = (string)json_tweets["statuses"][i]["favorite_count"];
                }

                //Console.WriteLine("#" + i.ToString() + ":");
                //Console.WriteLine("name = " + user_name);

                created_date = GetTweetDate(s_create_date);
                int tweetStatus = amDB.upsertTwitterRawCounts(
                      tweet_id
                    , tweet_text
                    , user_name
                    , user_id
                    , profile_image_url
                    , location
                    , favorite_count
                    , utc_offset == null ? 0 : decimal.Parse(utc_offset)
                    , place_name
                    , country_code
                    , lattitude
                    , longitude
                    , followers_count
                    , created_date
                    , hashTag);
                if (tweetStatus == 1)
                    Console.WriteLine(tweet_id + " inserted");

                if (retweeted_by != "")
                {
                    created_date = GetTweetDate(retw_s_create_date);
                    int retweetStatus = amDB.upsertRetweetRawCounts(
                          tweet_id
                        , retweeted_by
                        , retweeted_by_id
                        , retweeted_profile_image
                        , retweet_utc_offset == null ? 0 : decimal.Parse(retweet_utc_offset)
                        , int.Parse(retweet_count)
                        , place_name
                        , country_code
                        , retw_lattitude
                        , retw_longitude
                        , retw_followers
                        , ext_followers
                        , created_date
                        , hashTag);
                    if (retweetStatus == 1)
                        Console.WriteLine(tweet_id + " retweet inserted");
                }   

                i++;
                //Test for loop end
                try
                {
                    tweet_id = (string)json_tweets["statuses"][i]["id"];
                }
                catch (Exception e)
                {
                    tweet_id = null; //no more records
                }
            }

            //Commit tweet data to the geographical totals.
            amDB.twitter_aggregate();
        }

        //Process a single tweet
        public void processTweet(string p_tweet, string hashTag)
        {
            JObject json_tweets = JObject.Parse(p_tweet);

            string tweet_text;
            string user_name;
            string user_id;
            string profile_image_url;
            string location;
            string utc_offset;
            string retweeted_by = "";
            string retweeted_by_id = "";
            string retweeted_profile_image = "";
            string retweet_location = "";
            string retweet_utc_offset = "";
            string retweet_count = "";
            string retw_place_name = "";
            string retw_country_code = "";
            string retw_s_create_date = "";
            int retw_followers = 0;
            int retw_lattitude = 0;
            int retw_longitude= 0;
            string lat_lon;
            decimal lattitude = 0;
            decimal longitude = 0;
            string favorite_count;
            int followers_count;
            int ext_followers = 0;
            string place_name = "";
            string country_code = "";
            string s_create_date = "";
            DateTime created_date;

            //Set up loop
            string tweet_id = (string)json_tweets["id"];

            //check for a retweeting status
            JToken retweeted = json_tweets["retweeted_status"];
            retweeted_by = "";
            retweeted_by_id = "";
            retweeted_profile_image = "";
            retweet_location = "";
            retweet_utc_offset = "";
            retweet_count = "";

            //pull relevant data out of JSON tweet & retweet records
            if (retweeted != null && retweeted.Count() > 0) //get tweet and retweet data
            {
                //track the followers for this retweet
                //dic_followers = new Dictionary<string, string>();

                tweet_id = (string)json_tweets["retweeted_status"]["id"];
                tweet_text = (string)json_tweets["retweeted_status"]["text"];
                user_name = (string)json_tweets["retweeted_status"]["user"]["screen_name"];
                user_id = (string)json_tweets["retweeted_status"]["user"]["id"];
                profile_image_url = (string)json_tweets["retweeted_status"]["user"]["profile_image_url"];
                location = (string)json_tweets["retweeted_status"]["user"]["location"];
                utc_offset = (string)json_tweets["retweeted_status"]["user"]["utc_offset"];
                try
                {
                    followers_count = int.Parse((string)json_tweets["retweeted_status"]["user"]["followers_count"]);
                }
                catch
                {
                    followers_count = int.Parse((string)json_tweets["retweeted_status"]["user"]["entities"]["followers_count"]);
                }
                s_create_date = (string)json_tweets["retweeted_status"]["created_at"];

                //check for place (location details) data
                JToken place = json_tweets["retweeted_status"]["place"];
                //If place data exists, extract relevant fields
                place_name = "";
                country_code = "";
                lattitude = 0;
                longitude = 0;
                if (place != null && place.Count() > 0)
                {
                    place_name = (string)json_tweets["retweeted_status"]["place"]["name"];
                    country_code = (string)json_tweets["retweeted_status"]["place"]["country_code"];
                    JArray a_lat = (JArray)json_tweets["retweeted_status"]["place"]["bounding_box"]["coordinates"];
                    getCoordinates(a_lat, ref longitude, ref lattitude);
                }
                favorite_count = (string)json_tweets["retweeted_status"]["favorite_count"];

                retweeted_by = (string)json_tweets["user"]["screen_name"];
                retweeted_by_id = (string)json_tweets["user"]["id"];
                retweeted_profile_image = (string)json_tweets["user"]["profile_image_url"];
                retweet_location = (string)json_tweets["user"]["location"];
                retweet_utc_offset = (string)json_tweets["user"]["utc_offset"];
 
                retweet_count = (string)json_tweets["retweet_count"];
                int rt_count = int.Parse(retweet_count);
                // determine the impression of the retweet
                if (rt_count > 1)
                {
                    retw_followers = getRTImpression(tweet_id); //get the followers of the retweeters
                }
                else
                {
                    try
                    {
                        retw_followers = int.Parse((string)json_tweets["user"]["followers_count"]);
                    }
                    catch
                    {
                        retw_followers = int.Parse((string)json_tweets["user"]["entities"]["followers_count"]);
                    }
                }
                retw_s_create_date = (string)json_tweets["created_at"];

                //check for place (location details) data
                JToken retw_place = json_tweets["place"];
                //If place data exists, extract relevant fields
                retw_place_name = "";
                retw_country_code = "";
                retw_lattitude = 0;
                retw_longitude = 0;
                if (retw_place != null && retw_place.Count() > 0)
                {
                    retw_place_name = (string)json_tweets["place"]["name"];
                    retw_country_code = (string)json_tweets["place"]["country_code"];
                    JArray a_lat = (JArray)json_tweets["place"]["bounding_box"]["coordinates"];
                    getCoordinates(a_lat, ref longitude, ref lattitude);
                }

            }
            else //get just tweet data
            {
                tweet_id = (string)json_tweets["id"];
                tweet_text = (string)json_tweets["text"];
                user_name = (string)json_tweets["user"]["screen_name"];
                user_id = (string)json_tweets["user"]["id"];
                profile_image_url = (string)json_tweets["user"]["profile_image_url"];
                location = (string)json_tweets["user"]["location"];
                utc_offset = (string)json_tweets["user"]["utc_offset"];
                try
                {
                    followers_count = int.Parse((string)json_tweets["user"]["followers_count"]);
                }
                catch
                {
                    followers_count = int.Parse((string)json_tweets["user"]["entities"]["followers_count"]);
                }
                s_create_date = (string)json_tweets["created_at"];

                //check for place (location details) data
                JToken place = json_tweets["place"];
                //If place data exists, extract relevant fields
                place_name = "";
                country_code = "";
                lattitude = 0;
                longitude = 0;
                if (place != null && place.Count() > 0)
                {
                    place_name = (string)json_tweets["place"]["name"];
                    country_code = (string)json_tweets["place"]["country_code"];
                    JArray a_lat = (JArray)json_tweets["place"]["bounding_box"]["coordinates"];
                    getCoordinates(a_lat, ref longitude, ref lattitude);
                    //lattitude = decimal.Parse(s_lat);
                    //string s_long = (string)json_tweets["statuses"][i]["place"]["bounding_box"]["coordinates"][0][1];
                    //longitude = decimal.Parse(s_long);
                }
                favorite_count = (string)json_tweets["favorite_count"];
            }

            //Console.WriteLine("#" + i.ToString() + ":");
            Console.WriteLine("name = " + user_name);
            Console.WriteLine("location = " + location);
            Console.WriteLine("tweet text = " + tweet_text);

            created_date = GetTweetDate(s_create_date);
            int tweetCount = amDB.upsertTwitterRawCounts(
                    tweet_id
                , tweet_text
                , user_name
                , user_id
                , profile_image_url
                , location
                , favorite_count
                , utc_offset == null ? 0 : decimal.Parse(utc_offset)
                , place_name
                , country_code
                , lattitude
                , longitude
                , followers_count
                , created_date
                , hashTag);
            Console.WriteLine(tweetCount.ToString() + " tweet upserted");

            if (retweeted_by != "")
            {
                created_date = GetTweetDate(retw_s_create_date);
                int retweetCount = amDB.upsertRetweetRawCounts(
                        tweet_id
                    , retweeted_by
                    , retweeted_by_id
                    , retweeted_profile_image
                    , retweet_utc_offset == null ? 0 : decimal.Parse(retweet_utc_offset)
                    , int.Parse(retweet_count)
                    , place_name
                    , country_code
                    , retw_lattitude
                    , retw_longitude
                    , retw_followers
                    , ext_followers
                    , created_date
                    , hashTag);
                Console.WriteLine(retweetCount.ToString() + " retweet upserted");
            }   


            //Commit tweet data to the geographical totals.
            amDB.twitter_aggregate();
        }

        public DateTime GetTweetDate(string p_tweetDate)
        {
            DateTime result;
            string[] td = p_tweetDate.Split(' ');
            string month = td[1];
            string day = td[2];
            string year = td[5];
            result = DateTime.Parse(year + "-" + month + "-" + day);
            return result;
        }

        public void getCoordinates(JArray p_polygon, ref decimal p_long, ref decimal p_lat)
        {
            string coords = "";
            var temp_tok = p_polygon.First;
            var coor_tok = temp_tok.First;
            string long_tok = (string)coor_tok[0];
            p_long = decimal.Parse(long_tok);
            string lat_tok = (string)coor_tok[1];
            p_lat = decimal.Parse(lat_tok);
        }

        //Search Twitter for youtube videos to determine how many times they've been shared.
        public int getTWYTVideoShares(string video_id)
        {

            Tweets tweetsChoreo = new Tweets(session);

            // Set inputs
            tweetsChoreo.setAccessToken(accessToken);
            tweetsChoreo.setQuery("https://www.youtube.com/watch?v="+video_id);
            tweetsChoreo.setAccessTokenSecret(accessTokenSecret);
            tweetsChoreo.setConsumerSecret(consumer_secret);
            tweetsChoreo.setConsumerKey(consumer_key);

            // Execute Choreo
            TweetsResultSet tweetsResults = tweetsChoreo.execute();
            JObject jo = JObject.Parse(tweetsResults.Response);
            try
            {
                JArray ja = (JArray)jo["statuses"];
                return ja.Count;
            }
            catch (Exception e)
            {
                return 0;
            }
        }

        public int getRTImpression(string tweet_id)
        {
            GetRetweeters getRetweetersChoreo = new GetRetweeters(session);

            // Set inputs
            getRetweetersChoreo.setAccessToken(accessToken);
            getRetweetersChoreo.setID(tweet_id);
            getRetweetersChoreo.setAccessTokenSecret(accessTokenSecret);
            getRetweetersChoreo.setConsumerSecret(consumer_secret);
            getRetweetersChoreo.setConsumerKey(consumer_key);

            // get retweeters
            Console.WriteLine("Getting retweeters");
            GetRetweetersResultSet getRetweetersResults = getRetweetersChoreo.execute();
            JObject json_response = JObject.Parse(getRetweetersResults.Response);
            JArray ja = (JArray)json_response["ids"];
            string s_users = "";
            foreach (JToken jt in ja)
            {
                if (jt != ja.Last)
                {
                    s_users += jt.ToString() + ",";
                }
                else
                {
                    s_users += jt.ToString();
                }
            }

            Lookup lookupChoreo = new Lookup(session);
            // Set inputs
            lookupChoreo.setAccessToken(accessToken);
            lookupChoreo.setAccessTokenSecret(accessTokenSecret);
            lookupChoreo.setConsumerSecret(consumer_secret);
            lookupChoreo.setConsumerKey(consumer_key);
            lookupChoreo.setIncludeEntities("false");
            lookupChoreo.setUserId(s_users);

            // get user data
            Console.WriteLine("Getting retweeter followers");
            LookupResultSet lookupResults = lookupChoreo.execute();
            ja = JArray.Parse(lookupResults.Response);
            string s_followers = "";
            int followers_count = 0;
            foreach (JToken jt in ja)
            {
                s_followers = (string)jt["followers_count"];
                followers_count += int.Parse(s_followers);
            }
            // add up followers

            // return followers
            return followers_count;
        }
    }


}
