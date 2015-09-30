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
using Temboo.Library.Google.OAuth;
using Temboo.Library.YouTube.Channels;
using Temboo.Library.YouTube.Search;
using Temboo.Library.YouTube.Videos;
using Temboo.Library.Utilities.HTTP;

namespace KIP_Social_Pull
{
    public class YouTube
    {
        public String client_id = "";
        public String client_secret = "";
        protected String callBackId = "";
        public String accessToken = "";
        public String refreshToken = "";
        public TembooSession session;
        public AwarenessMapDB amDB;
        public string dbScope = "";

        public YouTube(string scope)
        {
            var applicationKeys = ConfigurationManager.AppSettings;
            client_id = applicationKeys.Get("Youtube_client_id");
            client_secret = applicationKeys.Get("Youtube_client_secret");
            // Instantiate the Choreo, using a previously instantiated TembooSession object, eg:
            session = new TembooSession("keepitpumping", "KeepItPumping-Awareness-Map", "692238482e2b4bc7b62d09234193c202");
            amDB = new AwarenessMapDB();

            if (scope.Contains("youtube.readonly"))
            {
                amDB.getYoutubeAuth("YouTube.readonly", ref accessToken, ref refreshToken);
                dbScope = "YouTube.readonly";
            }
            else if (scope.Contains("yt-analytics.readonly"))
            {
                amDB.getYoutubeAuth("YouTube.analytics", ref accessToken, ref refreshToken);
                dbScope = "YouTube.analytics";
            }

            if (accessToken == "" || refreshToken == "")
            {
                InitializeOAuth initializeOAuthChoreo = new InitializeOAuth(session);

                // Set inputs
                initializeOAuthChoreo.setClientID(client_id);
                initializeOAuthChoreo.setScope(scope);

                // Execute Choreo
                InitializeOAuthResultSet initializeOAuthResults = initializeOAuthChoreo.execute();

                Process.Start(initializeOAuthResults.AuthorizationURL);
                Thread.Sleep(20000);

                FinalizeOAuth finalizeOAuthChoreo = new FinalizeOAuth(session);

                // Set inputs
                finalizeOAuthChoreo.setCallbackID(initializeOAuthResults.CallbackID);
                finalizeOAuthChoreo.setClientSecret(client_secret);
                finalizeOAuthChoreo.setClientID(client_id);

                // Execute Choreo
                FinalizeOAuthResultSet finalizeOAuthResults = finalizeOAuthChoreo.execute();

                accessToken = finalizeOAuthResults.AccessToken;
                refreshToken = finalizeOAuthResults.RefreshToken;
                amDB.updateYoutubeAuth(dbScope, accessToken, refreshToken);
            }

        }

    }

    public class YTTemboo
    {
        private int requestsMade = 0;
        private YouTube yto;

        public YTTemboo(YouTube o) 
        { 
            yto = o;
        }

        public string getTembooData(Object Choreo)
        {
            int retry = 0;
            string tbResponse = "";


            while (retry >= 0)
            {
                if (requestsMade >= 6)
                {
                    Thread.Sleep(5000);
                    requestsMade = 0;
                }

                try
                {
                    if (Choreo.GetType() == typeof(ListMyChannels))
                    {
                        ((ListMyChannels)Choreo).setAccessToken(yto.accessToken);
                        ListMyChannelsResultSet choreoResultSet;
                        choreoResultSet = ((ListMyChannels)Choreo).execute();
                        tbResponse = choreoResultSet.Response;
                    }
                    else if (Choreo.GetType() == typeof(ListChannelsByID))
                    {
                        ((ListChannelsByID)Choreo).setAccessToken(yto.accessToken);
                        ListChannelsByIDResultSet choreoResultSet;
                        choreoResultSet = ((ListChannelsByID)Choreo).execute();
                        tbResponse = choreoResultSet.Response;
                    }
                    else if (Choreo.GetType() == typeof(ListMySubscribers))
                    {
                        ((ListMySubscribers)Choreo).setAccessToken(yto.accessToken);
                        ListMySubscribersResultSet choreoResultSet;
                        choreoResultSet = ((ListMySubscribers)Choreo).execute();
                        tbResponse = choreoResultSet.Response;
                    }
                    else if (Choreo.GetType() == typeof(ListSearchResults))
                    {
                        ((ListSearchResults)Choreo).setAccessToken(yto.accessToken);
                        ListSearchResultsResultSet choreoResultSet;
                        choreoResultSet = ((ListSearchResults)Choreo).execute();
                        tbResponse = choreoResultSet.Response;
                    }
                    else if (Choreo.GetType() == typeof(ListVideosByID))
                    {
                        ((ListVideosByID)Choreo).setAccessToken(yto.accessToken);
                        ListVideosByIDResultSet choreoResultSet;
                        choreoResultSet = ((ListVideosByID)Choreo).execute();
                        tbResponse = choreoResultSet.Response;
                    }
                    requestsMade++;
                    return tbResponse;
                }
                catch
                {
                    if (retry == 1) return ""; //call to choreo has failed with refreshed access token so return null result
                    yto.accessToken = YTRefresh.RefreshAccessToken(yto.session, yto.client_id, yto.client_secret, yto.refreshToken);
                    yto.amDB.updateYoutubeAuth(yto.dbScope, yto.accessToken, yto.refreshToken);
                    requestsMade++;
                    retry++; //try once more
                }
            }
            return "";
        }
    }

    public class YouTubeData : YouTube
    {
        static private string ytDataScope = "https://www.googleapis.com/auth/youtube.readonly";

        public YouTubeData() : base(ytDataScope) {}


        public void getYouTubeData()
        {
            YTTemboo yttb = new YTTemboo((YouTube)this);
            ListMyChannels listMyChannelsChoreo = new ListMyChannels(session);
            string s_channels = yttb.getTembooData(listMyChannelsChoreo);

            // Execute Choreo
            /*
            ListMyChannelsResultSet listMyChannelsResults;
            try
            {
                listMyChannelsResults = listMyChannelsChoreo.execute();
                //Console.WriteLine(listMyChannelsResults.Response);
            }
            catch
            {
                Console.WriteLine("exception thrown");
                //refresh accessToken with refreshToken
                accessToken = YTRefresh.RefreshAccessToken(session, client_id, client_secret, refreshToken);

                Thread.Sleep(1000);
                //try again
                listMyChannelsChoreo.setAccessToken(accessToken);
                listMyChannelsResults = listMyChannelsChoreo.execute();
                //Console.WriteLine(listMyChannelsResults.Response);
            }

            //examine response
            string s_channels = listMyChannelsResults.Response;
             */

            JObject json_channels = JObject.Parse(s_channels);

            string no_records = (string)json_channels["pageInfo"]["totalResults"];
            string subscribers = (string)json_channels["items"][0]["statistics"]["subscriberCount"];
            string comments = (string)json_channels["items"][0]["statistics"]["commentCount"];
            string channel_id = (string)json_channels["items"][0]["id"];

            /*
            ListChannelsByID channelChereo = new ListChannelsByID(session);
            channelChereo.setAccessToken(accessToken);
            channelChereo.setChannelID(channel_id);
            ListChannelsByIDResultSet channelChereoResults;
            Thread.Sleep(1000);

            try
            {
                channelChereoResults = channelChereo.execute();
            }
            catch
            {
                accessToken = YTRefresh.RefreshAccessToken(session, client_id, client_secret, refreshToken);
                Thread.Sleep(1000);
                channelChereo.setAccessToken(accessToken);
                channelChereoResults = channelChereo.execute();
            }
            */

            ListMySubscribers listMySubscribersChoreo = new ListMySubscribers(session);
            // Set inputs
            /*
            listMySubscribersChoreo.setAccessToken(accessToken);
            listMySubscribersChoreo.setMaxResults("50");
            // Execute Choreo
            Thread.Sleep(1000);
            ListMySubscribersResultSet listMySubscribersResults = listMySubscribersChoreo.execute();
            string s_subscribers = listMySubscribersResults.Response;
            */
            listMySubscribersChoreo.setMaxResults("50");
            string s_subscribers = yttb.getTembooData(listMySubscribersChoreo);

            JObject json_subscribers = JObject.Parse(s_subscribers);
            string no_subscr = (string)json_subscribers["totalResults"];

            int k = 0;
            string subscr_name = (string)json_subscribers["items"][k]["snippet"]["title"];        
            while (subscr_name != null)
            {
                //get subscriber fields from JSON
                string googlep_id = (string)json_subscribers["items"][k]["contentDetails"]["googlePlusUserId"];
                string subscr_id = (string)json_subscribers["items"][k]["id"];
                //add to youtube_subscribers db table
                amDB.upsertYouTubeSubscriber(subscr_id, subscr_name, "", "", "Unknown", googlep_id);
                k++;

                try
                {
                    subscr_name = (string)json_subscribers["items"][k]["snippet"]["title"];
                }
                catch
                {
                    subscr_name = null;
                }
            }

            ListSearchResults listSearchResultsChoreo = new ListSearchResults(session);

            // Set inputs
            //listSearchResultsChoreo.setAccessToken(accessToken);
            listSearchResultsChoreo.setChannelID(channel_id);
            listSearchResultsChoreo.setType("video");
            listSearchResultsChoreo.setMaxResults("50");

            // Execute Choreo
            //Thread.Sleep(1000);
            //ListSearchResultsResultSet listSearchResultsResults = listSearchResultsChoreo.execute();

            string s_videos = yttb.getTembooData(listSearchResultsChoreo);//listSearchResultsResults.Response;
            JObject json_videos = JObject.Parse(s_videos);

            //Get # videos
            int i = 0;
            int num_videos = 0;
            string s_video_ids = "";
            Dictionary<string, string> d_videos = new Dictionary<string,string>();
            JArray vid_array = JArray.Parse(json_videos["items"].ToString());

            //create comma list of video ids
            foreach(JToken vt in vid_array)
            {
                string tmp = "";
                tmp = vt["id"]["videoId"].ToString();
                string title = vt["snippet"]["title"].ToString();
                d_videos.Add(tmp, title);
                if (vt == vid_array.Last)
                    s_video_ids += tmp;
                else
                    s_video_ids += tmp + ",";
                i++;
            }
            num_videos = i;

            //list video statistics
            ListVideosByID listVideosByIDChoreo = new ListVideosByID(session);

            // Set inputs
            listVideosByIDChoreo.setVideoID(s_video_ids);
            //listVideosByIDChoreo.setAccessToken(accessToken);
            listVideosByIDChoreo.setPart("statistics");

            // Execute Choreo
            //Thread.Sleep(1000); //Pause for a second to avoid API quota overuse
            //ListVideosByIDResultSet listVideosByIDResults = listVideosByIDChoreo.execute();

            //loop on videos
            //tally comments, views, likes, favorites, dislikes
            string s_statistics = yttb.getTembooData(listVideosByIDChoreo); //listVideosByIDResults.Response;
            JObject json_statistics = JObject.Parse(s_statistics);
            for (i = 0; i < num_videos; i++)
            {
                string s_video_id = (string)json_statistics["items"][i]["id"];
                string s_video_name = d_videos[s_video_id];

                int viewCount = int.Parse((string)json_statistics["items"][i]["statistics"]["viewCount"]);
                int likeCount = int.Parse((string)json_statistics["items"][i]["statistics"]["likeCount"]);
                int dislikeCount = int.Parse((string)json_statistics["items"][i]["statistics"]["dislikeCount"]);
                int favoriteCount = int.Parse((string)json_statistics["items"][i]["statistics"]["favoriteCount"]);
                int commentCount = int.Parse((string)json_statistics["items"][i]["statistics"]["commentCount"]);
                //int shareCount = fb.GetFBYTVideoShares(s_video_id);
                //shareCount += tw.getTWYTVideoShares(s_video_id);
                //Pause for a second to avoid API quota overuse
                if (i % 4 == 0) Thread.Sleep(1000);
                string country = "Unknown";
                amDB.upsertYouTubeCounts(channel_id,
                    s_video_id,
                    s_video_name,
                    viewCount,
                    likeCount,
                    dislikeCount,
                    favoriteCount,
                    0,
                    commentCount,
                    country);

            }

            amDB.youtube_aggregate();

        } //end getYouTubeData()

        public string getVideoTitleById(string video_id)
        {
            YTTemboo yttb = new YTTemboo((YouTube)this);
            ListVideosByID listVideosByIDChoreo = new ListVideosByID(session);

            // Set inputs
            listVideosByIDChoreo.setVideoID(video_id);
            listVideosByIDChoreo.setFields("items/snippet/title");
            // Execute Choreo
            //ListVideosByIDResultSet listVideosByIDResults = listVideosByIDChoreo.execute();
            string resultSet = yttb.getTembooData(listVideosByIDChoreo);
            JObject json_title = JObject.Parse(resultSet);
            string title = json_title["items"][0]["snippet"]["title"].ToString();
            return title;
        }
    }



    public class youTubeAnalytics : YouTube
    {
        private static string ytAnalyticsScope = "https://www.googleapis.com/auth/yt-analytics.readonly";
        private string YTA_URL = "https://www.googleapis.com/youtube/analytics/v1/reports?";
        private static int successiveCalls = 0;
        private string yt_metrics = "views,likes,dislikes,comments,subscribersGained";
        private string yt_sort = "day";
        private YouTubeData yt_readonly;

        public youTubeAnalytics(YouTubeData o) : base(ytAnalyticsScope)
        {
            yt_readonly = o;
        }

        private JObject callYouTube(string encURL)
        {
            GetResultSet getResults = null;
            int retry = 0;
            if (successiveCalls > 5) //throttle the calls to youtube to keep under quota
            {
                Thread.Sleep(5000);
                successiveCalls = 0;
            }
            else
                successiveCalls++;

            Get getChero = new Get(session);
            string new_encURL = encURL + "&access_token=" + accessToken;
            //string new_encURL = encURL;
            while (retry <= 1)
            {
                getChero.setURL(new_encURL);
                try
                {
                    getResults = getChero.execute();
                    break;
                }
                catch (Exception e)
                {
                    if (retry == 1)
                    {
                        Console.WriteLine(e.Message);
                        return null; //call to choreo has failed with refreshed access token so return null result
                    }
                    accessToken = YTRefresh.RefreshAccessToken(session, client_id, client_secret, refreshToken);
                    amDB.updateYoutubeAuth(dbScope, accessToken, refreshToken);
                    new_encURL = encURL + "&access_token=" + accessToken;
                    successiveCalls++;
                    retry++; //try once more
                }
            }
            string anaData = getResults.Response;
            JObject o = JObject.Parse(anaData);
            return o;
        }

        private JObject getYTAnalyticsByCountry(string video_id, 
            DateTime dStartDate, 
            DateTime dEndDate)
        {
            string startDate = dStartDate.ToString("yyyy-MM-dd");
            string endDate = dEndDate.ToString("yyyy-MM-dd");
            string metrics = WebUtility.UrlEncode("views");
            string dimensions = "country";
            string sort = "-views";
            string channel_id = "MINE";
            //string ids = "channel%3D%3D"+channel_id;
            string filters = WebUtility.UrlEncode("video==" + video_id);
            string ids = WebUtility.UrlEncode("channel==" + channel_id);
            string fullURL = YTA_URL+
                "ids="+ids+
                "&start-date="+startDate+
                "&end-date="+endDate+
                "&metrics="+metrics+
                "&sort="+sort+
                "&filter="+filters+
                "&dimensions="+dimensions
                //+"&access_token="+accessToken
                ;

            //string encFullURL = WebUtility.UrlEncode(fullURL);
            JObject jsonCountry = callYouTube(fullURL);
            //Console.WriteLine(jsonCountry.ToString());
            return jsonCountry;
        }   

        private JObject getYTAnalyticsByDate(
            string video_id, 
            string countryCode, 
            DateTime dStartDate, 
            DateTime dEndDate,
            string metrics,
            string sort)
        {
            metrics = WebUtility.UrlEncode(metrics);
            string startDate = dStartDate.ToString("yyyy-MM-dd");
            string endDate = dEndDate.ToString("yyyy-MM-dd");
            string dimensions = "day";
            string channel_id = "MINE";
            //string ids = "channel%3D%3D"+channel_id;
            string filters = WebUtility.UrlEncode("video==" + video_id + ";" + "country==" + countryCode);
            string ids = WebUtility.UrlEncode("channel==" + channel_id);
            string fullURL = YTA_URL +
                "ids=" + ids +
                "&start-date=" + startDate +
                "&end-date=" + endDate +
                "&metrics=" + metrics +
                "&sort=" + sort +
                "&filters=" + filters +
                "&dimensions=" + dimensions 
                //+"&access_token=" + accessToken
                ;

            JObject jsonDate = callYouTube(fullURL);
            //Console.WriteLine(jsonDate.ToString());
            return jsonDate;
        }

        public JObject getYTVideos(DateTime dStart, DateTime dEnd)
        {
            string startDate = dStart.ToString("yyyy-MM-dd");
            string endDate = dEnd.ToString("yyyy-MM-dd");
            string metrics = WebUtility.UrlEncode("views");
            string dimensions = "video";
            string sort = "-views";
            string channel_id = "MINE";
            //string ids = "channel%3D%3D"+channel_id;
            string ids = WebUtility.UrlEncode("channel==" + channel_id);
            string fullURL = YTA_URL +
                "ids=" + ids +
                "&start-date=" + startDate +
                "&end-date=" + endDate +
                "&metrics=" + metrics +
                "&sort=" + sort +
                "&max-results=200" +  //This is the max allowable by YouTube Analytics.
                "&dimensions=" + dimensions 
                //+"&access_token=" + accessToken
                ;

            JObject jsonVids = callYouTube(fullURL);
            //Console.WriteLine(jsonVids.ToString());
            return jsonVids;
        }

        public void backFillYTAna(DateTime dStartDate, DateTime dEndDate)
        {
            //get video list, store
            JObject jsonYA = getYTVideos(dStartDate, dEndDate);
            string s_jsonYA = jsonYA.ToString();
            JArray jaYA;
            try
            {
                jaYA = JArray.Parse(jsonYA["rows"].ToString());
            }
            catch
            {
                Console.WriteLine("No videos found.");
                return;
            }
            //get video ids and names
            Dictionary<string, string> vidDic = amDB.getVideoNames();
            //loop on video to get countries
            foreach (JToken vid in jaYA)
            {
                JArray jaVid = JArray.Parse(vid.ToString());
                string video_name = "";
                string video_id = jaVid[0].ToString(); //grabbing the first json array item since YouTube won't return records without metrics
                try
                {
                    video_name = vidDic[video_id];
                }
                catch
                {
                    video_name = yt_readonly.getVideoTitleById(video_id);
                }
                JObject jsonCountry = getYTAnalyticsByCountry(video_id, dStartDate, dEndDate);
                JArray jaCtrys = JArray.Parse(jsonCountry["rows"].ToString());
                //loop on countries to get dates
                foreach (JToken cntry in jaCtrys)
                {
                    JArray jaCntrys = JArray.Parse(cntry.ToString());
                    string country_id = jaCntrys[0].ToString();
                    JObject jsonDates = getYTAnalyticsByDate(video_id, country_id, dStartDate, dEndDate, yt_metrics, yt_sort);
                    try
                    {
                        string tmp = jsonDates["rows"].ToString();
                        JArray dates = JArray.Parse(jsonDates["rows"].ToString());

                        foreach (JToken date in dates)
                        {
                            //Insert record into DB
                            JArray ytDate = JArray.Parse(date.ToString());
                            DateTime collect_date = DateTime.Parse(ytDate[0].ToString());
                            int views = int.Parse(ytDate[1].ToString());
                            int likes = int.Parse(ytDate[2].ToString());
                            int dislikes = int.Parse(ytDate[3].ToString());
                            int comments = int.Parse(ytDate[4].ToString());
                            int subscrGained = int.Parse(ytDate[5].ToString());
                            int j = amDB.insertYTAnalytics(
                                video_id,
                                video_name,
                                DateTime.Now,
                                dStartDate,
                                dEndDate,
                                country_id,
                                collect_date,
                                views,
                                likes,
                                dislikes,
                                comments,
                                subscrGained);
                            if (j == -2) throw new DBException("DB failed in YT Analytics " + video_id + ":" + country_id + ":" + collect_date.ToString());
                        }
                        Console.WriteLine(video_id + ":" + country_id);

                    }
                    catch
                    {
                        Console.WriteLine("No data for " + video_id + ":" + country_id);
                    }
                    
                }
            }
            amDB.CommitAnalytics();
        }
    }
}
