using System;
using System.IO;
using System.Collections;
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
using Temboo.Library.Facebook.OAuth;
using Temboo.Library.Facebook.Searching;
using Temboo.Library.Facebook.Reading;

namespace KIP_Social_Pull
{
    public class DBException : Exception
    {
        public DBException(string message)
            : base(message) { }
    }

    public class Facebook
    {
        String app_id = "";
        String app_secret = "";
        String callBackId = "";
        String accessToken = "";
        String pageURL = "https://www.facebook.com/keepitpumpingmovement";
        TembooSession session;
        AwarenessMapDB amDB;

        public Facebook()
        {
            // Instantiate the Choreo, using a previously instantiated TembooSession object, eg:
            var appSettings = ConfigurationManager.AppSettings;
            //session = new TembooSession(appSettings.Get("Temboo_account"), 
            //    appSettings.Get("Temboo_application"), 
            //    appSettings.Get("Temboo_application_key"));
            session = new TembooSession("keepitpumping", "KeepItPumping-Awareness-Map", "692238482e2b4bc7b62d09234193c202");
            app_id = appSettings.Get("Facebook_app_id");
            app_secret = appSettings.Get("Facebook_app_secret");

            amDB = new AwarenessMapDB();
            amDB.getFacebookAuth(ref accessToken);
            if (accessToken == "")
            {
                InitializeOAuth initializeOAuthChoreo = new InitializeOAuth(session);

                // Set inputs
                initializeOAuthChoreo.setAppID(app_id);
                initializeOAuthChoreo.setScope("public_profile, email, user_about_me, user_friends, user_likes, user_status," +
                    "publish_pages, publish_actions, read_insights,user_managed_groups, user_status, user_videos," +
                    "user_website, manage_pages, read_insights");

                // Execute Choreo
                InitializeOAuthResultSet initializeOAuthResults = initializeOAuthChoreo.execute();

                callBackId = initializeOAuthResults.CallbackID;

                Process.Start(initializeOAuthResults.AuthorizationURL);
                Thread.Sleep(10000);

                FinalizeOAuth finalizeOAuthChoreo = new FinalizeOAuth(session);

                // Set inputs
                finalizeOAuthChoreo.setCallbackID(callBackId);
                finalizeOAuthChoreo.setAppSecret(app_secret);
                finalizeOAuthChoreo.setAppID(app_id);

                // Execute Choreo
                FinalizeOAuthResultSet finalizeOAuthResults = finalizeOAuthChoreo.execute();
                accessToken = finalizeOAuthResults.AccessToken;
                amDB.updateFacebookAuth(accessToken);
            }
        }

        //Processing delegate for realtime data of historical
        public delegate void ProcessFBDelegate(string name_token, 
            string period,
            DateTime pull_date,
            JToken json_insight, 
            bool running_total, 
            bool Geo_total);

        public void getFacebookData()
        {
            URLLookup uRLLookupChoreo = new URLLookup(session);

            // Set inputs
            uRLLookupChoreo.setAccessToken(accessToken);
            uRLLookupChoreo.setIDs(pageURL);

            // Execute Choreo
            URLLookupResultSet uRLLookupResults = uRLLookupChoreo.execute();

            // Print results
            //Console.WriteLine(uRLLookupResults.Response);
            string s_lookup = uRLLookupResults.Response;
            JObject json_lookup = JObject.Parse(s_lookup);

            string page_id = (string)json_lookup[pageURL]["id"];
            Console.WriteLine("page ID = " + page_id);

            GetObject getObjectChoreo = new GetObject(session);

            // Set inputs
            getObjectChoreo.setAccessToken(accessToken);
            getObjectChoreo.setFields("insights");
            getObjectChoreo.setObjectID(page_id);

            // Execute Choreo
            GetObjectResultSet getObjectResults = getObjectChoreo.execute();

            // Print results
            //Console.WriteLine(getObjectResults.Response);

            JObject json_insight = JObject.Parse(getObjectResults.Response);
            string name_token = (string)json_insight["insights"]["data"][0]["name"];
            int i = 0;
            DateTime pull_date = DateTime.Now;

            ProcessFBDelegate pFBDel = new ProcessFBDelegate(processFBField);
            processInsight(pFBDel, json_insight, pull_date);

            //Console.WriteLine("found data = " + saved_fields);
        }

        public void getFBHistorical(String fileLocName)
        {
            //Read JSON file
            JObject json_insight = JObject.Parse(File.ReadAllText(fileLocName));
            ProcessFBDelegate pFBHist = new ProcessFBDelegate(processFBHistory);
            DateTime pull_date = DateTime.Now;
            //processInsight(pFBHist, json_insight, pull_date);
            processInsightNewPoints(pFBHist, json_insight, pull_date);
        }

        private void processInsight(ProcessFBDelegate processFBRec, JObject json_insight, DateTime pull_date)
        {
            string name_token = "";
            string period = "";
            int i = 0;
            JArray insightArr = JArray.Parse(json_insight["insights"]["data"].ToString());

            foreach(JToken ins in insightArr)
            //while (name_token != null)
            {
                name_token = ins["name"].ToString();
                switch (name_token)
                {
                    case "page_fans":
                        period = ins["period"].ToString();
                        if (period == "lifetime")
                        {
                            var running_total = false;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;

                    case "page_positive_feedback_by_type":
                        period = ins["period"].ToString();
                        if (period == "day")
                        {
                            var running_total = true;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;

                    case "page_stories_by_story_type":
                        period = ins["period"].ToString();
                        if (period == "day")
                        {
                            var running_total = true;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;

                    case "page_video_views":
                        period = ins["period"].ToString();
                        if (period == "day") //|| period == "days_28")
                        {
                            var running_total = false;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;

                    case "page_impressions":
                        period = ins["period"].ToString();
                        if (period == "day")
                        {
                            var running_total = true;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;

                    case "page_impressions_unique":
                        period = ins["period"].ToString();;
                        if (period == "day")
                        {
                            var running_total = true;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;

                    case "page_consumptions":
                        period = ins["period"].ToString();
                        if (period == "day")
                        {
                            var running_total = true;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;

                    case "page_consumptions_unique":
                        period = ins["period"].ToString();
                        if (period == "day")
                        {
                            var running_total = true;
                            var geo_total = true;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        if (period == "days_28")
                        {
                            period = "28_days";
                            var running_total = false;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;
                    case "page_impressions_by_country_unique":
                        period = ins["period"].ToString();
                        if (period == "day")
                        {
                            var running_total = true;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;
                    case "page_story_adds_by_country_unique":
                        period = ins["period"].ToString();
                        if (period == "day")
                        {
                            var running_total = true;
                            var geo_total = true;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;
                    case "page_fans_country":
                        period = ins["period"].ToString();
                        if (period == "lifetime")
                        {
                            var running_total = false;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;
                }

            }

        }

        private void processInsightNewPoints(ProcessFBDelegate processFBRec, JObject json_insight, DateTime pull_date)
        {
            string name_token = "";
            string period = "";
            int i = 0;
            JArray insightArr = JArray.Parse(json_insight["insights"]["data"].ToString());

            foreach (JToken ins in insightArr)
            //while (name_token != null)
            {
                name_token = ins["name"].ToString();
                switch (name_token)
                {
                    case "page_impressions_by_country_unique":
                        period = ins["period"].ToString();
                        if (period == "day")
                        {
                            var running_total = true;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;

                    case "page_story_adds_by_country_unique":
                        period = ins["period"].ToString();
                        if (period == "day")
                        {
                            var running_total = true;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;

                    case "page_storytellers_by_country":
                        period = ins["period"].ToString();
                        if (period == "day") //|| period == "days_28")
                        {
                            var running_total = true;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;

                    case "page_fans_country":
                        period = ins["period"].ToString();
                        if (period == "lifetime")
                        {
                            var running_total = false;
                            var geo_total = false;
                            processFBRec(name_token, period, pull_date, ins, running_total, geo_total);
                        }
                        break;
                }

            }

        }



        private void processFBHistory(string name_token, 
            string period,
            DateTime pull_date,
            JToken json_insight, 
            bool running_total, 
            bool Geo_total)
        {
            //For each field
            //  Find existing field
            //  Found?
            //      Skip to next date
            //  Not found?
            //      Insert into DB
            //      Carry forward inserted record
            //          Cursor on field_name, category, total_status order by as_of_date
            //              Add new field value to existing
            //              update field

            int i = 0;
            string total_status = "";
            if (running_total && Geo_total)
                total_status = "RUNNING";
            else if (running_total)
                total_status = "run";
            else
                total_status = period;

            string field_title = json_insight["title"].ToString();

            //JObject t_obj = getLastToken(json_insight["values"]);
            JArray t_arr = JArray.Parse(json_insight["values"].ToString());

            foreach(JToken t_obj in t_arr)
            {
                var end_time = t_obj["end_time"].ToString();
                //is there a category?

                var dt_end = DateTime.Parse(DateTime.Parse(end_time).ToShortDateString());
                string s = (t_obj["value"]).ToString();
                Console.WriteLine("field=" + field_title);
                int j = 0;
                int field_value = 0;
                Dictionary<string, string> value_cats = new Dictionary<string,string>();

                try
                {
                    value_cats = JsonConvert.DeserializeObject<Dictionary<string, string>>(s);
                }
                catch
                {
                    field_value = int.Parse(s);
                }
                
                if (value_cats.Count > 0 && (total_status == "RUNNING" || total_status == "run"))
                {
                    foreach (var item in value_cats)
                    {
                        int db_field_value = amDB.findFBDBRecord(name_token, period, total_status, 
                            item.Key, dt_end, ref field_title);
                        if (db_field_value >= 0)
                        {
                            //Insert new record and carry the value forward to any future records
                            amDB.InsertHistInsight(
                                DateTime.Now,
                                name_token,
                                period,
                                field_title,
                                total_status,
                                item.Key,
                                dt_end,
                                db_field_value,
                                int.Parse(item.Value));
                            Console.WriteLine("category=" + item.Key + ":value=" + item.Value);
                            var a = item.Key;
                        }
                        else if (db_field_value == -2)
                        {
                            //throw an exception
                            throw new DBException("DB failed in FB History " + name_token);
                        }
                    }
                }
                else if (value_cats.Count > 0)
                {
                    foreach (var item in value_cats)
                    {
                        //Insert record with no added processing
                        int db_field_value = amDB.findFBDBRecord(name_token, period, total_status, item.Key, dt_end, ref field_title);
                        if (db_field_value >= 0)
                        {
                            //Insert new record
                            amDB.InsertHistInsight(
                                DateTime.Now,
                                name_token,
                                period,
                                field_title,
                                total_status,
                                item.Key,
                                dt_end,
                                int.Parse(item.Value));
                            Console.WriteLine("category =" + item.Key + ":value=" + item.Value);
                            var a = item.Key;
                        }
                        else if (db_field_value == -2)
                        {
                            //throw an exception
                            throw new DBException("DB failed in FB History " + name_token);
                        }
                    }
                }
                else if (total_status == "RUNNING" || total_status == "run") 
                {
                    int db_field_value = amDB.findFBDBRecord(name_token, period, total_status, null, dt_end, ref field_title);
                    if (db_field_value >= 0)
                    {
                        //Insert new record and carry the value forward to any future records
                        amDB.InsertHistInsight(
                            DateTime.Now,
                            name_token,
                            period,
                            field_title,
                            total_status,
                            "??",
                            dt_end,
                            db_field_value,
                            field_value);
                        Console.WriteLine("value=" + field_value);
                        var a = field_value;
                    }
                    else if (field_value == -2)
                    {
                        //throw an exception
                        throw new DBException("DB failed in FB History " + name_token);                    
                    }
                }
                else
                {
                    //Insert record with no added processing
                    int db_field_value = amDB.findFBDBRecord(name_token, period, total_status, null, dt_end, ref field_title);
                    if (db_field_value >= 0)
                    {
                        //Insert new record and carry the value forward to any future records
                        amDB.InsertHistInsight(
                            DateTime.Now,
                            name_token,
                            period,
                            field_title,
                            total_status,
                            "??",
                            dt_end,
                            field_value);
                        Console.WriteLine("value=" + field_value);
                        var a = field_value;
                    }
                    else if (field_value == -2)
                    {
                        //throw an exception
                        throw new DBException("DB failed in FB History "+name_token);
                    }
                }
            }
        }

        private void processFBField(string name_token,
            string period,
            DateTime pull_date,
            JToken json_insight,
            bool running_total,
            bool Geo_total)
        {

            int i = 0;
            string total_status = "";
            if (running_total && Geo_total)
                total_status = "RUNNING";
            else if (running_total)
                total_status = "run";
            else
                total_status = period;

            //JObject t_obj = getLastToken(json_insight["values"]);
            JArray t_arr = JArray.Parse(json_insight["values"].ToString());

            foreach (JToken t_obj in t_arr)
            {
                string field_title = (string)json_insight["title"];
                //t_obj = getLastToken(json_insight["values"]);
                var end_time = (t_obj["end_time"]).ToString();
                var dt_end = DateTime.Parse(DateTime.Parse(end_time).ToShortDateString());
                var last_date = amDB.facebook_last_update(name_token, period, total_status);
                if (dt_end > last_date)
                {
                    string s = (t_obj["value"]).ToString();
                    Console.WriteLine("field=" + name_token + ":" + field_title);
                    int j = 0;

                    try
                    {
                        var value_cats = JsonConvert.DeserializeObject<Dictionary<string, string>>(s);
                        if (value_cats.Count > 0)
                        {
                            foreach (var item in value_cats)
                            {
                                //insert each record
                                Console.Write("category=" + item.Key);
                                j = amDB.facebook_insight_insert(pull_date,
                                    name_token, period,
                                    field_title,
                                    int.Parse(item.Value),
                                    total_status,
                                    item.Key,
                                    dt_end,
                                    last_date);
                                //in the event of a database issue, display the error, exit nicely.
                                if (j == -2) throw new DBException("DB failed in FB Insight " + name_token);
                                Console.WriteLine(":value=" + item.Value);
                            }
                        }
                    }
                    catch
                    {
                        Console.Write("category=??");
                        j = amDB.facebook_insight_insert(pull_date,
                            name_token,
                            period,
                            field_title,
                            int.Parse(s),
                            total_status,
                            "??",
                            dt_end,
                            last_date);
                        //in the event of a database issue, display the error, exit nicely.
                        if (j == -2) throw new DBException("DB failed in FB Insight " + name_token);
                        Console.WriteLine(":value=" + s);
                    }
                    amDB.update_insights_commit(name_token, period);
                }
            }
        }

        private JObject getLastToken(JToken t_jo)
        {
            JToken t = t_jo;
            JToken t_last = t.Last;
            return (JObject)t_last;
        }

        private void getJsonItems(string filename)
        {
            URLLookup uRLLookupChoreo = new URLLookup(session);

            // Set inputs
            uRLLookupChoreo.setAccessToken(accessToken);
            uRLLookupChoreo.setIDs(pageURL);

            // Execute Choreo
            URLLookupResultSet uRLLookupResults = uRLLookupChoreo.execute();

            // Print results
            //Console.WriteLine(uRLLookupResults.Response);
            string s_lookup = uRLLookupResults.Response;
            JObject json_lookup = JObject.Parse(s_lookup);

            string page_id = (string)json_lookup[pageURL]["id"];
            Console.WriteLine("page ID = " + page_id);

            GetObject getObjectChoreo = new GetObject(session);

            // Set inputs
            getObjectChoreo.setAccessToken(accessToken);
            getObjectChoreo.setFields("insights");
            getObjectChoreo.setObjectID(page_id);

            // Execute Choreo
            GetObjectResultSet getObjectResults = getObjectChoreo.execute();

            string search_it = getObjectResults.Response;
            JObject j_search_it = JObject.Parse(search_it);
            System.IO.StreamWriter file = new System.IO.StreamWriter(filename);

            int i = 0;
            string name_token = (string)j_search_it["insights"]["data"][0]["name"];

            while (name_token != null)
            {
                file.WriteLine("name: " + (string)j_search_it["insights"]["data"][i]["name"]);
                file.WriteLine("title: " + (string)j_search_it["insights"]["data"][i]["title"]);
                file.WriteLine("description: " + (string)j_search_it["insights"]["data"][i]["description"]);
                file.WriteLine("");
                i++;
                try
                {
                    name_token = (string)j_search_it["insights"]["data"][i]["name"];
                }
                catch
                {
                    name_token = null;
                }

            }
            file.Close();

        }

        // find youtube videos on Facebook to determine how many times they have been shared
        public int GetFBYTVideoShares(string video_id)
        {
            URLLookup uRLLookupChoreo = new URLLookup(session);

            // Set inputs
            uRLLookupChoreo.setAccessToken(accessToken);
            string video_url = "https://www.youtube.com/watch?v=" + video_id;
            uRLLookupChoreo.setIDs(video_url);

            // Execute Choreo
            URLLookupResultSet uRLLookupResults = uRLLookupChoreo.execute();
            JObject json_response = JObject.Parse(uRLLookupResults.Response);
            string s_shares = "";
            try
            {
                s_shares = (string)json_response[video_url]["share"]["share_count"];
            }
            catch
            {
                s_shares = "0";
            }
            return int.Parse(s_shares);
        }

    }
}
