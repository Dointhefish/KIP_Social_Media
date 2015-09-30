using System;
using System.IO;
using System.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using System.Data.Sql;
using System.Data.SqlClient;

namespace KIP_Social_Pull
{
    public class AwarenessMapDB
    {
        string ConnectionString = "";
        string facebook_raw_table = "facebook_raw_insights";
        //string facebook_insight_table = "facebook_insights_newpoints";
        string facebook_insight_table = "facebook_insights_committed";
        string FB_Commit_SP = "Commit_Facebook_Data";
        //string facebook_raw_table = "facebook_raw_test";
        //string facebook_insight_table = "facebook_insights_bad";
        //string FB_Commit_SP = "Commit_Facebook_Test";
        string youtube_analytics_table = "youtube_analytics";
        string youtube_committed = "youtube_committed";

        public AwarenessMapDB() 
        {
            var connects = ConfigurationManager.ConnectionStrings;
            ConnectionString = connects["AWS_map"].ToString();
        }

        /*
         * Authentication
         */

        //Twitter
        public void getTwitterAuth(ref string access_token, ref string access_secret)
        {
            string selectStmt1 = "SELECT credential_value FROM KIP_credentials WHERE " +
                "media_system = 'Twitter' AND credential_name = 'access_token';";
            string selectStmt2 = "SELECT credential_value FROM KIP_credentials WHERE " +
                "media_system = 'Twitter' AND credential_name = 'access_token_secret';";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(selectStmt1, conn);
            try
            {
                conn.Open();
                SqlDataReader reader = command.ExecuteReader();
                reader.Read();
                access_token = reader[0].ToString();
                conn.Close();
                command = new SqlCommand(selectStmt2, conn);
                conn.Open();
                reader = command.ExecuteReader();
                reader.Read();
                access_secret = reader[0].ToString();
                conn.Close();
            }
            catch
            {
                access_secret = "";
                access_token = "";
            }
        
        }

        public int updateTwitterAuth(string p_access_token, string p_access_secret)
        {
            string updateStmt1 = "UPDATE KIP_credentials " +
                "SET credential_value = @p_access_token " +
                "WHERE media_system = 'Twitter' AND credential_name = 'access_token';";
            string updateStmt2 = "UPDATE KIP_credentials " +
                "SET credential_value = @p_access_secret " +
                "WHERE media_system = 'Twitter' AND credential_name = 'access_token_secret';";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(updateStmt1, conn);
            command.Parameters.AddWithValue("@p_access_token", p_access_token);
            conn.Open();
            int row_count = command.ExecuteNonQuery();
            conn.Close();
            command = new SqlCommand(updateStmt2, conn);
            command.Parameters.AddWithValue("@p_access_secret", p_access_secret);
            conn.Open();
            row_count += command.ExecuteNonQuery();
            conn.Close();
            return row_count;
        }

        //Facebook
        public void getFacebookAuth(ref string access_token)
        {
            string selectStmt1 = "SELECT credential_value FROM KIP_credentials WHERE " +
                "media_system = 'Facebook' AND credential_name = 'access_token';";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(selectStmt1, conn);
            try
            {
                conn.Open();
                SqlDataReader reader = command.ExecuteReader();
                reader.Read();
                access_token = reader[0].ToString();
                conn.Close();
            }
            catch
            {
                access_token = "";
            }

        }

        public int updateFacebookAuth(string p_access_token)
        {
            string updateStmt1 = "UPDATE KIP_credentials " +
                "SET credential_value = @p_access_token " +
                "WHERE media_system = 'Facebook' AND credential_name = 'access_token';";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(updateStmt1, conn);
            command.Parameters.AddWithValue("@p_access_token", p_access_token);
            conn.Open();
            int row_count = command.ExecuteNonQuery();
            conn.Close();
            return row_count;
        }

        //Youtube
        public void getYoutubeAuth(string scope, ref string access_token, ref string refresh_token)
        {
            string selectStmt1 = "SELECT credential_value FROM KIP_credentials WHERE " +
                "media_system = '" + scope + "' AND credential_name = 'access_token';";
            string selectStmt2 = "SELECT credential_value FROM KIP_credentials WHERE " +
                "media_system = '" + scope + "' AND credential_name = 'refresh_token';";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(selectStmt1, conn);
            try
            {
                conn.Open();
                SqlDataReader reader = command.ExecuteReader();
                reader.Read();
                access_token = reader[0].ToString();
                conn.Close();
                command = new SqlCommand(selectStmt2, conn);
                conn.Open();
                reader = command.ExecuteReader();
                reader.Read();
                refresh_token = reader[0].ToString();
                conn.Close();
            }
            catch
            {
                refresh_token = "";
                access_token = "";
            }

        }

        public int updateYoutubeAuth(string scope, string p_access_token, string p_refresh_token)
        {
            string updateStmt1 = "UPDATE KIP_credentials " +
                "SET credential_value = @p_access_token " +
                "WHERE media_system = '" + scope + "' AND credential_name = 'access_token';";
            string updateStmt2 = "UPDATE KIP_credentials " +
                "SET credential_value = @p_refresh_token " +
                "WHERE media_system = '" + scope + "' AND credential_name = 'refresh_token';";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(updateStmt1, conn);
            command.Parameters.AddWithValue("@p_access_token", p_access_token);
            conn.Open();
            int row_count = command.ExecuteNonQuery();
            conn.Close();
            command = new SqlCommand(updateStmt2, conn);
            command.Parameters.AddWithValue("@p_refresh_token", p_refresh_token);
            conn.Open();
            row_count += command.ExecuteNonQuery();
            conn.Close();
            return row_count;
        }


        /*
         * YouTube
         */

        public int upsertYouTubeCounts(
            string  p_channel_id,
            string  p_video_id,
            string  p_video_name,
            int     p_viewCount,
            int     p_likeCount,
            int     p_dislikeCount,
            int     p_favoriteCount,
            int     p_shareCount,
            int     p_commentCount,
            string  p_country)
        {

            string first_query = "INSERT INTO youtube_raw_counts (" +
                "channel_id, " + 
                "video_id, " + 
                "video_name, " +
                "viewCount, " + 
                "likeCount, " +
                "dislikeCount," +
                "favoriteCount, " +
                "shareCount, " +
                "commentCount, " +
                "lastSampleDate, " +
                "country) " +
                "values " +
                "(@p_channel_id, " +
                "@p_video_id, " +
                "@p_video_name, " +
                "@p_viewCount, " +
                "@p_likeCount, " +
                "@p_dislikeCount, " +
                "@p_favoriteCount, " + 
                "@p_shareCount, " +
                "@p_commentCount, " +
                "@right_now, " +
                "@p_country);";

            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(first_query, conn);
            command = new SqlCommand(first_query, conn);
            command.Parameters.AddWithValue("@p_video_id", p_video_id);
            command.Parameters.AddWithValue("@p_channel_id", p_channel_id);
            command.Parameters.AddWithValue("@p_video_name", p_video_name);
            command.Parameters.AddWithValue("@p_viewCount", p_viewCount);
            command.Parameters.AddWithValue("@p_likeCount", p_likeCount);
            command.Parameters.AddWithValue("@p_dislikeCount", p_dislikeCount);
            command.Parameters.AddWithValue("@p_favoriteCount", p_favoriteCount);
            command.Parameters.AddWithValue("@p_shareCount", p_shareCount);
            command.Parameters.AddWithValue("@p_commentCount", p_commentCount);
            command.Parameters.AddWithValue("@right_now", DateTime.Now);
            command.Parameters.AddWithValue("@p_country", p_country);
            conn.Open();
            int row_count = command.ExecuteNonQuery();
            conn.Close();

            return row_count;
        }

        public int upsertYouTubeSubscriber(
        string p_subscriber_id,
        string p_name,
        string p_city,
        string p_state,
        string p_country,
        string p_google_id)
        {

            string first_query = "SELECT subscriber_id FROM youtube_subscribers WHERE subscriber_id = @p_subscriber_id;";
            string second_query = "";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(first_query, conn);
            command.Parameters.AddWithValue("@p_subscriber_id", p_subscriber_id);
            try
            {
                conn.Open();
                SqlDataReader reader = command.ExecuteReader();
                reader.Read();
                string s_video_name = reader[0].ToString();
                second_query = "UPDATE youtube_subscribers SET subscriber_id = @p_subscriber_id, " +
                    "name = @p_name, " +
                    "city = @p_city, " +
                    "state = @p_state, " +
                    "country = @p_country, " +
                    "dateAdded = @right_now, " +
                    "google_id = @p_google_id " +
                    "where subscriber_id = @p_subscriber_id;";
                conn.Close();
            }
            catch (Exception ex)
            {
                //assuming no records returned exception
                conn.Close();
                Console.WriteLine("Adding " + p_name);
                second_query = "INSERT INTO youtube_subscribers (" +
                    "subscriber_id, " +
                    "name, " +
                    "city, " +
                    "state, " +
                    "dateAdded, " +
                    "country, " +
                    "google_id) " +
                    "values " +
                    "(@p_subscriber_id, " +
                    "@p_name, " +
                    "@p_city, " +
                    "@p_state, " +
                    "@right_now, " +
                    "@p_country, " +
                    "@p_google_id " +
                    ");";
            }

            command = new SqlCommand(second_query, conn);
            command.Parameters.AddWithValue("@p_subscriber_id", p_subscriber_id);
            command.Parameters.AddWithValue("@p_name", p_name);
            command.Parameters.AddWithValue("@p_city", p_city);
            command.Parameters.AddWithValue("@p_state", p_state);
            command.Parameters.AddWithValue("@p_country", p_country);
            command.Parameters.AddWithValue("@p_google_id", p_google_id);
            command.Parameters.AddWithValue("@right_now", DateTime.Now);
            conn.Open();
            int row_count = command.ExecuteNonQuery();
            conn.Close();

            return row_count;
        }


        public void youtube_aggregate()
        {
            string first_query = "EXEC Aggregate_YT_Counts;";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(first_query, conn);
            try
            {
                conn.Open();
                command.ExecuteNonQuery();
                conn.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Stored procedure execution failed.");
                conn.Close();
            }
        }

        /*
         * Twitter
         */

        public int upsertTwitterRawCounts(
            string  p_tweet_id,
            string  p_tweet_text,
            string  p_user_name,
            string  p_user_id,
            string  p_profile_image_url,
            string  p_location,
            string  p_favorite_count,
            decimal p_utc_offset,
            string  p_place_name,
            string  p_country_code,
            decimal p_lattitude,
            decimal p_longitude,
            int p_followers,
            DateTime p_tweet_created_date,
            string p_hashtag)
        {
            int row_count = 0;
            int return_code = 0;
            string first_query = "SELECT tweet_created_date FROM twitter_raw_counts WHERE tweet_id = @p_tweet_id;";
            string second_query = "";
            string s_tw_created_date;
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(first_query, conn);
            command.Parameters.AddWithValue("@p_tweet_id", p_tweet_id);
            try
            {
                conn.Open();
                SqlDataReader reader = command.ExecuteReader();
                reader.Read(); //if exception is thrown, insert record

                s_tw_created_date = reader[0].ToString();
                DateTime d_tw_created_date = DateTime.Parse(s_tw_created_date);
                //if (d_tw_created_date < p_tweet_created_date) // if this existing tweet is newer, ignore what's incoming
                //{
                    second_query = "UPDATE twitter_raw_counts SET " +
                    "tweet_text = @p_tweet_text," +
                    "user_name = @p_user_name," +
                    "user_id = @p_user_id," +
                    "profile_image_url = @p_profile_image_url," +
                    "location = @p_location," +
                    "utc_offset = @p_utc_offset," +
                    "favorite_count = @p_favorite_count," +
                    "place_name = @p_place_name," +
                    "country_code = @p_country_code," +
                    "lattitude = @p_lattitude, " +
                    "longitude = @p_longitude, " +
                    "updated_date = @right_now, " +
                    "followers = @p_followers, " +
                    //"tweet_created_date = @p_tweet_created_date, " +
                    "hashtag = @p_hashtag, " +
                    "counted = 0 " +
                    "WHERE tweet_id = @p_tweet_id " +
                    ";";
                    
                //}
                //else
                //{
                //    conn.Close();
                //    return 1;
                //}
            }
            catch (Exception ex)
            {
                second_query = "INSERT INTO twitter_raw_counts (" +
                "tweet_id," +
                "tweet_text," +
                "user_name," +
                "user_id," +
                "profile_image_url," +
                "location," +
                "utc_offset," +
                "favorite_count," +
                "place_name," +
                "country_code, " +
                "lattitude, " +
                "longitude, " +
                "followers, " +
                "created_date," +
                "tweet_created_date, " +
                "counted, " +
                "hashtag)" +
                "values (" +
                "@p_tweet_id," +
                "@p_tweet_text," +
                "@p_user_name," +
                "@p_user_id," +
                "@p_profile_image_url," +
                "@p_location," +
                "@p_utc_offset," +
                "@p_favorite_count," +
                "@p_place_name," +
                "@p_country_code, " +
                "@p_lattitude, " +
                "@p_lattitude," +
                "@p_followers," +
                "@right_now," +
                "@p_tweet_created_date, " +
                "0," +
                "@p_hashtag " +
                ");";
                return_code = 1;
            }

            conn.Close();
            command = new SqlCommand(second_query, conn);
            command.Parameters.AddWithValue("@right_now", DateTime.Now);
            command.Parameters.AddWithValue("@p_tweet_id", p_tweet_id);
            command.Parameters.AddWithValue("@p_tweet_text", p_tweet_text);
            command.Parameters.AddWithValue("@p_user_name", p_user_name);
            command.Parameters.AddWithValue("@p_user_id", p_user_id);
            command.Parameters.AddWithValue("@p_profile_image_url", p_profile_image_url);
            command.Parameters.AddWithValue("@p_location", p_location);
            command.Parameters.AddWithValue("@p_utc_offset" , p_utc_offset);
            command.Parameters.AddWithValue("@p_favorite_count", p_favorite_count);
            command.Parameters.AddWithValue("@p_place_name" ,p_place_name);
            command.Parameters.AddWithValue("@p_country_code" ,p_country_code);
            command.Parameters.AddWithValue("@p_lattitude", p_lattitude);
            command.Parameters.AddWithValue("@p_longitude", p_longitude);
            command.Parameters.AddWithValue("@p_followers", p_followers);
            command.Parameters.AddWithValue("@p_tweet_created_date", p_tweet_created_date);
            command.Parameters.AddWithValue("@p_hashtag", p_hashtag);
            conn.Open();
            row_count = command.ExecuteNonQuery();
            conn.Close();

            return return_code;
        }

        public int upsertRetweetRawCounts(
            string p_tweet_id,
            string p_user_name,
            string p_user_id,
            string p_profile_image_url,
            decimal p_utc_offset,
            int p_retweet_count,
            string p_place_name,
            string p_country_code,
            decimal p_lattitude,
            decimal p_longitude,
            int p_followers,
            int p_ext_followers,
            DateTime p_rt_created_date,
            string p_hashtag)
        {
            int return_code = 0;
            int row_count = 0;
            string first_query = "SELECT retweet_created_date FROM retweet_raw_counts WHERE tweet_id = @p_tweet_id;";
            string second_query = "";
            string s_rt_created_date;
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(first_query, conn);
            command.Parameters.AddWithValue("@p_tweet_id", p_tweet_id);
            try
            {
                conn.Open();
                SqlDataReader reader = command.ExecuteReader();
                reader.Read();

                s_rt_created_date = reader[0].ToString();
                DateTime d_rt_created_date = DateTime.Parse(s_rt_created_date);
                //if (d_rt_created_date < p_rt_created_date)
                //{
                    second_query = "UPDATE retweet_raw_counts SET " +
                    "user_name = @p_user_name," +
                    "user_id = @p_user_id," +
                    "profile_image_url = @p_profile_image_url," +
                    "utc_offset = @p_utc_offset," +
                    "retweet_count = @p_retweet_count," +
                    "place_name = @p_place_name," +
                    "country_code = @p_country_code," +
                    "lattitude = @p_lattitude, " +
                    "longitude = @p_longitude, " +
                    "updated_date = @right_now, " +
                    "followers = @p_followers, " +
                    //"extended_followers = @p_ext_followers, " +
                    "retweet_created_date = @p_rt_created_date, " +
                    "counted = 0, " +
                    "hashtag = @p_hashtag " +
                    "WHERE tweet_id = @p_tweet_id" +
                    ";";
                    return_code = 0;
                //}
                //else
                //{
                //    conn.Close();
                //    return 1;
                //}
            }
            catch (Exception ex)
            {
                second_query = "INSERT INTO retweet_raw_counts (" +
                     "tweet_id," +
                     "user_name," +
                     "user_id," +
                     "profile_image_url," +
                     "utc_offset," +
                     "retweet_count," +
                     "place_name," +
                     "country_code, " +
                     "lattitude, " +
                     "longitude," +
                     "followers," +
                     "counted, " +
                     "created_date, " +
                     "retweet_created_date, " +
                     "hashtag) " +
                     "values (" +
                     "@p_tweet_id," +
                     "@p_user_name," +
                     "@p_user_id," +
                     "@p_profile_image_url," +
                     "@p_utc_offset," +
                     "@p_retweet_count," +
                     "@p_place_name," +
                     "@p_country_code, " +
                     "@p_lattitude, " +
                     "@p_lattitude," +
                     "@p_followers," +
                     "0," +
                     "@right_now," +
                     "@p_rt_created_date," +
                     "@p_hashtag" +
                     ");";
                return_code = 1;
            }

            conn.Close();
            command = new SqlCommand(second_query, conn);
            command.Parameters.AddWithValue("@right_now", DateTime.Now);
            command.Parameters.AddWithValue("@p_tweet_id", p_tweet_id);
            //command.Parameters.AddWithValue("@p_tweet_text", p_tweet_text);
            command.Parameters.AddWithValue("@p_user_name", p_user_name);
            command.Parameters.AddWithValue("@p_user_id", p_user_id);
            command.Parameters.AddWithValue("@p_profile_image_url", p_profile_image_url);
            //command.Parameters.AddWithValue("@p_location", p_location);
            command.Parameters.AddWithValue("@p_utc_offset", p_utc_offset);
            //command.Parameters.AddWithValue("@p_retweeted_by" ,p_retweeted_by);
            //command.Parameters.AddWithValue("@p_retweeted_location" ,p_retweeted_location);
            //command.Parameters.AddWithValue("@p_retweeted_utc_offset" ,p_retweeted_utc_offset);
            command.Parameters.AddWithValue("@p_retweet_count" ,p_retweet_count);
            command.Parameters.AddWithValue("@p_place_name", p_place_name);
            command.Parameters.AddWithValue("@p_country_code", p_country_code);
            command.Parameters.AddWithValue("@p_lattitude", p_lattitude);
            command.Parameters.AddWithValue("@p_longitude", p_longitude);
            command.Parameters.AddWithValue("@p_followers", p_followers);
            //command.Parameters.AddWithValue("@p_ext_followers", p_ext_followers);
            command.Parameters.AddWithValue("@p_rt_created_date", p_rt_created_date);
            command.Parameters.AddWithValue("@p_hashtag", p_hashtag);
            conn.Open();
            row_count = command.ExecuteNonQuery();
            conn.Close();

            return return_code;
        }

        public void twitter_aggregate()
        {
            string first_query = "EXEC Aggregate_Tweet_Counts;";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(first_query, conn);
            try
            {
                conn.Open();
                command.ExecuteNonQuery();
                conn.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Stored procedure execution failed.");
                conn.Close();
            }
        }

        public bool isPreviousRetweet(string p_tweet_id, int p_retweet_count)
        {
            int rt_count = 0;
            //check if this field has been inserted already
            string first_query = "SELECT retweet_count FROM retweet_raw_counts WHERE tweet_id = @p_tweet_id;";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(first_query, conn);
            command.Parameters.AddWithValue("@p_tweet_id", p_tweet_id);

            try
            {
                conn.Open();
                SqlDataReader reader = command.ExecuteReader();
                reader.Read();
                string s_rec_count = reader[0].ToString();
                rt_count = int.Parse(s_rec_count);
                if (rt_count == p_retweet_count)
                    return true;
                else
                    return false;
            }
            catch
            {
                return false;
            }

        }

        /*
         * Facebook
         */

        //query the commited insight fields get the date of the last update
        public DateTime facebook_last_update(string p_field_name, string p_period, string total_status)
        {
            if (total_status == "run" || total_status == "RUNNING")
                p_period = "days_28";
            
            string date_query = "SELECT max(as_of_date) FROM " + facebook_insight_table +
                " WHERE field_name = @p_field_name AND period = @p_period;";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(date_query, conn);
            command.Parameters.AddWithValue("@p_field_name", p_field_name);
            command.Parameters.AddWithValue("@p_period", p_period);
            //try
            //{
                conn.Open();
                //check if data to insert is new
                SqlDataReader reader = command.ExecuteReader();
                reader.Read();
                string s_as_of_date = reader[0].ToString();
                conn.Close();
                DateTime dt_as_of_date;
                if (s_as_of_date != "")
                    dt_as_of_date = DateTime.Parse(DateTime.Parse(s_as_of_date).ToShortDateString());
                else
                    dt_as_of_date = DateTime.Now.AddMonths(-1);
                
                return dt_as_of_date;
            //}
            //catch
            //{
              //  return DateTime.Today.AddDays(1); 
            //}
        }

        //insert insight field data. Do not allow repeats.
        public int facebook_insight_insert(
            DateTime p_pull_date
            , string p_field_name
            , string p_period
            , string p_field_title
            , int p_field_value
            , string p_total_type
            , string p_category
            , DateTime p_as_of_date
            , DateTime p_last_date)
        {
            string p_total_status = "";
            string insert_query = "";
            int insight_value = 0;

            //changing days_28 running total to a lifetime by adding the days value from FB to the last value;

            if (p_total_type == "run" || p_total_type == "RUNNING")
            {
                p_total_status = p_total_type;
                p_period = "days_28";
            }

            SqlConnection conn = new SqlConnection(ConnectionString);
            //if this is a run or RUNNING, get the previous total to calc the new one
            SqlCommand command;
            if (p_total_type == "RUNNING" || p_total_type == "run")
            {
                string date_query = "SELECT distinct field_title from " + facebook_insight_table +
                    " WHERE field_name = @p_field_name AND period = @p_period and as_of_date = @p_last_date;";
                command = new SqlCommand(date_query, conn);
                command.Parameters.AddWithValue("@p_field_name", p_field_name);
                command.Parameters.AddWithValue("@p_period", p_period);
                command.Parameters.AddWithValue("@p_last_date", p_last_date);

                try
                {
                    conn.Open();
                    //check if data to insert is new
                    SqlDataReader reader = command.ExecuteReader();
                    reader.Read();
                    p_field_title = reader[0].ToString();
                    conn.Close();
                    DateTime dt_as_of_date;
                    dt_as_of_date = DateTime.Parse(p_last_date.ToShortDateString());

                    string select_query = "SELECT field_value from " + facebook_insight_table +
                        " WHERE field_name = @p_field_name AND period = @p_period and" +
                        " category = @p_category and as_of_date = @p_as_of_date;";
                    command = new SqlCommand(select_query, conn);
                    command.Parameters.AddWithValue("@p_field_name", p_field_name);
                    command.Parameters.AddWithValue("@p_period", p_period);
                    command.Parameters.AddWithValue("@p_category", p_category);
                    command.Parameters.AddWithValue("@p_as_of_date", dt_as_of_date);

                    string field_value = "";
                    try
                    {
                        conn.Open();
                        reader = command.ExecuteReader();
                        reader.Read();
                        field_value = reader[0].ToString();
                    }
                    catch
                    {
                        field_value = "0"; //record does not exist
                    }
                    conn.Close();
                    insight_value = p_field_value;
                    p_field_value = int.Parse(field_value) + p_field_value;  //record exists and this is its value

                }
                catch
                {
                    p_field_title += ": Accumulated";  //insert a NEW field type
                }
            }

            //string p_total_status_db = p_total_status == "RUNNING" ? "RUNNING" : "";

            insert_query = "INSERT INTO " + facebook_raw_table + " (" +
                 "pull_date,    field_name,    period,    field_title,    field_value,    category,    as_of_date,    add_to_total,   total_status) values (" +
                 "@p_pull_date, @p_field_name, @p_period, @p_field_title, @p_field_value, @p_category, @p_as_of_date, @insight_value, @p_total_status);";
            
            conn = new SqlConnection(ConnectionString);
            //Prepare for insert
            command = new SqlCommand(insert_query, conn);
            command.Parameters.AddWithValue("@p_field_name", p_field_name);
            command.Parameters.AddWithValue("@p_as_of_date", p_as_of_date);
            command.Parameters.AddWithValue("@p_pull_date", p_pull_date);
            command.Parameters.AddWithValue("@p_period", p_period);
            command.Parameters.AddWithValue("@p_field_title", p_field_title);
            command.Parameters.AddWithValue("@p_field_value", p_field_value);
            command.Parameters.AddWithValue("@p_category", p_category);
            command.Parameters.AddWithValue("@p_total_status", p_total_status);
            command.Parameters.AddWithValue("@insight_value", insight_value);
            //Perform insert
            try
            {
                conn.Open();
                var row_count = command.ExecuteNonQuery();
                conn.Close();
                return row_count;
            }
            catch(Exception e)
            {
                Console.WriteLine("DATABASE ERROR ENCOUNTERED!");
                Console.WriteLine(e.Message);
                return -2;
            }
            
        }


        public void update_insights_commit(string p_field_name, string p_period)
        {
            //string update_query = "EXEC Commit_Facebook_Data @p_field_name, @p_period;";
            string update_query = "EXEC " + FB_Commit_SP + ";";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(update_query, conn);
            //command.Parameters.AddWithValue("@p_field_name", p_field_name);
            //command.Parameters.AddWithValue("@p_period", p_period);
            conn.Open();
            command.ExecuteNonQuery();
            conn.Close();
        }


        /*
         * In the event historical Facebook data needs to be inserted into the Facebook insights table,
         * this function will search for an existing record matching the name, period, total status, 
         * as_of_date and category of the proposed new record. If it does not already exist, the field
         * value of the previous record will be returned for further processing (0 is a valid value). 
         * Otherwise, the not-found status of -1 will be returned.
         * 
         * In the case of a database exception, a -2 status is returned.
         */ 
        public int findFBDBRecord(
            string p_field_name, 
            string p_period, 
            string p_total_status,
            string p_category,
            DateTime p_as_of_date,
            ref string field_title)
        {
            int row_count = 0;
            
            if (p_total_status == "run" || p_total_status == "RUNNING")
            {
                p_period = "days_28";
            }

            // Query the database to see if there are any existing records matching the one we want to insert
            string selectStmt = "SELECT count(*) FROM " + facebook_insight_table +
                " WHERE field_name = @p_field_name AND " +
                " period = @p_period AND " +
                " as_of_date = @p_as_of_date";
            if (p_category != null) //add the category to the query if it is not null
                selectStmt += " AND category = @p_category;";
            else
                selectStmt += ";";
            SqlConnection conn = new SqlConnection(ConnectionString);

            try
            {
                SqlCommand command = new SqlCommand(selectStmt, conn);
                command.Parameters.AddWithValue("@p_field_name", p_field_name);
                command.Parameters.AddWithValue("@p_total_status", p_total_status);
                command.Parameters.AddWithValue("@p_period", p_period);
                command.Parameters.AddWithValue("@p_as_of_date", p_as_of_date);
                if (p_category != null)
                    command.Parameters.AddWithValue("@p_category", p_category);

                conn.Open();
                SqlDataReader reader = command.ExecuteReader();
                reader.Read();
                row_count = int.Parse(reader[0].ToString());
                conn.Close();
            }
            catch (Exception e)
            {
                //Database access exception. Very bad.
                Console.WriteLine("DATABASE ERROR ENCOUNTERED!");
                Console.WriteLine(e.Message);
                return -2;
            }

            if (row_count == 0)
            {

                // The record DOES NOT exist so now we need the maximum value of the previous rows to carry forward
                selectStmt = "SELECT field_title, max(field_value) FROM " + facebook_insight_table +
                    " WHERE field_name = @p_field_name AND " +
                    " period = @p_period AND " +
                    " as_of_date < @p_as_of_date";
                if (p_category != null)
                    selectStmt += " AND category = @p_category GROUP BY field_title;";
                else
                    selectStmt += " GROUP BY field_title;";

                SqlCommand command = new SqlCommand(selectStmt, conn);
                command.Parameters.AddWithValue("@p_field_name", p_field_name);
                command.Parameters.AddWithValue("@p_total_status", p_total_status);
                command.Parameters.AddWithValue("@p_period", p_period);
                command.Parameters.AddWithValue("@p_as_of_date", p_as_of_date);
                if (p_category != null)
                    command.Parameters.AddWithValue("@p_category", p_category);

                try
                {
                    conn.Open();
                    SqlDataReader reader = command.ExecuteReader();
                    reader.Read();
                    field_title = reader[0].ToString();
                    string s_field_value = reader[1].ToString();
                    int field_value = int.Parse(s_field_value);
                    conn.Close();
                    return field_value;
                }
                catch (Exception e)
                {
                    conn.Close();
                    //No records exist. Are there any?
                    selectStmt = "SELECT distinct field_title FROM " + facebook_insight_table +
                                " WHERE field_name = @p_field_name AND " +
                                " period = @p_period AND " +
                                " total_status = @p_total_status;";

                    command = new SqlCommand(selectStmt, conn);
                    command.Parameters.AddWithValue("@p_field_name", p_field_name);
                    command.Parameters.AddWithValue("@p_total_status", p_total_status);
                    command.Parameters.AddWithValue("@p_period", p_period);
                    try
                    {
                        conn.Open();
                        SqlDataReader reader = command.ExecuteReader();
                        reader.Read();
                        field_title = reader[0].ToString(); 
                        //newer records exist. Get the title text.
                        conn.Close();
                        return 0; //return 0 since this will now be the first record in the set
                    }
                    catch
                    {
                        return 0;
                    }
                }
            }
            else
            {
                return -1;
            }
        }

        /*
         * Insert a new historical (not current) Facebook record and carry it's value forward to the subsequent records
         */

        internal int InsertHistInsight(
            DateTime p_pull_date,
            string p_field_name, 
            string p_period, 
            string p_field_title,
            string p_total_status, 
            string p_category, 
            DateTime p_as_of_date, 
            int p_field_value,
            int p_seed_field_value)
        {
            if (p_total_status == "run" || p_total_status == "RUNNING")
            {
                p_period = "days_28";
            }

            string insertStmt = "INSERT INTO " + facebook_insight_table +
                "       (pull_date,    field_name,    period,    field_title,    field_value,   field_type, category,    as_of_date,    add_to_total,        total_status) OUTPUT Inserted.insight_id " +
                "values (@p_pull_date, @p_field_name, @p_period, @p_field_title, @p_field_value, null,      @p_category, @p_as_of_date, @p_seed_field_value, @p_total_status);";
            SqlConnection conn = new SqlConnection(ConnectionString);
            //Prepare for insert
            SqlCommand command = new SqlCommand(insertStmt, conn);
            command.Parameters.AddWithValue("@p_field_name", p_field_name);
            command.Parameters.AddWithValue("@p_as_of_date", p_as_of_date);
            command.Parameters.AddWithValue("@p_pull_date", p_pull_date);
            command.Parameters.AddWithValue("@p_period", p_period);
            command.Parameters.AddWithValue("@p_field_title", p_field_title);
            command.Parameters.AddWithValue("@p_field_value", p_field_value+p_seed_field_value);
            command.Parameters.AddWithValue("@p_seed_field_value", p_seed_field_value);
            command.Parameters.AddWithValue("@p_category", p_category);
            command.Parameters.AddWithValue("@p_total_status", p_total_status);
            //Perform insert
            try
            {
                conn.Open();
                var row_obj = command.ExecuteScalar();
                conn.Close();
                int row_id = int.Parse(row_obj.ToString());
                if (row_id > 0)
                {
                    //CarryForward(row_id, p_seed_field_value, p_total_status);
                    return row_id;
                }
                else
                    return 0;
            }
            catch (Exception e)
            {
                Console.WriteLine("DATABASE ERROR ENCOUNTERED!");
                Console.WriteLine(e.Message);
                return -2;
            }

        }

        private void CarryForward(int p_record_id, int p_seed_value, string p_total_status)
        {
            //string update_query = "EXEC Commit_Facebook_Data @p_field_name, @p_period;";
            string update_query = "EXEC Facebook_Hist_Carry_Forward @p_record_id,@p_seed_value,@p_total_status;";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(update_query, conn);
            command.Parameters.AddWithValue("@p_record_id", p_record_id);
            command.Parameters.AddWithValue("@p_seed_value", p_seed_value);
            command.Parameters.AddWithValue("@p_total_status", p_total_status);
            conn.Open();
            command.ExecuteNonQuery();
            conn.Close();
        }

        /*
        * Insert a new historical (not current) Facebook record 
        */

        internal int InsertHistInsight(
            DateTime p_pull_date,
            string p_field_name,
            string p_period,
            string p_field_title,
            string p_total_status,
            string p_category,
            DateTime p_as_of_date,
            int p_seed_field_value)
        {
            string insertStmt = "INSERT INTO  " + facebook_insight_table +
                "       (pull_date,    field_name,    period,    field_title,    field_value,   field_type, category,    as_of_date,    add_to_total,    total_status) OUTPUT Inserted.insight_id " +
                "values (@p_pull_date, @p_field_name, @p_period, @p_field_title, @p_field_value, null,      @p_category, @p_as_of_date, @p_add_to_total, @p_total_status);";
            SqlConnection conn = new SqlConnection(ConnectionString);
            //Prepare for insert
            SqlCommand command = new SqlCommand(insertStmt, conn);
            command.Parameters.AddWithValue("@p_field_name", p_field_name);
            command.Parameters.AddWithValue("@p_as_of_date", p_as_of_date);
            command.Parameters.AddWithValue("@p_pull_date", p_pull_date);
            command.Parameters.AddWithValue("@p_period", p_period);
            command.Parameters.AddWithValue("@p_field_title", p_field_title);
            command.Parameters.AddWithValue("@p_field_value", p_seed_field_value);
            command.Parameters.AddWithValue("@p_category", p_category);
            command.Parameters.AddWithValue("@p_total_status", p_total_status);
            command.Parameters.AddWithValue("@p_add_to_total", 0);

            //Perform insert
            try
            {
                conn.Open();
                var row_obj = command.ExecuteScalar();
                conn.Close();
                int row_id = int.Parse(row_obj.ToString());
                return row_id;
            }
            catch (Exception e)
            {
                Console.WriteLine("DATABASE ERROR ENCOUNTERED!");
                Console.WriteLine(e.Message);
                return -2;
            }
            
        }


        public int insertYTAnalytics(
            string p_video_id,
            string p_video_name,
            DateTime p_create_date,
            DateTime p_start_date,
            DateTime p_end_date,
            string p_country_id,
            DateTime p_collect_date,
            int p_views,
            int p_likes,
            int p_dislikes,
            int p_comments,
            int p_newSubscribers)
        {
            string insertStmt = "INSERT INTO  " + youtube_analytics_table +
            "        (video_id,    video_name,     create_date,    start_date,    end_date,    country_id,   collect_date,     views,    likes,    dislikes,     comments, newSubscribers) OUTPUT Inserted.yt_ana_id " +
            "values (@p_video_id, @p_video_name,  @p_create_date, @p_start_date, @p_end_date, @p_country_id, @p_collect_date, @p_views, @p_likes, @p_dislikes, @p_comments, @p_newSubscribers);";
            SqlConnection conn = new SqlConnection(ConnectionString);
            //Prepare for insert
            SqlCommand command = new SqlCommand(insertStmt, conn);
            command.Parameters.AddWithValue("@p_video_id", p_video_id);
            command.Parameters.AddWithValue("@p_video_name", p_video_name);
            command.Parameters.AddWithValue("@p_create_date", p_create_date);
            command.Parameters.AddWithValue("@p_start_date", p_start_date);
            command.Parameters.AddWithValue("@p_end_date", p_end_date);
            command.Parameters.AddWithValue("@p_country_id", p_country_id);
            command.Parameters.AddWithValue("@p_collect_date", p_collect_date);
            command.Parameters.AddWithValue("@p_views", p_views);
            command.Parameters.AddWithValue("@p_likes", p_likes);
            command.Parameters.AddWithValue("@p_dislikes", p_dislikes);
            command.Parameters.AddWithValue("@p_comments", p_comments);
            command.Parameters.AddWithValue("@p_newSubscribers", p_newSubscribers);

            //Perform insert
            try
            {
                conn.Open();
                var row_obj = command.ExecuteScalar();
                int row_id = int.Parse(row_obj.ToString());
                conn.Close();
                return row_id;
            }
            catch (Exception e)
            {
                if (e.Message.Contains("UNIQUE KEY"))
                    return 0;
                Console.WriteLine("DATABASE ERROR ENCOUNTERED!");
                Console.WriteLine(e.Message);
                return -2;
            }

        }

        public void CommitAnalytics()
        {
            //string update_query = "EXEC Commit_Facebook_Data @p_field_name, @p_period;";
            string update_query = "EXEC Aggregate_YT_Ana_Counts;";
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(update_query, conn);
            conn.Open();
            command.ExecuteNonQuery();
            conn.Close();
        }


        //got to the database and get the names of the videos;
        public Dictionary<string, string> getVideoNames()
        {
            string selectStmt = "SELECT distinct video_id, video_name from " + youtube_committed;
            Dictionary<string, string> videoDic = new Dictionary<string,string>();

            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand command = new SqlCommand(selectStmt, conn);
            try
            {
                string video_id;
                string video_name;
                conn.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    video_id = reader[0].ToString(); 
                    video_name = reader[1].ToString();
                    videoDic.Add(video_id,video_name);
                }
                conn.Close();
                return videoDic; //return 0 since this will now be the first record in the set
            }
            catch
            {
                return null;
            }

        }

    }
}
