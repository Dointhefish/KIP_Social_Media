using System;
using System.IO;
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

namespace KIP_Social_Pull
{
    class Program
    {
        
        static void Main(string[] args)
        {
            //int cycle_count = 0;
            //GooglePlus gp = new GooglePlus();
            Facebook fb = new Facebook();
            Twitter tw = new Twitter();
            YouTubeData ytd = new YouTubeData();
            youTubeAnalytics yta = new youTubeAnalytics(ytd);

            //yta.backFillYTAna(DateTime.Parse("2015-09-19"), DateTime.Parse("2015-09-20"));

            /*
             * Json parsing utility
             */
            //fb.getJsonItems("C:\\temp\\fb_json_fields.txt");

            /*
             * Twitter user tweet retrieval
             */
            //tweetRetrieval(tw);

            /*
             * Twitter history retrieval
             */
            //tw.getTwitterHashtagData("KeepItPumping", DateTime.Parse("2015-10-14"));
            //tw.getTwitterHashtagData("KeepItPumping", DateTime.Parse("2015-10-15"));
            //tw.getTwitterHashtagData("KeepItPumping", DateTime.Parse("2015-10-16"));

            /*
             * Load historical Facebook data from files
             */
            
            //facebookHistorical(fb);

            /*
             * get profile data from Google+ based on google_id
             */
            //gp.getProfile("101892303162187893133");
            
            /*
             * social media processing for all sites
             * runs on 15 minute cycles
             */
            //Console.WriteLine("YouTube Analytics:");
            //DateTime startTime = DateTime.Parse(DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd"));
            //DateTime endTime = DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd"));
            //yta.backFillYTAna(startTime, endTime);
            
            //while (true)
            //{
            
                Console.WriteLine(DateTime.Now.ToString());
                
                Console.WriteLine("YouTube:");
                ytd.getYouTubeData();

                Console.WriteLine("YouTube Analytics:");
                DateTime startTime = DateTime.Parse(DateTime.Now.AddDays(-4).ToString("yyyy-MM-dd"));
                DateTime endTime = DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd"));
                yta.backFillYTAna(startTime, endTime);
                
                Console.WriteLine("Facebook:");
                fb.getFacebookData();
              
                Console.WriteLine("Twitter Hashtag: KeepItPumping");
                tw.getTwitterHashtagData("KeepItPumping");
                Console.WriteLine("Twitter User: @KeepItPumping");
                tw.getTwitterUserData("@KeepItPumping");
                Console.WriteLine("Done");
              
                //Thread.Sleep(900000);
            //}
            
            
        }

        public static void facebookHistorical(Facebook fb)
        { 
         
            fb.getFBHistorical("2015-07-31", "2015-08-05");
            fb.getFBHistorical("2015-08-05", "2015-08-10");
            fb.getFBHistorical("2015-08-10", "2015-08-15");
            fb.getFBHistorical("2015-08-15", "2015-08-20");
            fb.getFBHistorical("2015-08-20", "2015-08-25");
            fb.getFBHistorical("2015-08-25", "2015-08-30");
            fb.getFBHistorical("2015-08-30", "2015-09-01");
            fb.getFBHistorical("2015-09-01", "2015-09-05");
            fb.getFBHistorical("2015-09-05", "2015-09-10");
            fb.getFBHistorical("2015-09-10", "2015-09-15");
            fb.getFBHistorical("2015-09-15", "2015-09-20");
            fb.getFBHistorical("2015-09-20", "2015-09-25");
            fb.getFBHistorical("2015-09-25", "2015-09-30");
            fb.getFBHistorical("2015-09-30", "2015-10-01");
            fb.getFBHistorical("2015-10-01", "2015-10-05");
            fb.getFBHistorical("2015-10-05", "2015-10-10");
            fb.getFBHistorical("2015-10-10", "2015-10-15");
            fb.getFBHistorical("2015-10-15", "2015-10-19");
            fb.getFBHistorical("2015-10-19", "2015-10-22");
            fb.getFBHistorical("2015-10-22", "2015-10-24");
            fb.getFBHistorical("2015-10-24", "2015-10-26");
            fb.getFBHistorical("2015-10-26", "2015-10-28");
            fb.getFBHistorical("2015-10-28", "2015-10-30");



            /*
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\July_31_Aug_1_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_1-2_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_2-3_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_3-4_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_4-5_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_5-6_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_6-7_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_7-8_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_8-9_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_10-11_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_11-12_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_12-13_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_13-14_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_14-15_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_15-16_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_16-17_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_17-18_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_18-19_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_19-20_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_20-21_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_21-22_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_22-23_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_23-24_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Aug_24-25_insight.json");

            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Aug_25-26_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Aug_26-27_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Aug_28-29_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Aug_30-31_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Aug_31_Sept_1_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept2-3insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept_3-4_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept_4-5_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept_5-6_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept_6-7_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept_7-8_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept_8-9_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept_9-10_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept_10-11_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept_11-12_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept_12-13_Insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v1\Sept_13-14_Insight.json");

            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Sept_15-16_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Sept_17-18_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Sept_19-20_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Sept_21-22_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Sept_23-24_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Sept_23-24_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Sept_25-26_insight.json");
            
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Sept_27-30_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Oct_1-4_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Oct_5-9_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Oct_9-12_insight.json");
            fb.getFBHistorical(@"C:\Users\abowers\Downloads\KIP Awareness v2\KIP Awareness\Sept_15-26\Oct_12-15_insight.json");
            */

        }

        public static void tweetRetrieval(Twitter tw)
        {
            tw.findOnTimeLine("TedRubin", "#KeepItPumping");
            tw.findOnTimeLine("Parentng", "#KeepItPumping");
            tw.findOnTimeLine("R_onR", "#KeepItPumping");
            tw.findOnTimeLine("Tammileetips", "#KeepItPumping");
            tw.findOnTimeLine("Just_B_Nice", "#KeepItPumping");
            tw.findOnTimeLine("5minutesformom", "#KeepItPumping");
            tw.findOnTimeLine("WhitneyMWS", "#KeepItPumping");
            tw.findOnTimeLine("VeraSweeney", "#KeepItPumping");
            tw.findOnTimeLine("RonRwoots", "#KeepItPumping");
            tw.findOnTimeLine("GoAptaris", "#KeepItPumping");
            tw.findOnTimeLine("Brand_Connector", "#KeepItPumping");
        }
    }
}