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
using Temboo.Library.Utilities.Authentication.OAuth2;

namespace KIP_Social_Pull
{
    static public class YTRefresh
    {
        static public String RefreshAccessToken(TembooSession session, String client_id, String client_secret, String refreshToken )
        {
            Console.WriteLine("Refresh YouTube access token");
            RefreshToken refreshTokenChoreo = new RefreshToken(session);

            // Set inputs
            refreshTokenChoreo.setClientSecret(client_secret);
            refreshTokenChoreo.setRefreshToken(refreshToken);
            refreshTokenChoreo.setAccessTokenEndpoint("https://accounts.google.com/o/oauth2/token");
            refreshTokenChoreo.setClientID(client_id);

            // Execute Choreo
            RefreshTokenResultSet refreshTokenResults = refreshTokenChoreo.execute();

            // Print results
            Console.WriteLine(refreshTokenResults.Response);
            string s_refresh = refreshTokenResults.Response;
            JObject json_refresh = JObject.Parse(s_refresh);

            string accessToken = (string)json_refresh["access_token"];
            string expires = (string)json_refresh["expires_in"];
            Console.WriteLine("New AccessToken = " + accessToken + " which expires in " + expires + " minutes.");
            return accessToken;
        }
    }
}
