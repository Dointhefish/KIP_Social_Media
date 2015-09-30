using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Temboo.Core;
using Temboo.Library.Google.OAuth;
using System.Diagnostics;

namespace KIP_Social_Pull
{

    public class GoogleOAuth
    {
        String client_id = "560333471353-4jemti20mjgumkh51mnba3imn15v2j1s.apps.googleusercontent.com";
        String client_secret = "T2mSutBcodtu96FkjY4xmFw4";
        String callBackId = "";
        public String accessToken = "";
        public String refreshToken = "";
        public TembooSession session;
        AwarenessMapDB amDB;

        public GoogleOAuth(string scope)
        {
            session = new TembooSession("keepitpumping", "KeepItPumping-Awareness-Map", "692238482e2b4bc7b62d09234193c202");
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

            amDB = new AwarenessMapDB();

        }

    }
}
