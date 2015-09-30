using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Temboo.Core;
using Temboo.Library.Google.Plus.People;

namespace KIP_Social_Pull
{
    public class GooglePlus
    {
        string scope = "https://www.googleapis.com/auth/plus.login";
        GoogleOAuth goa;

        public GooglePlus()
        {
            goa = new GoogleOAuth(scope);
        }

        public string getProfile(string googlePlusID)
        {
            Get getChoreo = new Get(goa.session);
            string accessToken = goa.accessToken;
            return "";
        }

    }
}
