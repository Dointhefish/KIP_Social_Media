using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KIP_Social_Pull
{
    public class Facebook_field
    {
        private string field;
        private string fetch;
        private string reporting;
        private int running;

        public Facebook_field(string field_name,
            string fetch_period,
            string reporting_period,
            int running_total)
        {
            field = field_name;
            fetch = fetch_period;
            reporting = reporting_period;
            running = running_total;
        }

        public string field_name
        {
            get { return field; }
        }
        public string fetch_period
        {
            get { return fetch; }
        }
        public string reporting_period
        {
            get { return reporting; }
        }
        public int running_total
        {
            get { return running; }
        }

        
    }

}
