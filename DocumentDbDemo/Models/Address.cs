using Newtonsoft.Json;

namespace DocumentDbDemo.Models
{
    public class Address
    {
        [JsonProperty(PropertyName = "line1")]
        public string AddressLine1 { get; set; }

        [JsonProperty(PropertyName = "line2")]
        public string AddressLine2 { get; set; }

        [JsonProperty(PropertyName = "city")]
        public string City { get; set; }

        [JsonProperty(PropertyName = "state")]
        public string State { get; set; }

        [JsonProperty(PropertyName = "zipcode")]
        public string Zipcode { get; set; }
    }
}
