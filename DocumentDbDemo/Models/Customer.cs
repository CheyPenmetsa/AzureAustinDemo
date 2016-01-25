using Newtonsoft.Json;

namespace DocumentDbDemo.Models
{
    public class Customer
    {
        [JsonProperty(PropertyName = "id")]
        public string CustomerId { get; set; }

        [JsonProperty(PropertyName = "fname")]
        public string FirstName { get; set; }

        [JsonProperty(PropertyName = "lname")]
        public string LastName { get; set; }

        [JsonProperty(PropertyName = "orders")]
        public Order[] Orders { get; set; }
    }
}
