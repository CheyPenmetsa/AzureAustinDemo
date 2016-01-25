using Newtonsoft.Json;

namespace DocumentDbDemo.Models
{
    public class Order
    {
        [JsonProperty(PropertyName = "ord_num")]
        public string OrderNumber { get; set; }

        [JsonProperty(PropertyName = "ship_adrs")]
        public Address ShippingAddress { get; set; }

        [JsonProperty(PropertyName = "ord_prds")]
        public Product[] Products { get; set; }

        [JsonProperty(PropertyName = "ord_amt")]
        public decimal OrderAmount { get; set; }
    }
}
