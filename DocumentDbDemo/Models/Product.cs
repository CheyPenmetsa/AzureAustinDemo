using Newtonsoft.Json;

namespace DocumentDbDemo.Models
{
    public class Product
    {
        [JsonProperty(PropertyName = "prod_id")]
        public int  ProductId { get; set; }

        [JsonProperty(PropertyName = "prod_desc")]
        public string Description { get; set; }

        [JsonProperty(PropertyName = "prod_prc")]
        public decimal Price { get; set; }

        [JsonProperty(PropertyName = "prod_qtn")]
        public int Quantity { get; set; }
    }
}
