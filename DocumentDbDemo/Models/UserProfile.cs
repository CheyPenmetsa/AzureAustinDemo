using Newtonsoft.Json;

namespace DocumentDbDemo.Models
{
    public class UserProfile
    {

        public UserProfile(
            string userId,
            string userName)
        {
            this.UserId = userId;
            this.UserName = userName;
        }

        [JsonProperty(PropertyName = "id")]
        public string UserId { get; set; }

        public string UserName { get; set; }

    }
}
