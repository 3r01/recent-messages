let cfg = {
  client_id: "e7dqb3z6wicuij1m1ubdrup831mhl6",
  redirect_uri: "https://rm.iore.tv/authorized",
  // human readable strings for the home page and API documentation
  messages_expire_after: "24 hours",
  channels_expire_after: "24 hours",
  sessions_expire_after: "7 days",
  max_buffer_size: "800",
  // used for both the documentation as well as the actual API calls made by the web app. Don't include a trailing slash
  api_base_url: "https://rm.iore.tv/api/v2",
  recent_messages_base_url: "https://rm.iore.tv/api",

  general_contact_email_rot13: "vber@vber.gi",
  // don't include a trailing slash
  repo_url: "https://github.com/3R01/recent-messages",
  upstream_repo_url: "https://github.com/robotty/recent-messages2",
  sponsor_url: "https://github.com/sponsors/3R01",

  // Remember to update privacy_last_updated_on when updating these!
  privacy_how_do_i_store_your_data:
    "The collected data described above is stored on an access-controlled OVHcloud " +
    "virtual private server. Runtime databases and credentials are not publicly " +
    "accessible, and public traffic reaches the service through Cloudflare Tunnel.",
  privacy_contact_email_rot13: "vber@vber.gi",
  privacy_last_updated_on: "13 July 2026",
};

export type Config = typeof cfg;

if (process.env.NODE_ENV === "development") {
  cfg = {
    ...cfg,
    client_id: "199iyze11rzmuu05ddxqzix4g8soou",
    redirect_uri: "http://localhost:2790/authorized",
    api_base_url: "http://localhost:2790/api/v2",
  };
}

export default cfg;
