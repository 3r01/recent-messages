import * as React from "react";
import { Link } from "react-router-dom";
import config from "../config";

function rot13(value: string): string {
  return value.replace(
    /[A-Z]/gi,
    (character) =>
      "NOPQRSTUVWXYZABCDEFGHIJKLMnopqrstuvwxyzabcdefghijklm"[
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".indexOf(
          character,
        )
      ],
  );
}

const contactEmail = rot13(config.general_contact_email_rot13);

export function Home() {
  return (
    <>
      <section>
        <p>Provides recent Twitch chat messages for a channel.</p>
        <p>
          Based on{" "}
          <a href={config.upstream_repo_url}>robotty/recent-messages2</a>. The{" "}
          <a href={config.repo_url}>source code</a> is available. It is licensed
          under the{" "}
          <a href={config.repo_url + "/blob/main/LICENSE"}>GNU AGPLv3</a>.
        </p>
        <p>
          <a href={config.sponsor_url}>Support this service</a> through GitHub
          Sponsors.
        </p>
        <p>
          Contact me: <a href={"mailto:" + contactEmail}>{contactEmail}</a>
        </p>
      </section>
      <API />
      <Privacy />
    </>
  );
}

export function API() {
  return (
    <>
      <h2 id="api">API</h2>

      <section>
        <h3>Request</h3>
        <pre>
          <code>
            GET {config.recent_messages_base_url}/&#123;channel&#125;
            {"\n"}GET {config.api_base_url}/recent-messages/&#123;channel&#125;
          </code>
        </pre>
        <p>The two paths are aliases for the same endpoint.</p>
        <p>
          For clients that add <code>/recent-messages/&#123;channel&#125;</code>
          , the base URL is <code>{config.api_base_url}</code>.
        </p>
        <p>CORS is enabled for any origin.</p>
      </section>

      <section>
        <h3>Query parameters</h3>
        <p>
          <code>limit</code>: newest 0 to {config.max_buffer_size} messages
          <br />
          <code>after</code>, <code>before</code>: exclusive{" "}
          <code>rm-received-ts</code> bounds in Unix milliseconds
          <br />
          <code>hide_moderation_messages</code>: omit <code>CLEARCHAT</code> and{" "}
          <code>CLEARMSG</code>
          <br />
          <code>hide_moderated_messages</code>: omit deleted messages
          <br />
          <code>clearchat_to_notice</code>: convert <code>CLEARCHAT</code> to{" "}
          <code>NOTICE</code>
        </p>
        <p>
          Use <code>&amp;</code> to include more than one parameter.
        </p>
        <pre>
          <code>
            {`GET ${config.recent_messages_base_url}/{channel}?limit=50
GET ${config.recent_messages_base_url}/{channel}?limit=100&after=1783980000000
GET ${config.recent_messages_base_url}/{channel}?limit=100&hide_moderation_messages=true`}
          </code>
        </pre>
      </section>

      <section>
        <h3>Response</h3>
        <pre>
          <code>
            {
              '{\n  "messages": [\n    "@badge-info=subscriber/48;badges=subscriber/48,la-velada-iv/1;client-nonce=61341dae5cd349049144fb8498a5c198;color=#C7FF00;display-name=Synofle;emote-only=1;emotes=emotesv2_a05bc1bd12cb42c69aba1d6060dc8f8a:0-8;flags;historical=1;id=41c3a920-ecb8-422f-ad24-4592ecfacc65;rm-received-ts=1783984864228;room-id=22484632;subscriber=1;tmi-sent-ts=1783984864065;user-id=81468738;user-type :synofle!synofle@synofle.tmi.twitch.tv PRIVMSG #forsen database0",\n    "@badge-info=subscriber/2;badges=subscriber/0,bloom-badge/3;client-nonce=3ec35d662a464dcd8d76380615f084a6;color=#0000FF;display-name=solowolo422;emotes=emotesv2_b7482780923442c499ae7b4706040695:0-11;flags;historical=1;id=9bfafb4b-c183-4253-9d79-3645294bbe89;rm-received-ts=1783984883318;room-id=22484632;subscriber=1;tmi-sent-ts=1783984883130;user-id=546381996;user-type :solowolo422!solowolo422@solowolo422.tmi.twitch.tv PRIVMSG #forsen :forsenInsane GYAAAAOOOOOO",\n    "@badge-info;badges=no_audio/1;color=#2E8B57;display-name=feliz_navibaj;emotes;flags;historical=1;id=98dd927d-2d27-4a67-8ccc-32a8cc1f808c;rm-received-ts=1783984913115;room-id=22484632;tmi-sent-ts=1783984912921;user-id=50336442;user-type :feliz_navibaj!feliz_navibaj@feliz_navibaj.tmi.twitch.tv PRIVMSG #forsen :@toughpeanut, that\'s your opinion, but nobody can prove which of us is right"\n  ],\n  "error": null,\n  "error_code": null\n}'
            }
          </code>
        </pre>
        <p>
          Raw IRC lines are ordered oldest to newest. Retained commands are{" "}
          <code>PRIVMSG</code>, <code>USERNOTICE</code>, <code>CLEARCHAT</code>,
          and <code>CLEARMSG</code>. Each line has <code>historical=1</code> and{" "}
          <code>rm-received-ts</code>.
        </p>
      </section>

      <section>
        <h3>Errors</h3>
        <p>
          <code>400</code> invalid request, <code>403</code> opted-out channel,
          <code>408</code> timeout, <code>503</code> unavailable
        </p>
      </section>
    </>
  );
}

export function DonationThankYou() {
  return (
    <>
      <h1>Thank you</h1>
      <p>Your support helps cover the cost of running this instance.</p>
      <p>
        <Link to="/">Return home</Link>
      </p>
    </>
  );
}

export function Privacy() {
  const privacyEmail = rot13(config.privacy_contact_email_rot13);
  return (
    <section id="privacy">
      <h2>Privacy</h2>
      <p>
        Public Twitch chat and moderation events are stored for up to{" "}
        {config.messages_expire_after} to provide recent messages.
      </p>
      <p>
        If you log in, Twitch provides your user ID, username, display name, and
        profile-image URL for channel-owner controls. Authorization expires
        after {config.sessions_expire_after}. Logging out revokes it.
      </p>
      <p>
        Channel owners can use <Link to="/settings">channel controls</Link> to
        purge messages or opt out. Privacy questions and requests can be sent to{" "}
        <a href={"mailto:" + privacyEmail}>{privacyEmail}</a>.
      </p>
    </section>
  );
}
