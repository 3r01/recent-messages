import * as React from "react";
import config from "../config";

type Tags = Record<string, string>;

type ChatMessage = {
  command: string;
  name: string;
  color?: string;
  text: string;
  tags: Tags;
  timestamp?: Date;
};

type Emote = {
  name: string;
  url: string;
  provider: string;
};

type Badge = {
  title: string;
  url: string;
};

type Cosmetics = {
  badges: Map<string, Badge>;
  emotes: Map<string, Emote>;
};

const emptyCosmetics = (): Cosmetics => ({
  badges: new Map(),
  emotes: new Map(),
});

function decodeTag(value: string): string {
  const escapes: Record<string, string> = {
    s: " ",
    n: "\n",
    r: "\r",
    ":": ";",
    "\\": "\\",
  };
  return value.replace(/\\([snr:\\])/g, (_, escape: string) => {
    return escapes[escape];
  });
}

function formatDuration(totalSeconds: number): string {
  if (!Number.isFinite(totalSeconds) || totalSeconds < 0) return "";
  let remaining = Math.floor(totalSeconds);
  const parts: string[] = [];
  const units: Array<[number, string]> = [
    [86400, "d"],
    [3600, "h"],
    [60, "m"],
    [1, "s"],
  ];
  for (const [seconds, suffix] of units) {
    const value = Math.floor(remaining / seconds);
    remaining %= seconds;
    if (value > 0) parts.push(value + suffix);
  }
  return parts.join(" ");
}

function parseMessage(raw: string): ChatMessage | null {
  let rest = raw;
  const tags: Tags = {};
  if (rest.startsWith("@")) {
    const end = rest.indexOf(" ");
    if (end < 0) return null;
    for (const tag of rest.slice(1, end).split(";")) {
      const separator = tag.indexOf("=");
      const key = separator < 0 ? tag : tag.slice(0, separator);
      tags[key] = decodeTag(separator < 0 ? "" : tag.slice(separator + 1));
    }
    rest = rest.slice(end + 1);
  }

  let prefix = "";
  if (rest.startsWith(":")) {
    const end = rest.indexOf(" ");
    if (end < 0) return null;
    prefix = rest.slice(1, end);
    rest = rest.slice(end + 1);
  }
  const commandEnd = rest.indexOf(" ");
  const command = commandEnd < 0 ? rest : rest.slice(0, commandEnd);
  rest = commandEnd < 0 ? "" : rest.slice(commandEnd + 1);
  const trailing = rest.indexOf(" :");
  const parameters = (trailing < 0 ? rest : rest.slice(0, trailing))
    .split(" ")
    .filter(Boolean);
  let text = trailing < 0 ? "" : rest.slice(trailing + 2);
  if (
    !text &&
    (command === "PRIVMSG" ||
      command === "USERNOTICE" ||
      command === "CLEARMSG")
  ) {
    text = parameters.slice(1).join(" ");
  }
  const name =
    tags["display-name"] || tags.login || prefix.split("!", 1)[0] || "Twitch";
  const timestampValue = tags["rm-received-ts"] || tags["tmi-sent-ts"];
  const timestamp = timestampValue
    ? new Date(Number(timestampValue))
    : undefined;

  if (command === "USERNOTICE") {
    text = [tags["system-msg"] || "User notice", text]
      .filter(Boolean)
      .join(" ");
  } else if (command === "CLEARCHAT") {
    const target = parameters[1] || text;
    const duration = formatDuration(Number(tags["ban-duration"]));
    if (!target) {
      text = "Chat has been cleared by a moderator.";
    } else if (tags["ban-duration"] && duration) {
      text = target + " has been timed out for " + duration + ".";
    } else {
      text = target + " has been permanently banned.";
    }
  } else if (command === "CLEARMSG") {
    const target = tags.login;
    text = target
      ? "A message from " + target + " was deleted: " + text
      : "A message was deleted: " + text;
  }

  return { command, name, color: tags.color, text, tags, timestamp };
}

function addFfzSets(emotes: Map<string, Emote>, sets: unknown) {
  if (!sets || typeof sets !== "object") return;
  for (const set of Object.values(sets)) {
    const emoticons = (set as { emoticons?: Array<Record<string, unknown>> })
      .emoticons;
    for (const emote of emoticons || []) {
      if (typeof emote.name === "string" && emote.id != null) {
        emotes.set(emote.name, {
          name: emote.name,
          provider: "FFZ",
          url: "https://cdn.frankerfacez.com/emote/" + emote.id + "/2",
        });
      }
    }
  }
}

async function loadCosmetics(channelId: string): Promise<Cosmetics> {
  const cosmetics = emptyCosmetics();
  const sevenTvQuery = `{
    emoteSets { global { emotes { items { id alias } } } }
    users { userByConnection(platform: TWITCH, platformId: "${channelId}") {
      style { activeEmoteSet { emotes { items { id alias } } } }
    } }
  }`;
  const requests = await Promise.allSettled([
    fetch("/api/viewer/badges/" + channelId).then((response) => {
      if (!response.ok) throw new Error("badges");
      return response.json();
    }),
    fetch("https://api.betterttv.net/3/cached/emotes/global").then((response) =>
      response.json(),
    ),
    fetch("https://api.betterttv.net/3/cached/users/twitch/" + channelId).then(
      (response) => {
        if (!response.ok) return {};
        return response.json();
      },
    ),
    fetch("https://api.frankerfacez.com/v1/set/global").then((response) =>
      response.json(),
    ),
    fetch("https://api.frankerfacez.com/v1/room/id/" + channelId).then(
      (response) => {
        if (!response.ok) return {};
        return response.json();
      },
    ),
    fetch("https://7tv.io/v4/gql", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: sevenTvQuery }),
    }).then((response) => response.json()),
  ]);

  const [badges, bttvGlobal, bttvChannel, ffzGlobal, ffzChannel, sevenTv] =
    requests.map((request) =>
      request.status === "fulfilled" ? request.value : {},
    );

  for (const [key, badge] of Object.entries(badges.badges || {})) {
    const value = badge as Badge;
    if (value.url) cosmetics.badges.set(key, value);
  }
  for (const emote of bttvGlobal || []) {
    cosmetics.emotes.set(emote.code, {
      name: emote.code,
      provider: "BTTV",
      url: "https://cdn.betterttv.net/emote/" + emote.id + "/2x.webp",
    });
  }
  for (const emote of [
    ...(bttvChannel.channelEmotes || []),
    ...(bttvChannel.sharedEmotes || []),
  ]) {
    cosmetics.emotes.set(emote.code, {
      name: emote.code,
      provider: "BTTV",
      url: "https://cdn.betterttv.net/emote/" + emote.id + "/2x.webp",
    });
  }
  addFfzSets(cosmetics.emotes, ffzGlobal.sets);
  addFfzSets(cosmetics.emotes, ffzChannel.sets);

  const globalSevenTv = sevenTv.data?.emoteSets?.global?.emotes?.items || [];
  const channelSevenTv =
    sevenTv.data?.users?.userByConnection?.style?.activeEmoteSet?.emotes
      ?.items || [];
  for (const emote of [...globalSevenTv, ...channelSevenTv]) {
    cosmetics.emotes.set(emote.alias, {
      name: emote.alias,
      provider: "7TV",
      url: "https://cdn.7tv.app/emote/" + emote.id + "/2x.webp",
    });
  }
  return cosmetics;
}

function ThirdPartyText({
  text,
  emotes,
}: {
  text: string;
  emotes: Map<string, Emote>;
}) {
  return (
    <>
      {text.split(/(\s+)/).map((part, index) => {
        const emote = emotes.get(part);
        if (emote) {
          return (
            <img
              className="chat-emote"
              src={emote.url}
              alt={emote.name}
              title={emote.name + " (" + emote.provider + ")"}
              loading="lazy"
              key={index}
            />
          );
        }
        if (/^https?:\/\/\S+$/.test(part)) {
          return (
            <a href={part} target="_blank" rel="noreferrer" key={index}>
              {part}
            </a>
          );
        }
        return <React.Fragment key={index}>{part}</React.Fragment>;
      })}
    </>
  );
}

function MessageText({
  message,
  emotes,
}: {
  message: ChatMessage;
  emotes: Map<string, Emote>;
}) {
  let text = message.text;
  let action = false;
  if (text.startsWith("\u0001ACTION ") && text.endsWith("\u0001")) {
    text = text.slice(8, -1);
    action = true;
  }
  const characters = Array.from(text);
  const ranges: Array<{ id: string; start: number; end: number }> = [];
  for (const group of (message.tags.emotes || "").split("/")) {
    const [id, positions] = group.split(":");
    if (!id || !positions) continue;
    for (const position of positions.split(",")) {
      const [start, end] = position.split("-").map(Number);
      if (Number.isInteger(start) && Number.isInteger(end) && end >= start) {
        ranges.push({ id, start, end });
      }
    }
  }
  ranges.sort((left, right) => left.start - right.start);
  const parts: React.ReactNode[] = [];
  let cursor = 0;
  for (const range of ranges) {
    if (range.start < cursor || range.end >= characters.length) continue;
    if (range.start > cursor) {
      parts.push(
        <ThirdPartyText
          text={characters.slice(cursor, range.start).join("")}
          emotes={emotes}
          key={"text-" + cursor}
        />,
      );
    }
    const name = characters.slice(range.start, range.end + 1).join("");
    parts.push(
      <img
        className="chat-emote"
        src={
          "https://static-cdn.jtvnw.net/emoticons/v2/" +
          range.id +
          "/default/dark/2.0"
        }
        alt={name}
        title={name}
        loading="lazy"
        key={"emote-" + range.start}
      />,
    );
    cursor = range.end + 1;
  }
  if (cursor < characters.length) {
    parts.push(
      <ThirdPartyText
        text={characters.slice(cursor).join("")}
        emotes={emotes}
        key={"text-" + cursor}
      />,
    );
  }
  return (
    <span
      style={action && message.color ? { color: message.color } : undefined}
    >
      {parts}
    </span>
  );
}

function ChatLine({
  message,
  cosmetics,
}: {
  message: ChatMessage;
  cosmetics: Cosmetics;
}) {
  const badges = (message.tags.badges || "").split(",").filter(Boolean);
  const isChat = message.command === "PRIVMSG";
  const replyName = message.tags["reply-parent-display-name"];
  const replyText = message.tags["reply-parent-msg-body"];
  return (
    <div className={isChat ? "chat-line" : "chat-line chat-event"}>
      {(replyName || replyText) && (
        <div className="chat-reply">
          Replying to {replyName ? "@" + replyName : "a message"}
          {replyText ? ": " + replyText : ""}
        </div>
      )}
      <time>{message.timestamp?.toLocaleTimeString() || ""}</time>{" "}
      {isChat &&
        badges.map((badgeKey) => {
          const badge = cosmetics.badges.get(badgeKey);
          return badge ? (
            <img
              className="chat-badge"
              src={badge.url}
              alt={badge.title}
              title={badge.title}
              loading="lazy"
              key={badgeKey}
            />
          ) : null;
        })}
      {isChat && (
        <strong style={message.color ? { color: message.color } : undefined}>
          {message.name}:
        </strong>
      )}{" "}
      {isChat ? (
        <MessageText message={message} emotes={cosmetics.emotes} />
      ) : (
        <ThirdPartyText text={message.text} emotes={cosmetics.emotes} />
      )}
    </div>
  );
}

export function Viewer() {
  const search = new URLSearchParams(window.location.search);
  const [channel, setChannel] = React.useState(search.get("channel") || "");
  const [limit, setLimit] = React.useState(
    Math.min(800, Math.max(1, Number(search.get("limit")) || 800)),
  );
  const [messages, setMessages] = React.useState<ChatMessage[]>([]);
  const [cosmetics, setCosmetics] = React.useState<Cosmetics>(emptyCosmetics);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState("");
  const sequence = React.useRef(0);

  async function load(event?: React.FormEvent) {
    event?.preventDefault();
    const login = channel.trim().toLowerCase();
    if (!/^[a-z0-9_]{1,25}$/.test(login)) {
      setError("Enter a valid channel name.");
      return;
    }
    const currentSequence = ++sequence.current;
    setLoading(true);
    setError("");
    try {
      const url =
        config.recent_messages_base_url +
        "/" +
        encodeURIComponent(login) +
        "?limit=" +
        limit;
      const response = await fetch(url);
      if (!response.ok)
        throw new Error("Request failed (" + response.status + ")");
      const data = await response.json();
      const parsed: ChatMessage[] = ((data.messages || []) as string[])
        .map((raw: string) => parseMessage(raw))
        .filter(
          (message: ChatMessage | null): message is ChatMessage =>
            message != null,
        )
        .reverse();
      if (currentSequence !== sequence.current) return;
      setMessages(parsed);
      setCosmetics(emptyCosmetics());
      const channelId = parsed.find((message) => message.tags["room-id"])?.tags[
        "room-id"
      ];
      if (channelId) {
        void loadCosmetics(channelId).then((loaded) => {
          if (currentSequence === sequence.current) setCosmetics(loaded);
        });
      }
      window.history.replaceState(
        null,
        "",
        "/viewer?channel=" + encodeURIComponent(login) + "&limit=" + limit,
      );
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Request failed.");
    } finally {
      if (currentSequence === sequence.current) setLoading(false);
    }
  }

  React.useEffect(() => {
    if (channel) void load();
  }, []);

  return (
    <>
      <p>View recent Twitch chat messages.</p>
      <form className="viewer-form" onSubmit={load}>
        <label>
          Channel
          <input
            value={channel}
            onChange={(event) => setChannel(event.target.value)}
            placeholder="channel"
            autoFocus
          />
        </label>
        <label>
          Messages
          <input
            type="number"
            min="1"
            max="800"
            value={limit}
            onChange={(event) =>
              setLimit(Math.min(800, Math.max(1, Number(event.target.value))))
            }
          />
        </label>
        <div className="viewer-action">
          <button type="submit" disabled={loading}>
            {loading ? "Loading…" : "Load"}
          </button>
        </div>
      </form>
      {error && <p className="text-danger">{error}</p>}
      <div className="chat-viewer" aria-live="polite">
        {messages.map((message, index) => (
          <ChatLine
            message={message}
            cosmetics={cosmetics}
            key={
              (message.tags.id || message.tags["tmi-sent-ts"] || "line") + index
            }
          />
        ))}
      </div>
    </>
  );
}
