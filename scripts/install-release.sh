#!/bin/sh
set -eu

usage() {
    echo "usage: sudo $0 RELEASE_ID BINARY WEB_DIST CONFIG_TEMPLATE UNIT_FILE [HEALTH_CHECK]" >&2
    exit 2
}

[ "$#" -ge 5 ] && [ "$#" -le 6 ] || usage
[ "$(id -u)" -eq 0 ] || {
    echo "install-release must run as root" >&2
    exit 2
}

release_id=$1
binary=$2
web_dist=$3
config_template=$4
unit_file=$5
health_check=${6:-}
health_attempts=${HEALTH_ATTEMPTS:-12}

case "$release_id" in
    ''|*[!A-Za-z0-9._-]*)
        echo "release id may contain only letters, digits, dot, underscore, and dash" >&2
        exit 2
        ;;
esac

[ -f "$binary" ] || { echo "binary not found: $binary" >&2; exit 2; }
[ -f "$web_dist/index.html" ] || { echo "web build not found: $web_dist" >&2; exit 2; }
[ -f "$config_template" ] || { echo "config template not found: $config_template" >&2; exit 2; }
[ -f "$unit_file" ] || { echo "unit file not found: $unit_file" >&2; exit 2; }
[ -z "$health_check" ] || [ -x "$health_check" ] || {
    echo "health check is not executable: $health_check" >&2
    exit 2
}
case "$health_attempts" in
    ''|*[!0-9]*|0)
        echo "HEALTH_ATTEMPTS must be a positive integer" >&2
        exit 2
        ;;
esac

root=/opt/recent-messages
releases=$root/releases
release=$releases/$release_id
staging=$releases/.$release_id.tmp.$$
current=$root/current
next=$root/.current.next.$$
old_target=

cleanup() {
    rm -rf "$staging" "$next"
}
trap cleanup EXIT HUP INT TERM

if ! id -u recent-messages >/dev/null 2>&1; then
    useradd --system --home-dir /var/lib/recent-messages --shell /usr/sbin/nologin recent-messages
fi

install -d -o root -g root -m 0755 "$root" "$releases"
install -d -o root -g recent-messages -m 0750 /etc/recent-messages
install -d -o recent-messages -g recent-messages -m 0750 /var/lib/recent-messages

[ ! -e "$release" ] || {
    echo "release already exists: $release" >&2
    exit 1
}

install -d -o root -g root -m 0755 "$staging"
install -d -o root -g root -m 0755 "$staging/bin" "$staging/web/dist"
install -o root -g root -m 0755 "$binary" "$staging/bin/recent-messages2"
cp -a "$web_dist/." "$staging/web/dist/"
chown -R root:root "$staging/web"
find "$staging/web" -type d -exec chmod 0755 {} +
find "$staging/web" -type f -exec chmod 0644 {} +
mv "$staging" "$release"

if [ ! -e /etc/recent-messages/config.toml ]; then
    install -o root -g recent-messages -m 0640 "$config_template" /etc/recent-messages/config.toml
fi
install -o root -g root -m 0644 "$unit_file" /etc/systemd/system/recent-messages2.service
systemctl daemon-reload

if [ -L "$current" ]; then
    old_target=$(readlink "$current")
fi
ln -s "releases/$release_id" "$next"
mv -Tf "$next" "$current"
systemctl reset-failed recent-messages2.service || true
restart_ok=true
if ! systemctl restart recent-messages2.service; then
    restart_ok=false
fi

healthy=false
if [ "$restart_ok" = true ] && [ -z "$health_check" ]; then
    healthy=true
elif [ "$restart_ok" = true ]; then
    attempt=0
    while [ "$attempt" -lt "$health_attempts" ]; do
        if "$health_check"; then
            healthy=true
            break
        fi
        attempt=$((attempt + 1))
        sleep 5
    done
fi

if [ "$healthy" = true ]; then
    systemctl enable recent-messages2.service
    echo "installed release $release_id"
    exit 0
fi

echo "release $release_id failed health checks; rolling back" >&2
if [ -n "$old_target" ] && [ -d "$root/$old_target" ]; then
    ln -s "$old_target" "$next"
    mv -Tf "$next" "$current"
    systemctl restart recent-messages2.service
else
    systemctl stop recent-messages2.service
fi
exit 1
