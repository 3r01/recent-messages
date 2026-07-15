#!/bin/sh
set -eu

[ "$(id -u)" -eq 0 ] || {
    echo "install-public-edge must run as root" >&2
    exit 2
}

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
nginx_source=$repo_root/deploy/nginx/rm.iore.tv.conf
tunnel_unit_source=$repo_root/deploy/systemd/cloudflared-rm.service

[ -x /usr/sbin/nginx ] || {
    echo "nginx is not installed" >&2
    exit 2
}
[ -x /usr/bin/cloudflared ] || {
    echo "cloudflared is not installed" >&2
    exit 2
}

id -u cloudflared >/dev/null 2>&1 ||
    useradd --system --home-dir /var/lib/cloudflared --shell /usr/sbin/nologin cloudflared
install -d -o root -g cloudflared -m 0750 /etc/cloudflared

install -o root -g root -m 0644 "$nginx_source" /etc/nginx/sites-available/rm.iore.tv
ln -sfn /etc/nginx/sites-available/rm.iore.tv /etc/nginx/sites-enabled/rm.iore.tv
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl unmask nginx.service
systemctl enable nginx.service
systemctl reload-or-restart nginx.service

install -o root -g root -m 0644 "$tunnel_unit_source" /etc/systemd/system/cloudflared-rm.service
systemctl daemon-reload

if [ -f /etc/cloudflared/rm.env ]; then
    chown root:cloudflared /etc/cloudflared/rm.env
    chmod 0640 /etc/cloudflared/rm.env
    systemctl enable --now cloudflared-rm.service
else
    echo "proxy installed; tunnel remains disabled until /etc/cloudflared/rm.env exists"
fi
