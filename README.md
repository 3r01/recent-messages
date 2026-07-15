# recent-messages

Source code for [rm.iore.tv](https://rm.iore.tv/), based on
[robotty/recent-messages2](https://github.com/robotty/recent-messages2).

## API

```text
GET /api/{channel}
```

See [rm.iore.tv/api](https://rm.iore.tv/api) for request and response details.

## Build

```sh
cargo build --release
cd web
npm ci
npm run build
```

## License

[GNU Affero General Public License v3](LICENSE)
