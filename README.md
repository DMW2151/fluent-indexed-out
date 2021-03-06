# Indexed JSONL Logs

## Container Notes

```bash
# Build
docker build . --tag dmw2151/fluent-bit-plugins

# Run
docker run -ti \
    -p 127.0.0.1:24224:24224 \
    -v $(pwd)/cmd/examples/tmp/:/tmp \
    dmw2151/fluent-bit-plugins:latest \
    /fluent-bit/bin/fluent-bit -e /plugins/idx/plugin.so -i cpu -o go-indexed-file
```

```bash
# Publish
docker buildx create

# Cross Platform Build...
docker buildx build --platform \
    linux/amd64,linux/arm/v7,linux/arm64 \
    -t dmw2151/fluent-bit-plugins:latest \
    --push .
```

```bash
# No Linux/arm/v6 fluent-bit:1.8.12 image, rather than building from source or downgrading
# build go on the RPI0 host...
env CGO_ENABLED=1 go build -buildmode=c-shared \
    -o /home/pi/plugins/idx/plugin.so \
    ./main.go

# And then run with Fluent on the Host...
/opt/td-agent-bit/bin/td-agent-bit \
    -e /home/pi/plugins/idx/plugin.so \
    -i mem \
    -o go-indexed-file
```

## Fluent Plugin Structure

See original documentation from the Go Fluent plugin [repo](https://github.com/fluent/fluent-bit-go) for detailed explaination of the plugin callbacks.

| Plugin Phase        | Callback                   |
|---------------------|----------------------------|
| Registration        | FLBPluginRegister()        |
| Initialization      | FLBPluginInit()            |
| Runtime Flush       | FLBPluginFlush()           |
| Exit                | FLBPluginExit()            |

## Testing Notes

- Performance on full tree
- Performance on half tree
- Performance on 1-node tree
- Performance on 1024 tree
- Performance on 8192 tree
- MultiFlush...
- Add more nodes than file stated capacity??