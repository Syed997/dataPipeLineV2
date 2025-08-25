# Run INFO:

Download OpenTelemetry Contrib Collector
```
wget https://github.com/open-telemetry/opentelemetry-collector-releases/releases/latest/download/otelcol-contrib_<version>_linux_amd64.tar.gz
tar -xvf otelcol-contrib_<version>_linux_amd64.tar.gz
chmod +x otelcol-contrib
```

<version>: replace with latest version.

# 2. Update the ote-collector-config as needed and run:
```
./otelcol-contrib --config otel-collector-config.yaml
```

-----
# Check 

oteloutput
