# timeseries-bench
Timeseries database benchmarks

## Prerequisites
- Docker
- Kubernetes (activate inside docker)
- Helm

## Deployment

### InfluxDB

Add repository

```zsh
helm repo add influxdata https://helm.influxdata.com/
helm repo update
```

Create Namespace

```zsh
kubectl create namespace influxdb
```

Basic installation

```zsh
helm upgrade --install influxdb influxdata/influxdb2 \
  --namespace influxdb \
  --set persistence.enabled=true \
  --set persistence.size=10Gi \
  --set adminUser.user=admin \
  --set adminUser.password=admin123 \
  --set adminUser.token=my-super-secret-auth-token \
  --set adminUser.organization=benchmark \
  --set adminUser.bucket=benchmark
```

Port forward

```zsh
kubectl port-forward -n influxdb service/influxdb-influxdb2 8086:80
```

Access InfluxDB UI

```
http://localhost:8086
```

Custom Configuration (Optional)

values.yaml
```yaml
# influxdb-values.yaml
persistence:
  enabled: true
  size: 20Gi

resources:
  requests:
    memory: "2Gi"
    cpu: "1000m"
  limits:
    memory: "4Gi"
    cpu: "2000m"

adminUser:
  user: "admin"
  password: "admin123"
  token: "my-super-secret-auth-token"
  organization: "benchmark"
  bucket: "benchmark"
```

Install custom config

```zsh
helm upgrade --install influxdb influxdata/influxdb2 \
  -n influxdb \
  -f influxdb-values.yaml
```

