URL=https://kaasops.github.io/etcd-defrag-controller/helm

# Run linter for helm chart
lint:
	helm lint charts/etcd-defrag-controller

# Package chart into zip file
package:
	helm package charts/* -d packages

# Create index file (use only for initial setup)
index:
	helm repo index --url ${URL} .