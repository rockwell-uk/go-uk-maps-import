### docker
```
docker build -t go-uk-maps-import-ci .
docker run -v $(pwd)/..:/go-uk-maps-import go-uk-maps-import-ci
```