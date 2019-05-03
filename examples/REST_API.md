The following example demonstrates how to launch a workflow via REST API. To find out more about available API calls please refer to "[Check Digdag REST API](https://github.com/treasure-data/digdag)".

1. Launch a server with
```
$ digdag server --memory
```
2. Create and push the following workflow to the server (observe the value of id)
```
$ mkdir resttest
$ cd resttest
$ cat << 'EODAG' > resttest.dig
timezone: Europe/Zurich

+echoparam:
  echo>: ${msg}
EODAG

$ digdag push resttest
```
3. Call the workflow via REST API with curl to pass a message (workflowId has to be the value from the previous command)
```
$ curl -X PUT "http://localhost:65432/api/attempts" \
       -H  "accept: application/json" \
       -H  "Content-Type: application/json" \
       -d "{ \"params\": { \"msg\": \"Hello from REST API.\" }, \"sessionTime\": \"2019-05-01T13:38:52+09:00\", \"workflowId\": \"1\"}"
```
