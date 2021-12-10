curl -H "Content-type: application/json" -X POST -d '{"packageId":"11","jsScript":"var divideFn = function(a,b) { return a/b} ","functionName":"divideFn","tests": [{"testName":"test1","expectedResult":"2.0", "params":[2,1]},{"testName":"test2","expectedResult":"2.0","params":[4,2]}]}' http://localhost:5000/test
sleep 5;
curl -X GET http://localhost:5000/result?packageId=11