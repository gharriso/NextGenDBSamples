set -vx
#Retrieve document by ID 
curl http://oraclehost:8080/ords/opsg/soda/latest/ofilms/3


#Post a query 
cat>query1.json  <<EOF
{"Title": "AFFAIR PREJUDICE"}
EOF


curl -X POST --data-binary @query1.json -H "Content-Type: application/json" http://oraclehost:8080/ords/opsg/soda/latest/ofilms?action=query

#Post a more complex query 
cat>query2.json <<EOF
{ "\$and" : [{ "Length" : { "\$gte" : 120}} , { "Rating": "G" } ] }
EOF

cat query2.json

curl -X POST --data-binary @query2.json -H "Content-Type: application/json" http://oraclehost:8080/ords/opsg/soda/latest/ofilms?action=query
