#### <H>In memory queue management

This project is springboot web application which will perform in memory queue operations for 
producer and consumer. Messages can be produced using rest api call and consumers will consume once 
they found matching message. 

Below curl request used to call post api which will produce message using producer.
Consumers are running and they will keep on consuming messages.

curl --location --request POST 'localhost:8080/message' \
--header 'Content-Type: application/json' \
--data-raw '[
{
"id": "message id 1",
"sleep": 10,
"name": "abc"
},
{
"id": "message id 2",
"sleep": 20,
"name": "def"
},
{
"id": "message id 3",
"sleep": 10,
"name": "ghi",
"criteria": true,
"criteria1": "val1"
},
{
"id": "message id 4",
"sleep": 5,
"name": "jkl",
"criteria": true,
"criteria1": "val2"
}
]'