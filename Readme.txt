task.go is the main file containing all the routes
The server will run at port 8080
url for postman:  http://localhost:8080/

There are 5 routes
1) /set
2) /get
3) /qpush
4) /qpop
5) /bqpop


1)
The route /set will be getting a command of format
{
    "command":"SET b 4 EX 60 NX/XX/ (optional)"
}
If EX is given then expiry will be set in seconds else for indefinite period
IF NX given then it will check if the key is not present or has expired then set the value for that key else returns "key exists"
If XX given then it will check if key is present and has not expired

------------------------------------------------------------------------------------------------------------------

2)
The route /get will be getting a command of format
{
    "command":"GET b"
}
This command will check if they key exists and has not expired

------------------------------------------------------------------------------------------------------------------

3)
The route /qpush will be getting a command of format
{
    "command":"QPUSH list_a 1 2 3"
}

If the queue already exists with this name then it appends the value to that queue else create a new queue and then appends the value
------------------------------------------------------------------------------------------------------------------

4)
The route /qpop will be getting a command of format
{
    "command":"QPOP list_a"
}
It checks if the queue with provided key exists:
    if it exists and is not empty then returns the last appended value
    else return that queue is empty
if a queue is not present with this key then queue is not present is returned
------------------------------------------------------------------------------------------------------------------

5)
The route /bqpop will be getting a command of format
{
    "command":"BQPOP list_a timeout"
}

it checks if the queue exisits:
if it does not exisits then first queue is created
else checked in the exisiting queue

The code is made synchronised using the goroutines to hold the thread until bqpop completes its functionality
If the queue is not empty then it returns the last appended value
else wait until timeout seconds, if some user adds the value to it then the last appended value is returned
after timeout if it hasn't found any value then null will be returned