var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var es = require('elasticsearch');
var AWS = require('aws-sdk');
var KeyGenerator = "0123456789asdfghjklpqw";

//var dynamo = new AWS.DynamoDB.DocumentClient();
AWS.config.update({region: 'us-east-1'});
var dynamodb = new AWS.DynamoDB.DocumentClient();
http.listen(8081, function(){
  console.log('listening on *:8888');
});
var database = new es.Client({
    host: 'search-twittmap-6kcrspxukie6wotjhqthjgon4u.us-east-1.es.amazonaws.com:80'
});

app.get('/', function(req, res){
  res.sendfile('app.html');
});
app.get('/register', function(req, res){
  res.sendfile('register.html');
});
app.get('/GoNow/*', function(req, res) {
	res.sendfile('GoNow.html');
});
app.get('/AvailableDriver/*', function(req, res) {
	res.sendfile('AvailableDriver.html');
});
app.get('/AvailablePassenger/*', function (req, res){
	res.sendfile('AvailablePassenger.html');
});
app.get('/confirmedTrips/*', function(req, res) {
	res.sendfile('confirmedTrips.html');
});
app.get('/myPost/*', function(req, res) {
	res.sendfile('pendTrips.html');
});
app.get('/waitingTrips/*', function(req, res) {
	res.sendfile('waitingTrips.html');
});

function generateKey() {
	var id ="";
		for (var i = 0; i < 5; i++) {
		id += KeyGenerator.charAt(Math.floor(Math.random() * 20));
	}
	return id;
};
function isEmpty(obj) {
    for(var prop in obj) {
        if(obj.hasOwnProperty(prop)){
            return false;
        }
    }
    return true;
};
io.on('connection', function(socket){
	socket.emit('First message', {message: 'Connected!', id: socket.id});
	var result = [];
	socket.on('ConfirmedTrip', function(key){
		var driver = {
			TableName : "Request",
			FilterExpression : "Driver_email = :queryKey",
			ExpressionAttributeValues : {
				":queryKey" : key}			
		};
		dynamodb.scan(driver, function(err, data){
			if (err) {
				console.log(err);
			} else {
				result.push(data);
				socket.emit('responseConfirm', result);
			}
		});			
	});

	socket.on('pendTrips', function(key) {
		var status = "pending";
		var driver = {
			TableName : "Request",
			FilterExpression : "Driver_email = :queryKey",
			ExpressionAttributeValues : {
				":queryKey" : key}			
		};
		dynamodb.scan(driver, function(err, data){
			if (err) {
				console.log(err);
			} else {
				result.push(data);
				socket.emit('responsePend', result);
			}
		});		
	});

	socket.on('passList', function(key){
		var list = [];
		var passenger = {
			TableName : "RequestPassenger",
			FilterExpression : "Driver_email = :driverID",
			ExpressionAttributeValues : {
				":driverID" : key}	
		};
		console.log(passenger);
		dynamodb.scan(passenger, function(err, data){
			if (err) {
				console.log(err);
			} else {
				//list.push(data)
				socket.emit('passConfirm', data);
				console.log(data);
			}
		});	
	});
	socket.on('getPassengerList', function(requestID, callback){
		var status = "confirmed";
		var query = {
			TableName : "Pending",
			FilterExpression : "RequestId = :requestID and CurrStatus = :status",
			ExpressionAttributeValues : {
				":requestID" : requestID,
				":status" : status
			}			
		};
		dynamodb.scan(query, function(err, data) {
			callback(err, data);
		});
	});
	/////////////Yiting

	socket.on('getPendingInfo', function(requestID, callback){
		//console.log(requestID)
		var status = "pending";
		var query = {
			TableName: 'Pending',
			FilterExpression: "RequestId = :requestID and CurrStatus = :status",
			ExpressionAttributeValues : {
				":requestID" : requestID,
				":status" : status
			}	
		};
		console.log(query);
		dynamodb.scan(query,function(err,data) {

			console.log(data);
			callback(err,data);
		});
	})
	socket.on('getList', function(key, callback){
		var status = "pending";
		var query = {
			TableName: 'PendPassenger',
			FilterExpression: "PassengerID = :key and CurrStatus = :status",
			ExpressionAttributeValues : {
				":key" : key,
				":status" : status
			}	
		};
		dynamodb.scan(query,function(err,data) {
			console.log(data);
			callback(err,data);
		});

	});

	socket.on('getPendingPassInfo',function(requestID,callback){
		console.log("here")
		console.log(requestID)
		var status = "pending";
		var query = {
			TableName: 'PendPassenger',
			FilterExpression: "RequestId = :requestID and CurrStatus = :status",
			ExpressionAttributeValues : {
				":requestID" : requestID,
				":status" : status
			}	
		};
		console.log("pending passenger")
		console.log(query);
		dynamodb.scan(query,function(err,data) {

			console.log(data);
			callback(err,data);
		});
	});
	socket.on('getDriverPost', function(DriverID, callback) {
		var status = "confirmed";
		var query = {
			TableName : "PendPassenger",
			FilterExpression : "DriverID = :key and CurrStatus = :status",
			ExpressionAttributeValues : {
				":key" : DriverID,
				":status" : status
			}				
		};
		dynamodb.scan(query, function(err, data) {
			callback(err, data);
		});
	});
	socket.on('pendDriverPost', function(DriverID, callback) {
		var status = "pending";
		var query = {
			TableName : "PendPassenger",
			FilterExpression : "DriverID = :key and CurrStatus = :status",
			ExpressionAttributeValues : {
				":key" : DriverID,
				":status" : status
			}				
		};
		dynamodb.scan(query, function(err, data) {
			callback(err, data);
		});
	});	
	socket.on('queryByID', function(ID, callback){
		var query  = {
			TableName : 'RequestPassenger',
			Key:{
				'RequestId' : ID
			}
		};
		dynamodb.get(query, function(err, data){

			callback(err,data);
			
		});

	});
	socket.on('searchPass', function(DriverID,callback){
		var query = {
			TableName : "RequestPassenger",
			FilterExpression : "Driver_email = :key",
			ExpressionAttributeValues : {
				":key" : DriverID
			}				
		};
		dynamodb.scan(query, function(err, data) {
			console.log(data);
			callback(err, data);
		});		

	});
	socket.on('getDriverList', function(RequestId, callback){
		var status = "confirmed";
		var query = {
			TableName : "PendPassenger",
			FilterExpression : "RequestId = :key and CurrStatus = :status",
			ExpressionAttributeValues : {
				":key" : RequestId,
				":status" : status
			}				
		};
		dynamodb.scan(query, function(err, data) {
			console.log(data);
			callback(err, data);

		});	
	});

	socket.on('getPassengerPost', function(DriverID, callback) {
		var status = "confirmed";
		var query = {
			TableName : "Pending",
			FilterExpression : "PassengerID = :key and CurrStatus = :status",
			ExpressionAttributeValues : {
				":key" : DriverID,
				":status" : status
			}				
		};
		dynamodb.scan(query, function(err, data) {
			callback(err, data);
		});
	});
	socket.on('pendPassengerPost', function(DriverID, callback) {
		var status = "pending";
		var query = {
			TableName : "Pending",
			FilterExpression : "PassengerID = :key and CurrStatus = :status",
			ExpressionAttributeValues : {
				":key" : DriverID,
				":status" : status
			}				
		};
		dynamodb.scan(query, function(err, data) {
			callback(err, data);
		});
	});
	socket.on('queryByID1', function(ID, callback){
		var query  = {
			TableName : 'Request',
			Key:{
				'RequestId' : ID
			}
		};
		dynamodb.get(query, function(err, data){
			callback(err,data);
			
		});

	});

	socket.on('GetMypost', function(key) {
		console.log(key);
		var result = [];
		var pend = {
			TableName : "RequestPassenger",
			FilterExpression : "Driver_email= :queryKey",
			ExpressionAttributeValues : {
				":queryKey" : key
			}
		};
		dynamodb.scan(pend, function(err, data){
			if (err) {
				console.log(err);
			} else {
				console.log("first");
				console.log(result);
				result.push(data);
				var driver = {
					TableName : "Request",
					FilterExpression : "Driver_email = :queryKey",
					ExpressionAttributeValues : {
						":queryKey" : key
					}
				};
				dynamodb.scan(driver, function(err, data){
					if (err) {
						console.log(err);
					} else {
						result.push(data);
						//console.log(s);
						console.log("second");
						console.log(result);
						socket.emit('responsePost',result);
					}
				});		
			}
		});


	});
	socket.on('getMyPending', function(key) {
		var status = "pending";
		var pend = {
			TableName : "Pending",
			FilterExpression : "DriverID = :queryKey and CurrStatus = :status",
			ExpressionAttributeValues : {
				":queryKey" : key,
				":status" : status
			}
		};
		dynamodb.scan(pend, function(err, data){
			if (err) {
				console.log(err);
			} else {
				socket.emit('responsePending',data);
			}
		});
		
	});
	socket.on('UpdateStatus', function(data){

		var key = data;
		console.log(key)
		var tableName = '';
		if (data.charAt(0) == 'D') {
			tableName = 'Pending';
		} else {
			tableName = 'PendPassenger';
		}
		var info = {
			TableName : tableName,
			Key : {
				"PendingId" : key
			}
		};
		console.log(tableName);
		dynamodb.get(info, function(err, data) {
			if (err) {
				console.log(err);
			} else {
				if(!isEmpty(data)){
					console.log(data);
					var item = data.Item;
					var PendingId = item.PendingId;
					var CurrStatus = 'confirmed';
					var DriverID = item.DriverID;
					var Message = item.Message;
					var PassengerID = item.PassengerID;
					var RequestId = item.RequestId;
					var updateItem = {
						TableName : tableName,
						Key : {
							"PendingId" : PendingId
						},
						UpdateExpression : "SET CurrStatus= :CurrStatus, DriverID= :DriverID, Message= :Message,RequestId= :RequestId, PassengerID= :PassengerID",
						ExpressionAttributeValues : {
							":CurrStatus" : CurrStatus,
							":DriverID" : DriverID,
							":Message" : Message,
							":RequestId" : RequestId,
							":PassengerID" : PassengerID
						}
					};
					console.log(updateItem);
					dynamodb.update(updateItem, function(err, data) {
						if (err) {
							console.log(err);
						} else {
							console.log("success");
						}
					});

				} else {
					console.log("error to get user info");
				}

			}
		});
	});

	socket.on('transferData', function(data) {
		var key = data.key;	
		console.log(key);
		database.search({
			  q: key,
			  size : 100
			}).then(function (body) {
			  var hits = body.hits.hits;
			  console.log(hits);
			  socket.emit('informationTransmission', {data: hits});

			}, function (error) {
			  console.trace(error.message);
			});
	 });
	socket.on('SearchNow', function(destination) {
		var dest = destination;
		console.log(dest);
		socket.emit('Destination', dest);

	});
	socket.on('confirmPassenger', function(query) {
		var tableName = "Pending";
		var key = 'D' + generateKey();
		var param = {
			TableName : tableName,
			Item : {
				"PendingId" : key,
				"DriverID" : query.DriverID,
				"PassengerID" : query.PassengerID,
				"RequestId" : query.RequestId,
				"CurrStatus" : "pending"
			}
		};
		dynamodb.put(param, function(err, data){
			if(err) {
				console.log(err);
			} else {
				console.log("success");
			}

		});
	});
	socket.on('confirmDriver', function(query) {
		var tableName = "PendPassenger";
		var key = 'P' + generateKey();
		var param = {
			TableName : tableName,
			Item : {
				"PendingId" : key,
				"DriverID" : query.DriverID,
				"PassengerID" : query.PassengerID,
				"RequestId" : query.RequestId,
				"CurrStatus" : "pending"
			}
		};
		dynamodb.put(param, function(err, data){
			if(err) {
				console.log(err);
			} else {
				console.log("success");
			}

		});
	});


	socket.on('finduser', function(username, password) {
		var tableName = "Driver_info";
		var params = {
			TableName : tableName,
			Key : {"UserName" : username}
		};
		dynamodb.get(params, function(err, data){
			if (err) {
				console.log ("Error = " + JSON.stringify(err));
			} else {
				console.log(data);
				//console.log(data.Item.PassWord);
				if (data && data.Item && data.Item.PassWord) {
					if (data.Item.PassWord == password) {
						console.log("Correct password");
						socket.emit('getRespons','true', data);
					} else {
						console.log("Wrong password");
						socket.emit('getRespons','false', null);
					}
				}

			}
		});
	});

	socket.on('Register User', function(input) {
		console.log(input);
		var driverinfo = input.Personal;
		var car = input.CarInfo;
		var DriverLisence = "None";
		var licsencePlate = "None";
		if (car && car.licsencePlate) {
			licsencePlate = car.licsencePlate

		}
		if (DriverLisence) {
			DriverLisence = driverinfo.Liscense;
		}
		console.log(driverinfo);
		console.log(car);
		var userparams = {
			TableName : "Driver_info",
			Item : {
				"UserName" : driverinfo.email,
				"PassWord" : driverinfo.password,
				"LastName" : driverinfo.lastName,
				"FirstName" : driverinfo.firstName,
				"DriverLisence" : DriverLisence,
				"licsencePlate" : licsencePlate
			}
		}

		dynamodb.put(userparams, function(err, data) {
            if (err) {
                console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
                socket.emit('response', false);
            } else {
                console.log("Added item:", JSON.stringify(data, null, 2));
                socket.emit('response', true);
            }
         });
		if (car.licsencePlate) {
			var carparams = {
				TableName : "Car",
				Item : {
					"licsencePlate" : licsencePlate,
					"UserName" : driverinfo.email,
					"Make" : car.Make,
					"model" : car.model,
					"year" : car.year
				}
			};
			dynamodb.put(carparams, function(err, data) {
            if (err) {
                console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
            } else {
                console.log("Added item:", JSON.stringify(data, null, 2));
            }
         });

		}
	});
	socket.on('queryUser', function(myquery){
		console.log(myquery);
		var seat = Number(myquery.Seats);
		var cost = Number(myquery.Cost);
		var username = myquery.username;
		var parms = {
			TableName : "Request",
			FilterExpression : "Destination = :dest and Departure = :dep and SeatAvailable >= :seat and Cost <= :cost",
			ExpressionAttributeValues : {
				":dest" : myquery.Destination,
				":dep" : myquery.Departure,
				":seat" : seat,
				//":dep_date" : myquery.date,
				":cost" : cost
			}
		};
		console.log(parms);
		dynamodb.scan(parms, function(err, data){
			if(err) {
        		console.log("Unable to query. Error:", JSON.stringify(err, null, 2));
			} else {
				console.log("success");
				console.log(data);
				socket.emit('getResultUser', data);
				data.Items.forEach(function(item) {
		            console.log(item);
        		});
			}
		});
	});
	socket.on('postuser', function(mypost){
		var id =generateKey();
		var seat = Number(mypost.Seats);
		var cost = Number(mypost.cost);
		var parms = {
			TableName : "RequestPassenger",
			Item : {
				"RequestId" : id,
				"Destination" : mypost.Destination,
				"Departure" : mypost.Departure,
				"Dep_date" : mypost.date,
				"Cost" : cost,
				"SeatAvailable" : seat,
				"Smoke" : mypost.smoke,
				"Driver_email" : mypost.PassengerId,
				"Time" : mypost.Time
			}
		};
		console.log(parms);
		dynamodb.put(parms, function(err, data){
			if(err) {
				console.log(err);
			} else {
				console.log("success");
			}

		});


	});
	socket.on('queryPassenger', function(myquery) {
		var seat = Number(myquery.Seats);
		var cost = Number(myquery.Cost);
		var username = myquery.username;
		var parms = {
			TableName : "RequestPassenger",
			FilterExpression : "Destination = :dest and Departure = :dep and SeatAvailable <= :seat and Cost >= :cost",
			ExpressionAttributeValues : {
				":dest" : myquery.Destination,
				":dep" : myquery.Departure,
				":seat" : seat,
				//":dep_date" : myquery.date,
				":cost" : cost
			}
		};
		console.log(parms);
		dynamodb.scan(parms, function(err, data){
			if(err) {
        		console.log("Unable to query. Error:", JSON.stringify(err, null, 2));
			} else {
				console.log("success");
				console.log(data);
				socket.emit('getResultPassenger', data);
				data.Items.forEach(function(item) {
		            console.log(item);
        		});
			}
		});		

	});


	socket.on('postdriver', function(mypost) {
		var id = generateKey();

		var seat = Number(mypost.Seats);
		var cost = Number(mypost.cost);
		var parms = {
			TableName : "Request",
			Item : {
				"RequestId" : id,
				"Destination" : mypost.Destination,
				"Departure" : mypost.Departure,
				"Dep_date" : mypost.date,
				"Cost" : cost,
				"SeatAvailable" : seat,
				"Smoke" : mypost.smoke,
				"Driver_email" : mypost.email,
				"Time" : mypost.time
			}
		};
		console.log(parms);
		dynamodb.put(parms, function(err, data){
			if(err) {
				console.log(err);
			} else {
				console.log("success");
			}

		});

	});
	socket.on('ReserveNow', function(package){
		var pid = generateKey();
		var did = 'D' + generateKey();
		var cost = Number(package.Cost);
		var seat = Number(package.Seats);
		var PassengerID = package.PassengerId;
		var driverID = package.DriverID;
		var message = package.Message;
		var destination = package.Destination;
		var date = package.Date;
		var longitude = package.longitude;
		var latitude = package.latitude;
		var Passenger = {
			TableName : "Request",
			Item : {
				"RequestId" : pid,
				"Departure" : "New York",
				"Destination" : destination ,
				"Driver_email" : driverID,
				"SeatAvailable" : seat,
				"Cost" : cost,
				"Dep_date" : date
			}
		};
		console.log(Passenger);
		dynamodb.put(Passenger, function(err, data){
			if(err) {
				console.log(err);
			} else {
				console.log(data);
				var Driver = {
					TableName : "Pending",
					Item : {	
						"PendingId" : did,
						"Message" : message,
						"RequestId" : pid,
						"CurrStatus" : "pending",
						"DriverID" : driverID,
						"PassengerID" : PassengerID
					}			
				};
				dynamodb.put(Driver, function(err, data){
					if(err) {
						console.log(err);
					} else {
						console.log(data);
					}
				});
			}
		});
	});

	socket.on('serachRegion', function(latitude, longitude){
	 	console.log("received command");
	 	console.log(latitude);
	 	console.log(longitude);
		database.search({
			"index" : "driverinfo",
			"type" : "driver",
			"size" : "100",
			"body" : {
			    "query": {
			    "filtered": {
			      "query": {
			        "match_all": {}
			      },
			      "filter": {
			      	"geo_distance" : {
			      		"distance" : "1000km",
			      		"driver.geo" : {
			      			"lat" : latitude,
			      			"lon" : longitude
			      		}
			                    
			      	}
			      }
			    }
			  }
			}

			}).then(function (resp) {
			    var hits = resp.hits.hits;
			    console.log(hits.length);
			    socket.emit('test', {data: hits});

			}).catch(function (error) {
			    console.log("Error on geo_distance (coordiates)");
			    console.log(error);
			});
		}); 
	});

	


