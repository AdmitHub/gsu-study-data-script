
const MongoClient = require('mongodb').MongoClient;
const _ = require('lodash');
const fs = require('fs');
const csv = require('fast-csv');
const conf = require('./conf');
let _db;
let _aiMatcherDb;
ObjectId = require('mongodb').ObjectID;
let Users;
let BrandedUserProfiles;
let SmsLogs;
let BrandedApplicantConversations;
let MatcherLogs;
return new Promise(function(resolve, reject) {
  MongoClient.connect(conf.main, function(err,db) {
    if (err) {
      throw err;
    } else {
      console.log("successfully connected to the  main database");
    }
    _db = db;
    return resolve(db);
  })
}).then((database)=> {
  Users = database.collection('users');
  BrandedUserProfiles = database.collection('georgiaStateUsers');
  SmsLogs = database.collection('smslogs');
  BrandedApplicantConversations = database.collection('brandedapplicantconversations');
}).then(()=> {
  return new Promise(function(resolve, reject) {
    MongoClient.connect(conf.aiMatcher, function(err,db) {
      if (err) {
        throw err;
      } else {
        console.log("successfully connected to the  ai matcher database");
      }
      _aiMatcherDb = db;
      return resolve(_aiMatcherDb);
    })
  })
}).then((aiMatcherDb)=> {
  MatcherLogs = aiMatcherDb.collection('queryLogs');
  return BrandedUserProfiles.find({studyGroupMember: true, entryYear: 2016, abGroup: {"$lt": .01}}).toArray();
}).then((brandedUsers)=> {
  return brandedUsers.reduce(function(p, singleUser) {
    let statArray;
    let studentObj = {};
    studentObj.UserId = singleUser.userId;
    return p 
    .then((_statArray)=> {
      statArray = _statArray;
      return SmsLogs.find({incoming: false, userId: singleUser.userId}).count();
    }).then((outGoingCount)=> {
      studentObj.outGoingCount = outGoingCount;
      return SmsLogs.find({incoming: true, userId: singleUser.userId, body: {"$exists": true} }).count();
    }).then((incomingCount)=> {
      studentObj.incomingCount = incomingCount;
      let student_id = "" + singleUser._id;
      return BrandedApplicantConversations.aggregate([
        {$match: {"applicantId": student_id, "messages.sender" : "college"}},
        {$unwind: "$messages"},
        {$match: {"messages.sender" : "college"}},
        {$group: {_id: null, count: {$sum: 1}}}
      ])
    }).then((requiredResponseCount)=> {
      studentObj.requiredResponseCount = 0
      requiredResponseCount.forEach(function(countObject){
        studentObj.requiredResponseCount = countObject.count //should only iterate once with object that looks like { _id: null, count: 2 }
      })
      return Users.findOne({_id: singleUser.userId});
    }).then((userDoc)=> {
      if (_.get(userDoc, "smsHardStopped")) {
        studentObj.userExited = "true";
      } else {
        studentObj.userExited = "false";
      }
      return SmsLogs.findOne({userId: singleUser.userId, body: "Before I go away forever, can you please tell me why you decided to stop?"})
    }).then((log)=> {
      if (log) {
        studentObj.userExited = "true";
      }
    }).then(()=> {
      return MatcherLogs.find({userId: singleUser.userId, topic: {"$nin": ["/general/failure", "/general/notfound", "/chat/inappropriate"]}}).count();
    }).then((matcherCount) => {
      studentObj.automaticResponses = matcherCount;
      statArray.push(studentObj);
      return statArray;
    })
  }, Promise.resolve([]))
  
}).then((array)=> {
  console.log(array);
  const csvStream = csv.createWriteStream({headers: true})
  const writableStream = fs.createWriteStream("my.csv");

  writableStream.on("finish", function() {
    console.log("DONE!");
  })
  csvStream.pipe(writableStream);
  array.forEach(function(obj) {
    csvStream.write(obj);
  })
  csvStream.end();
})
.then(()=> {
  return _db.close();
  return _aiMatcherDb.close()
})
.catch((e)=> console.log(e))
