var databaseName = "RLS";
var database = db.getSiblingDB(databaseName);

function testOperation(name, func) {
    print("[TEST] " + name);
    try {
        var start = new Date();
        var result = func();
        var time = (new Date() - start) + "ms";
        print("[SUCCESS] (" + time + "): " + JSON.stringify(result));
    } catch (e) {
        print("[ERROR] " + e.message);
    }
}

testOperation("INSERT in zone USA (Shard 2 - Down)", function() {
    return database.Users.insertOne({ name: "DeadUser", region: "USA" });
});

testOperation("INSERT in zone EU (Shard 1 - Up)", function() {
    return database.Users.insertOne({ name: "LiveUser", region: "EU" });
});

testOperation("FIND in zone USA (Shard 2 - Down)", function() {
    return database.Users.find({ region: "USA" }).toArray(); 
});

testOperation("FIND in zone EU (Shard 1 - Up)", function() {
    return database.Users.find({ region: "EU" }).allowPartialResults().toArray();
});

testOperation("RANGE FIND Likes 101-200 (Shard 2 - Down)", function() {
    return database.Tweets.find({ likes: { $gt: 100, $lt: 200 } }).toArray();
});

testOperation("RANGE FIND Likes 0-100 (Shard 1 - Up)", function() {
    return database.Tweets.find({ likes: { $gt: 0, $lt: 100 } }).allowPartialResults().toArray();
});

print(sh.status());