var databaseName = "RLS";
var database = db.getSiblingDB(databaseName);
var NameUsers = databaseName + ".Users";
var NameTweets = databaseName + ".Tweets";

print(">>> ПОЧАТОК НАПОВНЕННЯ БАЗИ RLS...");

var users = [
    { n: "Shevchenko", r: "EU" }, { n: "Macron", r: "EU" }, { n: "Merkel", r: "EU" },
    { n: "Biden", r: "USA" }, { n: "Musk", r: "USA" }, { n: "Trump", r: "USA" },
    { n: "Jackie Chan", r: "Asia" }, { n: "BTS", r: "Asia" }, { n: "Ma", r: "Asia" },
];

print(">>> Вставка Users...");
users.forEach(function(u) {
    database.Users.insertOne({ name: u.n, region: u.r });
});

var tweets = [
    { m: "Low1", l: 5 }, { m: "Low2", l: 50 }, { m: "Low3", l: 99 },
    { m: "Mid1", l: 105 }, { m: "Mid2", l: 150 }, { m: "Mid3", l: 199 },
    { m: "High1", l: 300 }, { m: "High2", l: 5000 }, { m: "High3", l: 9999 },
];

print(">>> Вставка Tweets...");
tweets.forEach(function(t) {
    database.Tweets.insertOne({ msg: t.m, likes: t.l });
});

print("\n>>> ПЕРЕВІРКА ВСТАВКИ:");
var userCount = database.Users.countDocuments();
var tweetCount = database.Tweets.countDocuments();

if (userCount > 0 && tweetCount > 0) {
    print("✅ Дані успішно вставлено (Users: " + userCount + ", Tweets: " + tweetCount + ")");
} else {
    print("❌ Помилка: База порожня.");
}

print("[START] Manually splitting and moving chunks...");

sh.splitAt(NameTweets, { likes: 101 });
sh.splitAt(NameTweets, { likes: 201 });

print(">>> Moving 'Mid' Tweets to Shard 2...");
sh.moveChunk(NameTweets, { likes: 105 }, "rs_shard2"); 

print(">>> Moving 'High' Tweets to Shard 3...");
sh.moveChunk(NameTweets, { likes: 300 }, "rs_shard3");


sh.splitAt(NameUsers, { region: "USA", _id: MinKey() });
sh.splitAt(NameUsers, { region: "EU", _id: MinKey() });
sh.splitAt(NameUsers, { region: "Asia", _id: MinKey() });

print(">>> Moving 'USA' Users to Shard 2...");
sh.moveChunk(NameUsers, { region: "USA", _id: MinKey() }, "rs_shard2");

print(">>> Moving 'Asia' Users to Shard 3...");
sh.moveChunk(NameUsers, { region: "Asia", _id: MinKey() }, "rs_shard3");


print("\n>>> REFRESHING ROUTER CONFIG...");
db.adminCommand({ flushRouterConfig: "RLS" });

print("\n>>> ✅ DEFINITIVE CLUSTER STATE (Config DB):");
var configDB = db.getSiblingDB("config");

var printChunkMap = function(ns) {
    print("--- Distribution for " + ns + " ---");
    
    var coll = configDB.collections.findOne({ _id: ns });
    if (!coll) {
        print("Collection not found.");
        return;
    }
    
    var chunks = configDB.chunks.find({ uuid: coll.uuid }).sort({ min: 1 }).toArray();
    
    chunks.forEach(function(c) {
        var zoneLabel = "UNKNOWN";
        if(c.shard === "rs_shard1") zoneLabel = "EU / LOW";
        if(c.shard === "rs_shard2") zoneLabel = "USA / MID";
        if(c.shard === "rs_shard3") zoneLabel = "ASIA / HIGH";

        print(
            "[" + c.shard + "] " + 
            zoneLabel + "  \tRange: " + 
            JSON.stringify(c.min) + " -> " + JSON.stringify(c.max)
        );
    });
};

printChunkMap("RLS.Users");
print("\n");
printChunkMap("RLS.Tweets");
// print(sh.status());