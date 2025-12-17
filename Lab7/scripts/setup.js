var databaseName = "RLS";
var database = db.getSiblingDB(databaseName);
var NameUsers = databaseName + ".Users";
var NameTweets = databaseName + ".Tweets";

var rs1 = "rs_shard1";
var rs2 = "rs_shard2";
var rs3 = "rs_shard3";

var eu = "ZONE_EU";
var usa = "ZONE_USA";
var asia = "ZONE_ASIA";
var low = "LIKES_LOW";
var mid = "LIKES_MID";
var high = "LIKES_HIGH";

sh.addShard(rs1 + "/shard1:27017");
sh.addShard(rs2 + "/shard2:27017");
sh.addShard(rs3 + "/shard3:27017");

sh.enableSharding(databaseName);

try
{
   sh.removeShardFromZone(rs1, eu);
   sh.removeShardFromZone(rs2, usa);
   sh.removeShardFromZone(rs3, asia);
   
   sh.removeShardFromZone(rs1, low);
   sh.removeShardFromZone(rs2, mid);
   sh.removeShardFromZone(rs3, high);
}
catch(e) {}

print("[START] Tagging shards...");
sh.addShardToZone(rs1, eu);
sh.addShardToZone(rs2, usa);
sh.addShardToZone(rs3, asia);

sh.addShardToZone(rs1, low);
sh.addShardToZone(rs2, mid);
sh.addShardToZone(rs3, high);

print("[START] setting regions for Users...");
try { database.Users.drop(); } catch(e) {}
database.Users.createIndex({ region: 1, _id: 1 });
sh.shardCollection(NameUsers, { region: 1, _id: 1 });

sh.updateZoneKeyRange(NameUsers, { region: "EU", _id: MinKey }, { region: "EU", _id: MaxKey }, eu);
sh.updateZoneKeyRange(NameUsers, { region: "USA", _id: MinKey }, { region: "USA", _id: MaxKey }, usa);
sh.updateZoneKeyRange(NameUsers, { region: "Asia", _id: MinKey }, { region: "Asia", _id: MaxKey }, asia);

print("[START] setting Tweets...");
try { database.Tweets.drop(); } catch(e) {}
database.Tweets.createIndex({ likes: 1 });
sh.shardCollection(NameTweets, { likes: 1 });

sh.updateZoneKeyRange(NameTweets, { likes: 0 }, { likes: 101 }, low);
sh.updateZoneKeyRange(NameTweets, { likes: 101 }, { likes: 201 }, mid);
sh.updateZoneKeyRange(NameTweets, { likes: 201 }, { likes: MaxKey }, high);

print("[START] Cluster setup complete!");
print(sh.status());

print("[START] Inserting...");
var tweets = [
    { m: "Low1", l: 5 }, { m: "Low2", l: 50 }, { m: "Low3", l: 99 },    // -> Shard 1
    { m: "Mid1", l: 105 }, { m: "Mid2", l: 150 }, { m: "Mid3", l: 199 }, // -> Shard 2
    { m: "High1", l: 300 }, { m: "High2", l: 5000 }, { m: "High3", l: 9999 } // -> Shard 3
];

tweets.forEach(function(t) {
   database.Tweets.insertOne({ msg: t.m, likes: t.l });
});

database.Users.insertMany([
   { region: "Asia", name: "user_as_1" },
   { region: "Asia", name: "user_as_2" },
   { region: "Asia", name: "user_as_3" },
   { region: "Asia", name: "user_as_4" }
])
for (let i = 0; i < 5000; i++)
{
   database.Users.insertOne({
      region: i % 2 === 0 ? "EU" : "USA",
      name: "user_" + i
   })
}

print(database.Users.getShardDistribution());
print(database.Users.countDocuments());
try { database.Users.deleteMany({}); } catch(e) {}

db.adminCommand({ flushRouterConfig: "RLS.Users" });
db.adminCommand({ flushRouterConfig: "RLS.Tweets" });

print("[START] Setup complete!");