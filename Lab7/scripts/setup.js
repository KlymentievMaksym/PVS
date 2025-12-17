var databaseName = "RLS";
var database= db.getSiblingDB(databaseName);

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

print(sh.status());

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
database.Users.createIndex({ region: 1 });
sh.shardCollection(databaseName + ".Users", { region: 1 });

sh.updateZoneKeyRange(databaseName + ".Users", { region: "EU" }, { region: "EV" }, eu);
sh.updateZoneKeyRange(databaseName + ".Users", { region: "USA" }, { region: "USB" }, usa);
sh.updateZoneKeyRange(databaseName + ".Users", { region: "Asia" }, { region: "Asib" }, asia);

print("[START] setting Tweets...");
database.Tweets.createIndex({ likes: 1 });
sh.shardCollection(databaseName + ".Tweets", { likes: 1 });

sh.updateZoneKeyRange(databaseName + ".Tweets", { likes: 0 }, { likes: 101 }, low);
sh.updateZoneKeyRange(databaseName + ".Tweets", { likes: 101 }, { likes: 201 }, mid);
sh.updateZoneKeyRange(databaseName + ".Tweets", { likes: 201 }, { likes: MaxKey }, high);

print("[START] Cluster setup complete!");