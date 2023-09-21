let databaseName = 'mongo_datalake';
let collectionName = 'Users';
let db = db.getSiblingDB(databaseName);

db.createCollection(collectionName);
