var lang = require('lang-mini');
var each = lang.each;
var to_arr_strip_keys = lang.to_arr_strip_keys;
var get_arr_from_truth_map = lang.get_arr_from_truth_map;
var Evented_Class = lang.Evented_Class;
var xas2 = require('xas2');
var path = require('path');
// Would a distributed bittrex watcher be better?
//var Bittrex_Watcher = require('bittrex-watcher');
//var NextLevelDB_Model = require('nextleveldb-crypto-model');
//let Record_List = NextLevelDB_Model.Record_List;

// Making a Crypto model could be neater?
//var Model_Database = NextLevelDB_Model.Database;
//var Model_Table = NextLevelDB_Model.Table;
//var Model_Record = NextLevelDB_Model.Record;

// Seems like it would be worth starting up an Active Database.
//  Can ensure tables with Active Database

// Could use the asset client?
var NextLevelDB_Client = require('nextleveldb-assets-client');

// Should probably be simpler code, that does more.
// var readFile = Promise.promisify(require("fs").readFile);
const NT_XAS2_NUMBER = 1;
const NT_DATE = 2;
const NT_TIME = 3;
const NT_STRING = 4;
const NT_FLOAT32_LE = 5;

// Make this work on either a local server or remote.
// Definitely want to get this set up and running, deployed on a remote server easily.

var config = require('my-config').init({
    path: path.resolve('../../config/config.json') //,
    //env : process.env['NODE_ENV']
    //env : process.env
});

var app_config = require('my-config').init({
    path: path.resolve('./app-config.json') //,
    //env : process.env['NODE_ENV']
    //env : process.env
});

Object.assign(config, app_config);
let delay = config.delay;


// 11/05/2018 - Unfortunately this is not proving fully reliable, it crashes when another coin gets launched, and seems to write it to poisition 0
//  (incrementor fault.)
// Maybe the underlying code has been fixed but the deployed code has got problems.
//  It's worth redoing code to make it use promises, observables, and buffer backed DB model objects.

// 19/05/2018 - Will use the OO classes that represent the data.
//  Need to link them to the DB.
//  Will have methods in asset-client to ensure these OO records.

if (require.main === module) {
    // A version where it asyncronously gets a load of data, such as the markets and exchanges, and adds them all.
    //  They will need to be batched up as records, and the Model will be useful for this.

    //var bittrex_watcher = new Bittrex_Watcher();
    // Want to just start the bittrex watcher.
    //  Bittrex watcher will produce the data from its 'next' events.

    // Bittrex watcher should also be able to provide the list of currencies, markets, in simple ways.


    /*
    var config = require('my-config').init({
        path: path.resolve('../../config/config.json') //,
        //env : process.env['NODE_ENV']
        //env : process.env
    });
    */
    var server_data1 = config.nextleveldb_connections.localhost;

    let access_token = config.nextleveldb_access.root[0];
    //console.log('access_token', access_token);
    server_data1.access_token = access_token;

    var nldb_client = new NextLevelDB_Client(server_data1);

    (async () => {
        await nldb_client.start();
        nldb_client.collect_bittrex();
    })();
} else {
    //console.log('required as a module');

}