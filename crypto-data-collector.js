var lang = require('lang-mini');
var each = lang.each;
var to_arr_strip_keys = lang.to_arr_strip_keys;
var get_arr_from_truth_map = lang.get_arr_from_truth_map;

var Evented_Class = lang.Evented_Class;

//var autobahn = require('autobahn');
//var request = require('request');
//var moment = require('moment');

var xas2 = require('xas2');
var path = require('path');
// Would a distributed bittrex watcher be better?
var Bittrex_Watcher = require('bittrex-watcher');

var NextLevelDB_Model = require('nextleveldb-crypto-model');
// Making a Crypto model could be neater?
var Model_Database = NextLevelDB_Model.Database;
var Model_Table = NextLevelDB_Model.Table;
var Model_Record = NextLevelDB_Model.Record;

// Seems like it would be worth starting up an Active Database.
//  Can ensure tables with Active Database

// Could use the asset client?
var NextLevelDB_Client = require('nextleveldb-assets-client');

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
//console.log('config', config);

//console.log('app_config', app_config);
//throw 'stop';


let delay = config.delay;

//var app_config = 

// A more generalised data collector system?
//  Includes DB table defs, functions to collect, and mapping to the db records.

// Worth keeping this collecting for a while on a remote server.

// Worth making a Bittrex Collector and also having a Collector_Base.
//  The Collector would also check that the DB is set up correctly / ensure that it is.



// Collector could be a basis, with various functions and variables that get plugged into it.


// npm collector-base
//  May be a bit declarative, could have some functions in the declaration.
//  Ensure db structure
//   including structure records
//  Repeating processes
//   Mapping results to declared table structure

// Another version of this, but using a more generalised system, will work nicely.
//  Repeated get data, then function to turn them into records to be added / ensured, then put them into the DB.


//console.log('require.main', require.main);
if (require.main === module) {

    var tbl_market_providers, tbl_bittrex_currencies, tbl_bittrex_markets;
    // A version where it asyncronously gets a load of data, such as the markets and exchanges, and adds them all.
    //  They will need to be batched up as records, and the Model will be useful for this.

    var bittrex_watcher = new Bittrex_Watcher();
    var server_data1 = config.nextleveldb_connections.localhost;

    /*
    console.log('server_data2', server_data2);

    var local_info = {
        'server_address': 'localhost',
        //'server_address': 'localhost',
        //'db_path': 'localhost',
        'server_port': 420
    }
    */

    var nldb_client = new NextLevelDB_Client(server_data1);

    nldb_client.start((err, res_start) => {
        if (err) {
            console.trace();
            throw err;
        } else {

            var repeated_collect_market_summaries = () => {

                // May need to reload the currencies.

                var crypto_model_db = nldb_client.model;

                nldb_client.load_tables(['bittrex currencies', 'bittrex markets'], (err, res) => {
                    if (err) {
                        throw err;
                    } else {


                        var map_currencies = crypto_model_db.get_obj_map('bittrex currencies', 'Currency');

                        // get all records in a table, and make that map out of the value.



                        // object maps / indexes
                        //  we want an easy to use way of looking up the currencies by code.

                        // get_obj_map could be moved into the db stack. That means client handler, server handler, and server function to actually do it.



                        console.log('map_currencies', map_currencies);
                        //throw 'stop';

                        // then get the map of markets by name

                        var map_markets = crypto_model_db.get_obj_map('bittrex markets', 'MarketName');
                        //console.log('map_markets', map_markets);

                        var arr_markets = get_arr_from_truth_map(map_markets);



                        var collect_filtered_market_summaries = () => {
                            // config_all_bittrex
                            //console.log('collect_filtered_market_summaries');
                            // 
                            //throw 'stop';

                            bittrex_watcher.get_market_summaries_filter_by_arr_market_names(arr_markets, (err, at_market_summaries) => {
                                // Could make it put the error in a log file, and continue.

                                if (err) {
                                    console.log('get market summaries err', err);
                                } else {
                                    //console.log('at_market_summaries', at_market_summaries);
                                    // (Then create DB records out of these.)

                                    //throw 'stop';
                                    arr_market_summary_records = at_market_summaries.transform_each((value) => {
                                        var str_market_name = value[0],
                                            market_key = map_markets[str_market_name],
                                            d = Date.parse(value[6] + 'Z');
                                        console.log('str_market_name', str_market_name);
                                        var res = [
                                            [market_key, d],
                                            [value[4], value[7], value[8], value[3], value[5], value[9], value[10]]
                                        ];
                                        return res;
                                    });
                                    //console.log('stopping');
                                    //throw 'stop';

                                    console.log('arr_market_summary_records', arr_market_summary_records);
                                    console.log('arr_market_summary_records.length', arr_market_summary_records.length);

                                    // then use the Model DB to encode all these records as binary buffer.

                                    var tbl_bittrex_snapshots = crypto_model_db.map_tables['bittrex market summary snapshots'];
                                    //console.log('tbl_bittrex_snapshots.key_prefix', tbl_bittrex_snapshots.key_prefix);
                                    var buf_encoded_records = crypto_model_db.encode_table_model_rows('bittrex market summary snapshots', arr_market_summary_records);
                                    console.log('buf_encoded_records.length', buf_encoded_records.length);
                                    // then push these encoded records to the database.
                                    console.log('pre put records');
                                    throw 'stop';
                                    nldb_client.ll_put_records_buffer(buf_encoded_records, (err, res_put) => {
                                        if (err) {
                                            throw err;
                                        } else {
                                            console.log('* res_put', res_put);

                                            // not sure we really need these counts.

                                            var show_record_counts = () => {
                                                nldb_client.ll_count_records((err, num_records) => {
                                                    if (err) {
                                                        callback(err);
                                                    } else {
                                                        console.log('num_records', num_records);

                                                        var kp = crypto_model_db.map_tables['bittrex market summary snapshots'].key_prefix;


                                                        // Put / ensure a bittrex market record.
                                                        //  It will do the lookups itself to make sure that we reference the correct items.

                                                        // Also will check for existance of bittrex market records.

                                                        // have something in the client to count table records by key prefix

                                                        // Countring records in specific table will be faster, with maintained record counts in tables.
                                                        //  Slowing down the normal record puts, to update the count.

                                                        nldb_client.count_records_by_key_prefix(kp, (err, count) => {
                                                            if (err) {
                                                                throw err;
                                                            } else {
                                                                console.log('count', count);
                                                                // Could get the table names from the db
                                                                //  Could get table info including key prefixes

                                                                // Could even have record counts?
                                                                //  Some record counts take ages to do.
                                                            }
                                                        });
                                                    }
                                                });
                                            }
                                        }
                                    });
                                }
                            });
                        };
                        // First need to ensure the DB markets are up to date.=

                        var collect_market_summaries = () => {
                            // config_all_bittrex

                            bittrex_watcher.get_at_market_summaries((err, at_market_summaries) => {
                                // Could make it put the error in a log file, and continue.

                                if (err) {
                                    console.log('get market summaries err', err);
                                } else {
                                    //console.log('at_market_summaries', at_market_summaries);
                                    //throw 'stop';
                                    // (Then create DB records out of these.)
                                    arr_market_summary_records = at_market_summaries.transform_each((value) => {
                                        var str_market_name = value[0],
                                            market_key = map_markets[str_market_name],
                                            d = Date.parse(value[6] + 'Z');

                                        // Problem stemming from undefined market keys.
                                        //  Need to properly ensure the bittrex markets and currencies when we load them

                                        //console.log('market_key', market_key);
                                        if (!market_key) {
                                            throw 'Market ' + str_market_name + ' not found';
                                        }
                                        var res = [
                                            [market_key, d],
                                            [value[4], value[7], value[8], value[3], value[5], value[9], value[10]]
                                        ];
                                        return res;
                                    });

                                    var tbl_bittrex_snapshots = crypto_model_db.map_tables['bittrex market summary snapshots'];
                                    //console.log('tbl_bittrex_snapshots.key_prefix', tbl_bittrex_snapshots.key_prefix);

                                    // client.put_table_records ...
                                    //  That would be a friendlier way of doing it.
                                    //   Encodes the records, then does the ll put
                                    //    Nice if it returned the keys of the put records once the puts have been confirmed.
                                    //     Could have a confirm put option?
                                    //      The DB could even do a get after the put to test its there? Overkill maybe.

                                    var buf_encoded_records = crypto_model_db.encode_table_model_rows('bittrex market summary snapshots', arr_market_summary_records);
                                    nldb_client.ll_put_records_buffer(buf_encoded_records, (err, res_put) => {
                                        if (err) {
                                            throw err;
                                        } else {
                                            var d = new Date();
                                            var n = d.toLocaleTimeString();

                                            console.log('put ' + buf_encoded_records.length + ' bytes @' + n, res_put);

                                            // not sure we really need these counts.
                                        }
                                    });
                                }
                            });
                        };
                        collect_market_summaries();
                        setInterval(collect_market_summaries, delay);


                    }
                })


            }

            var wiping_start = () => {
                //console.log('wiping_start');

                nldb_client.ll_count_records((err, num_records) => {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('num_records', num_records);
                        //console.log('pre config_top_bittrex');

                        nldb_client.model = new NextLevelDB_Model.Database();

                        nldb_client.model.config_all_bittrex((err, res_config) => {
                            if (err) {
                                throw err;
                            } else {
                                console.log('res_config', res_config);
                                var buf_crypto_model_db = nldb_client.model.get_model_rows_encoded();

                                if (num_records === 0) {
                                    console.log('pre put records');
                                    nldb_client.ll_put_records_buffer(buf_crypto_model_db, (err, res_put) => {
                                        if (err) {
                                            throw 'err';
                                        } else {
                                            repeated_collect_market_summaries();
                                        }
                                    });
                                } else {

                                    console.log('pre wipe_replace');
                                    nldb_client.wipe_replace(buf_crypto_model_db, (err, res_wipe) => {
                                        if (err) {
                                            throw 'err';
                                        } else {
                                            console.log('res_wipe', res_wipe);

                                            repeated_collect_market_summaries();
                                        }
                                    });
                                }
                            };
                        });
                    }
                });
            }
            //wiping_start();

            var counting_start = () => {

                // Could ensure DB components, using ensure_table, then ensure the records.

                // console.log('counting current records');

                nldb_client.count_core((err, num_records) => {
                    console.log('num_records', num_records);
                    //throw 'stop';
                    if (num_records <= 16) {
                        wiping_start();
                    } else {
                        //ctu();

                        console.log('pre ensure_bittrex_structure_current');

                        // uses an observable in general, not a callback.

                        nldb_client.ensure_bittrex_structure_current((err, res) => {
                            if (err) {
                                throw err;
                            } else {
                                console.log('cb ensure_bittrex_structure_current');

                                //throw 'stop';
                                //console.log('res', res);


                                var crypto_model_db = nldb_client.model;
                                var tbl_currencies = crypto_model_db.map_tables['bittrex currencies'];
                                //console.log('tbl_currencies.records.arr_records', tbl_currencies.records.arr_records);
                                //console.log('tbl_currencies.field_names', tbl_currencies.field_names);
                                var map_currencies = crypto_model_db.get_obj_map('bittrex currencies', 'Currency');
                                console.log('map_currencies', map_currencies);
                                var map_markets = crypto_model_db.get_obj_map('bittrex markets', 'MarketName');
                                //console.log('map_markets', map_markets);
                                repeated_collect_market_summaries();

                            }
                        })

                        // ensure tables onto the server.
                        //  assets client could do that OK.


                        // Core model will have already been created and loaded on the server side.
                        //  The server side will make use of the model more in various cases.
                        //   It will be used to generate a record's index records.
                    }
                });
            }
            counting_start();
        }
    });


} else {
    //console.log('required as a module');

}