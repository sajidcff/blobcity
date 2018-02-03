var request = require('request');
var utf8 = require('utf8');

var http = "https://"; //in future set to https
var bQueryUrlPart = '/rest/bquery';
var sqlUrlPart = '/rest/sql';

exports.auth = function(ip, ds, username, password) {
    module.ip = ip;
    module.ds = ds;
    module.username = username;
    module.password = password;
}

exports.createDs = function(ds, callback) {
    var query = {
        q: 'create-ds',
        p: {
            name: ds
        }
    };

    request.post(
        http + module.ip + bQueryUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            q: utf8.encode(JSON.stringify(query))
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);

                if(json.ack === '1') {
                    callback(null, JSON.parse(body));
                } else {
                    callback(json.error);
                }
            } else {
                console.log('create-ds error')
                callback(error);
            }
        }
    );
}

exports.dsExists = function(ds, callback) {
    var query = {
        q: 'ds-exists',
        p: {
            ds: ds
        }
    };

    request.post(
        http + module.ip + bQueryUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            q: utf8.encode(JSON.stringify(query))
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);

                if(json.ack === '1') {
                    callback(null, json);
                } else {
                    callback(json.error);
                }
            } else {
                console.log('ds-exists error')
                callback(error);
            }
        }
    );
}

/**
 * Lists the datastore
 * @param callback function to callback with response
 */
exports.listDs = function(callback) {
    var query = {
        q: 'list-ds',
    };

    request.post(
        http + module.ip + bQueryUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            q: utf8.encode(JSON.stringify(query))
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);
                if(json.ack === '1') {
                    callback(null, json);
                } else {
                    callback(json.error);
                }
            } else {
                console.log('list-ds error')
                callback(error);
            }
        }
    );
}

/**
 * Creates the specified collection inside the specified datastore
 * @param {*} ds name of the datastore
 * @param {*} collection name of the new collection
 * @param {*} storageType storage type of the collection
 * @param {*} callback callback function with (err, success)
 */
exports.createCollection = function(ds, collection, storageType, callback) {
    var query = {
        ds: ds,
        q: 'create-collection',
        p: {
            name: collection,
            type: storageType
        }
    };

    request.post(
        http + module.ip + bQueryUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            ds: ds,
            q: utf8.encode(JSON.stringify(query))
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);
                if(json.ack === '1') {
                    callback(null, JSON.parse(body));
                } else {
                    callback(json.error);
                }
            } else {
                console.log('create-collection error')
                callback(error);
            }
        }
    );
}

/**
 * Creates a new collection with the specified storage type. Collection is created
 * inside the datastore as set in the connecting credentials
 * @param {*} collection name of new collection
 * @param {*} storageType storage type of the collection
 * @param {*} callback callback function with (err, success)
 */
exports.createCollection = function(collection, storageType, callback) {
    var query = {
        ds: module.ds,
        q: 'create-collection',
        p: {
            name: collection,
            type: storageType
        }
    };

    request.post(
        http + module.ip + bQueryUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            ds: module.ds,
            q: utf8.encode(JSON.stringify(query))
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);
                if(json.ack === '1') {
                    callback(null, JSON.parse(body));
                } else {
                    callback(json.error);
                }
            } else {
                console.log('create-collection error')
                callback(error);
            }
        }
    );
}

/**
 * Checks if the specified collection exists within the specified datastore
 * @param {*} ds name of datastore
 * @param {*} collection name of collection
 * @param {*} callback callback function with (err, success)
 */
exports.collectionExists = function(ds, collection, callback) {
    var query = {
        ds: module.ds,
        q: 'collection-exists',
        p: {
            ds: ds,
            c: collection
        }
    };

    request.post(
        http + module.ip + bQueryUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            q: utf8.encode(JSON.stringify(query))
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);
                if(json.ack === '1') {
                    callback(JSON.parse(body));
                } else {
                    callback(json.error);
                }
            } else {
                console.log('collection-exists error')
                callback(error);
            }
        }
    );
}

/**
 * Checks is a collection exists. Collection is searched for inside the datastore set
 * in the connecting credentials
 * @param {*} collection name of collection
 * @param {*} callback callback function with (err, success)
 */
exports.collectionExists = function(collection, callback) {
    var query = {
        ds: module.ds,
        q: 'collection-exists',
        p: {
            ds: module.ds,
            c: collection
        }
    };

    request.post(
        http + module.ip + bQueryUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            q: utf8.encode(JSON.stringify(query))
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);
                if(json.ack === '1') {
                    callback(JSON.parse(body));
                } else {
                    callback(json.error);
                }
            } else {
                console.log('collection-exists error')
                callback(error);
            }
        }
    );
}

exports.dropDs = function(ds, callback) {
    var query = {
        q: 'drop-ds',
        p: {
            name: ds
        }
    };

    request.post(
        http + module.ip + bQueryUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            q: utf8.encode(JSON.stringify(query))
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);

                if(json.ack === '1') {
                    callback(null, JSON.parse(body));
                } else {
                    callback(json.error);
                }
            } else {
                console.log('drop-ds error')
                callback(error);
            }
        }
    );
}

exports.dropCollection = function(collection, callback) {
    console.log('dropCollection not yet supported.');
}

/**
 * Inserts a single or multiple records
 * @param collection name of collection
 * @param data JSONArray of data
 * @param callback function to call back with result
 */
exports.insert = function(collection, data, callback) {

    var query = {
        ds: module.ds,
        t: collection,
        q: 'insert',
        p: {
            type: 'json',
            data: data
        }
    };

    request.post(
        http + module.ip + bQueryUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            ds: module.ds,
            q: utf8.encode(JSON.stringify(query))
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);
                if(json.ack === '1') {
                    callback(null, JSON.parse(body));
                } else {
                    callback(json.error);
                }
            } else {
                console.log('Insert error');
                callback(error);
            }
        }
    );
}

exports.save = function(collection, data, callback) {

    var query = {
        ds: module.ds,
        t: collection,
        q: 'save',
        p: {
            type: 'json',
            data: data
        }
    };

    request.post(
        http + module.ip + bQueryUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            ds: module.ds,
            q: utf8.encode(JSON.stringify(query))
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);
                if(json.ack == '1') {
                    callback(null, JSON.parse(body));
                } else {
                    callback(json.error);
                }
            } else {
                console.log('Save error');
                callback(error);
            }
        }
    );
}

exports.sql = function(sql, callback) {
    request.post(
        http + module.ip + sqlUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            ds: module.ds,
            q: utf8.encode(sql)
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);
                if(json.ack === '1') {
                    callback(null, JSON.parse(body));
                } else {
                    callback(json.error);
                }

            } else {
                console.log('SQL error');
                callback(error);
            }
        }
    );
}

exports.select = function(collection, pk, callback) {
    callback('not supported yet')
}

exports.deleteRecord = function(collection, pk, callback) {
    callback('not supported yet')
}

exports.data = function(response) {
    return response.p;
}

exports.addUser = function(username, password, callback) {
    var query = {
        q: 'add-username',
        p: {
            username: username,
            password: password
        }
    };

    request.post(
        http + module.ip + bQueryUrlPart,
        {form: {
            username: module.username,
            password: module.password,
            q: utf8.encode(JSON.stringify(query))
        }},
        function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var json = JSON.parse(body);

                if(json.ack === '1') {n
                    callback(null, JSON.parse(body));
                } else {
                    callback(json.error);
                }
            } else {
                console.log('add-user error')
                callback(error);
            }
        }
    );
}